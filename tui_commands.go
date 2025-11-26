package main

import (
	"fmt"
	"os"
	"strings"

	"ted/internal/dblib"
)

// SQL execution
func (e *Editor) executeSQL(query string) {
	if e.relation == nil || e.relation.DB == nil {
		e.SetStatusError("No database connection available")
		return
	}

	// pause refresh timer
	e.stopRefreshTimer()

	query = strings.TrimSpace(query)
	if query == "" {
		return
	}

	// Check for transaction statements
	queryUpper := strings.TrimSpace(strings.ToUpper(query))
	queryUpper = strings.Join(strings.Fields(queryUpper), " ") // Normalize whitespace

	// Disallow transaction control statements
	if strings.HasPrefix(queryUpper, "BEGIN") ||
		strings.HasPrefix(queryUpper, "START TRANSACTION") ||
		strings.HasPrefix(queryUpper, "BEGIN WORK") ||
		strings.HasPrefix(queryUpper, "BEGIN TRANSACTION") {
		e.SetStatusError("Transaction statements are not allowed in SQL mode")
		e.startRefreshTimer()
		return
	}

	// Check for multiple statements (semicolon not at the end)
	trimmedQuery := strings.TrimRight(query, "; \t\n\r")
	if strings.Contains(trimmedQuery, ";") {
		e.SetStatusError("Multiple statements are not allowed in SQL mode")
		e.startRefreshTimer()
		return
	}

	result, err := e.relation.DB.Exec(query)
	if err != nil {
		e.SetStatusError(err.Error())
		return
	}

	// Try to get last insert id
	lastInsertId, lastIdErr := result.LastInsertId()
	rowsAffected, rowsErr := result.RowsAffected()

	// Build status message
	var statusParts []string
	if rowsErr == nil {
		if rowsAffected == 1 {
			statusParts = append(statusParts, "1 row affected")
		} else {
			statusParts = append(statusParts, fmt.Sprintf("%d rows affected", rowsAffected))
		}
	}
	if lastIdErr == nil && lastInsertId > 0 {
		statusParts = append(statusParts, fmt.Sprintf("last insert id: %d", lastInsertId))
	}
	if len(statusParts) > 0 {
		e.SetStatusMessage(strings.Join(statusParts, ", "))
	} else {
		e.SetStatusMessage("Query executed successfully")
	}
	e.startRefreshTimer()
}

// Command execution
func (e *Editor) executeCommand(command string) {
	if command == "" {
		return
	}

	// Split command into parts
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return
	}

	cmd := parts[0]
	args := parts[1:]

	switch cmd {
	case "quit", "q":
		e.app.Stop()
	case "help", "h":
		e.SetStatusMessage("Commands: quit, refresh, help")
	case "log":
		if len(args) > 0 {
			e.SetStatusLog(strings.Join(args, " "))
		} else {
			e.SetStatusMessage("Usage: log <message>")
		}
	case "error":
		if len(args) > 0 {
			e.SetStatusError(strings.Join(args, " "))
		} else {
			e.SetStatusMessage("Usage: error <message>")
		}
	default:
		e.SetStatusError("Unknown command: " + cmd)
	}
}

// enterDeleteMode enters delete mode for the current row
func (e *Editor) enterDeleteMode(row, col int) {
	if row < 0 || row >= len(e.buffer) || e.buffer[row].data == nil {
		return
	}

	// Build DELETE preview
	// Convert records to [][]any for BuildDeletePreview
	recordsData := make([][]any, len(e.buffer))
	for i := range e.buffer {
		recordsData[i] = e.buffer[i].data
	}
	preview := e.relation.BuildDeletePreview(recordsData, row)
	if preview == "" {
		e.SetStatusError("Cannot delete row without key values")
		return
	}

	// Set mode to Delete and show preview
	e.setPaletteMode(PaletteModeDelete, false)
	e.commandPalette.SetPlaceholder(preview)
	e.commandPalette.SetPlaceholderStyle(e.commandPalette.GetPlaceholderStyle())
	e.SetStatusMessage("Enter to confirm deletion · Esc to cancel")

	// Store the row being deleted in table selection
	e.table.Select(row, col)
}

// executeDelete executes the DELETE statement for the current row
func (e *Editor) executeDelete() error {
	row, col := e.table.GetSelection()
	if row < 0 || row >= len(e.buffer) || e.buffer[row].data == nil {
		e.SetStatusError("Invalid row for deletion")
		return fmt.Errorf("invalid row")
	}

	// Execute the delete
	// Convert records to [][]any for DeleteDBRecord
	recordsData := make([][]any, len(e.buffer))
	for i := range e.buffer {
		recordsData[i] = e.buffer[i].data
	}
	err := e.relation.DeleteDBRecord(recordsData, row)
	if err != nil {
		e.SetStatusErrorWithSentry(err)
		return err
	}

	// Refresh data after deletion
	e.SetStatusMessage("Record deleted successfully")
	e.loadFromRowId(nil, e.buffer[e.lastRowIdx()].data != nil, col)
	return nil
}

func (e *Editor) executeFind(findValue string) {
	if e.relation == nil {
		e.SetStatusError("No database connection available")
		return
	}

	row, col := e.table.GetSelection()
	if row < 0 || row >= len(e.buffer) || e.buffer[row].data == nil {
		e.SetStatusError("Invalid current row")
		return
	}

	// Get current row's key values
	currentKeys := make([]any, len(e.relation.Key))
	for i, keyIdx := range e.relation.Key {
		if keyIdx < len(e.buffer[row].data) {
			currentKeys[i] = e.buffer[row].data[keyIdx]
		}
	}

	// Find the column index in relation.Columns
	findCol := -1
	colName := e.columns[col].Name
	if colIdx, ok := e.relation.ColumnIndex[colName]; ok {
		findCol = colIdx
	}

	if findCol == -1 {
		e.SetStatusError("Column not found in relation")
		return
	}

	// Use FindNextRow to search for the next matching row
	foundKeys, foundBelow, err := e.relation.FindNextRow(findCol, findValue, nil, nil, currentKeys)
	if err != nil {
		e.SetStatusErrorWithSentry(err)
		return
	}

	if foundKeys == nil {
		e.SetStatusMessage("No match found")
		return
	}

	// Check if the found row is within the current record window
	foundInWindow := false
	var foundRow int

	for i := 0; i < len(e.buffer); i++ {
		if e.buffer[i].data == nil {
			break
		}
		// Compare key values
		match := true
		for j, keyIdx := range e.relation.Key {
			if keyIdx >= len(e.buffer[i].data) || keyIdx >= len(foundKeys) {
				match = false
				break
			}
			if e.buffer[i].data[keyIdx] != foundKeys[j] {
				match = false
				break
			}
		}
		if match {
			foundInWindow = true
			foundRow = i
			break
		}
	}

	if foundInWindow {
		// Row is in the current window, just select it
		e.table.Select(foundRow, col)
		e.SetStatusMessage("Match found")
	} else {
		// Row is outside the current window, need to load from that row
		if foundBelow {
			// Found row is after current window, load from bottom (reverse order)
			if err := e.loadFromRowId(foundKeys, false, col); err != nil {
				e.SetStatusErrorWithSentry(err)
				return
			}
		} else {
			// Found row wrapped around to before current window, load from top
			if err := e.loadFromRowId(foundKeys, true, col); err != nil {
				e.SetStatusErrorWithSentry(err)
				return
			}
		}
		e.SetStatusMessage("Match found")
	}
}

// selectTableFromPicker handles selecting a table or view from the picker
func (e *Editor) selectTableFromPicker(tableName string) {
	fmt.Fprintf(os.Stderr, "[DEBUG] selectTableFromPicker: %s\n", tableName)
	e.pages.HidePage(pagePicker)
	e.app.SetAfterDrawFunc(nil) // Clear cursor style function
	e.setCursorStyle(0)         // Reset to default cursor style
	fmt.Fprintf(os.Stderr, "[DEBUG] Picker closed\n")

	// Reload the relation (table or view) data using the current database connection
	relation, err := dblib.NewRelation(e.db, e.dbType, tableName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[DEBUG] Error creating relation: %v\n", err)
		e.SetStatusErrorWithSentry(err)
		return
	}

	// Check if relation is keyable (has keys) - required for viewing
	if len(relation.Key) == 0 {
		e.SetStatusError(fmt.Sprintf("Relation %s has no keyable columns and cannot be viewed", tableName))
		return
	}

	// Update the relation
	fmt.Fprintf(os.Stderr, "[DEBUG] Relation created successfully\n")
	e.relation = relation

	// Reset columns
	fmt.Fprintf(os.Stderr, "[DEBUG] Resetting columns\n")
	e.columns = make([]dblib.DisplayColumn, 0, len(e.relation.Columns))
	keyColNames := make(map[string]bool)
	for _, keyIdx := range e.relation.Key {
		if keyIdx < len(e.relation.Columns) {
			keyColNames[e.relation.Columns[keyIdx].Name] = true
			e.columns = append(e.columns, dblib.DisplayColumn{Name: e.relation.Columns[keyIdx].Name, Width: 4})
		}
	}
	for _, col := range e.relation.Columns {
		if !keyColNames[col.Name] {
			e.columns = append(e.columns, dblib.DisplayColumn{Name: col.Name, Width: 8})
		}
	}

	// Update table headers
	fmt.Fprintf(os.Stderr, "[DEBUG] Updating table headers\n")
	headers := make([]HeaderColumn, len(e.columns))
	for i, col := range e.columns {
		headers[i] = HeaderColumn{
			Name:  col.Name,
			Width: col.Width,
		}
	}
	e.table.SetHeaders(headers).SetKeyColumnCount(len(e.relation.Key)).SetTableName(tableName).SetVimMode(e.vimMode)

	// Reload data from the beginning
	fmt.Fprintf(os.Stderr, "[DEBUG] Loading data from beginning\n")
	e.pointer = 0
	e.buffer = make([]Row, e.table.rowsHeight)
	e.loadFromRowId(nil, true, 0)
	e.renderData()
	e.table.Select(0, 0)
	fmt.Fprintf(os.Stderr, "[DEBUG] Data loaded and rendered\n")

	// Update title
	fmt.Fprintf(os.Stderr, "[DEBUG] Updating title\n")
	e.app.SetTitle(fmt.Sprintf("ted %s/%s %s", e.config.Database, tableName, databaseIcons[e.relation.DBType]))

	// Set focus back to table
	fmt.Fprintf(os.Stderr, "[DEBUG] Setting focus to table\n")
	e.app.SetFocus(e.table)
}
