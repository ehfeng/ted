package main

import (
	"fmt"
	"os"
	"slices"
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
	for i := range e.relation.Key {
		currentKeys[i] = e.buffer[row].data[i]
	}

	// Find the column index in relation.attributeOrder
	findCol := -1
	for i, name := range e.relation.AttributeOrder {
		if name == e.columns[col].Name {
			findCol = i
			break
		}
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
		for j := range e.relation.Key {
			if e.buffer[i].data[j] != foundKeys[j] {
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

// selectTableFromPicker handles selecting a table from the picker
func (e *Editor) selectTableFromPicker(tableName string) {
	fmt.Fprintf(os.Stderr, "[DEBUG] selectTableFromPicker: %s\n", tableName)
	e.pages.HidePage(pagePicker)
	e.app.SetAfterDrawFunc(nil) // Clear cursor style function
	e.setCursorStyle(0)         // Reset to default cursor style
	fmt.Fprintf(os.Stderr, "[DEBUG] Picker closed\n")

	// Reload the table data using the current database connection
	relation, err := dblib.NewRelation(e.db, e.dbType, tableName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[DEBUG] Error creating relation: %v\n", err)
		e.SetStatusErrorWithSentry(err)
		return
	}

	// Update the relation
	fmt.Fprintf(os.Stderr, "[DEBUG] Relation created successfully\n")
	e.relation = relation

	// Reset columns
	fmt.Fprintf(os.Stderr, "[DEBUG] Resetting columns\n")
	e.columns = make([]dblib.Column, 0, len(e.relation.AttributeOrder))
	for _, name := range e.relation.Key {
		e.columns = append(e.columns, dblib.Column{Name: name, Width: 4})
	}
	for _, name := range e.relation.AttributeOrder {
		if !slices.Contains(e.relation.Key, name) {
			e.columns = append(e.columns, dblib.Column{Name: name, Width: 8})
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
