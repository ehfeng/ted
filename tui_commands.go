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
		e.SetStatusError("Transactions are not supported")
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
	colName := e.table.GetHeaders()[col].Name
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

// validateAndCleanSQL validates SQL input, removes trailing semicolons,
// and checks for multiple statements. Returns empty string if invalid.
func (e *Editor) validateAndCleanSQL(sqlStr string) string {
	sqlStr = strings.TrimSpace(sqlStr)

	// Remove trailing semicolon if present
	sqlStr = strings.TrimSuffix(sqlStr, ";")
	sqlStr = strings.TrimSpace(sqlStr)

	// Check for multiple statements by looking for semicolons
	// (after removing the trailing one, any remaining semicolon indicates multiple statements)
	if strings.Contains(sqlStr, ";") {
		e.app.Stop()
		fmt.Fprintln(os.Stderr, "Error: Multiple SQL statements are not supported. Please enter a single SELECT or WITH query.")
		return ""
	}

	return sqlStr
}

// selectTableFromPicker handles selecting a table or view from the picker
func (e *Editor) selectTableFromPicker(tableName string) {
	e.pages.HidePage(pagePicker)
	e.app.SetAfterDrawFunc(nil) // Clear cursor style function
	e.setCursorStyle(0)         // Reset to default cursor style

	var relation *dblib.Relation
	var displayName string
	var err error

	// Check if this is SQL input
	if strings.HasPrefix(tableName, "[Execute SQL] ") {
		sqlStr := strings.TrimPrefix(tableName, "[Execute SQL] ")
		sqlStr = e.validateAndCleanSQL(sqlStr)
		if sqlStr == "" {
			return // Error already handled
		}
		relation, err = dblib.NewRelationFromSQL(e.db, e.dbType, sqlStr)
		displayName = sqlStr
	} else {
		// Check if it's SQL without the prefix
		searchUpper := strings.ToUpper(strings.TrimSpace(tableName))
		isSQL := strings.HasPrefix(searchUpper, "SELECT") || strings.HasPrefix(searchUpper, "WITH")

		if isSQL {
			cleanedSQL := e.validateAndCleanSQL(tableName)
			if cleanedSQL == "" {
				return // Error already handled
			}
			relation, err = dblib.NewRelationFromSQL(e.db, e.dbType, cleanedSQL)
			displayName = cleanedSQL
		} else {
			// It's a table/view name
			relation, err = dblib.NewRelation(e.db, e.dbType, tableName)
			displayName = tableName
		}
	}

	if err != nil {
		if err == dblib.ErrNoKeyableColumns {
			e.app.Stop()
			errMsg := fmt.Sprintf("relation '%s' has no keyable columns and cannot be viewed", tableName)
			fmt.Fprintln(os.Stderr, errMsg)
			return
		}
		e.SetStatusErrorWithSentry(err)
		return
	}

	// Check if relation is keyable (has keys) - required for viewing
	if len(relation.Key) == 0 {
		if relation.IsCustomSQL {
			e.SetStatusError("Custom SQL has no keyable columns and cannot be viewed")
		} else {
			e.SetStatusError(fmt.Sprintf("Relation %s has no keyable columns and cannot be viewed", displayName))
		}
		return
	}

	// Update the relation
	e.relation = relation

	// Build table headers in database schema order
	headers := make([]dblib.DisplayColumn, 0, len(e.relation.Columns))
	for i, col := range e.relation.Columns {
		isKey := false
		for _, keyIdx := range e.relation.Key {
			if keyIdx == i {
				isKey = true
				break
			}
		}
		editable := e.relation.IsColumnEditable(i)
		headers = append(headers, dblib.DisplayColumn{Name: col.Name, Width: DefaultColumnWidth, IsKey: isKey, Editable: editable})
	}
	e.table.SetHeaders(headers).SetTableName(displayName).SetVimMode(e.vimMode)

	// Reload data from the beginning
	e.pointer = 0
	e.buffer = make([]Row, e.table.rowsHeight)
	e.loadFromRowId(nil, true, 0)
	e.renderData()
	e.table.Select(0, 0)

	// Update title
	if e.relation.IsCustomSQL {
		e.app.SetTitle(fmt.Sprintf("ted %s/[SQL Query] %s", e.config.Database, databaseIcons[e.relation.DBType]))
	} else {
		e.app.SetTitle(fmt.Sprintf("ted %s/%s %s", e.config.Database, displayName, databaseIcons[e.relation.DBType]))
	}

	// Set focus back to table
	e.app.SetFocus(e.table)
}
