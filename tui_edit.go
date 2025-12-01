package main

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"ted/internal/dblib"
)

func (e *Editor) enterEditMode(row, col int) {
	// Check if column is editable (for views)
	if e.relation != nil && e.relation.IsView {
		colIdx, ok := e.relation.ColumnIndex[e.table.GetHeaders()[col].Name]
		if !ok || !e.relation.IsColumnEditable(colIdx) {
			e.SetStatusMessage("Column is not editable")
			return
		}
	}

	var currentValue any
	if len(e.table.insertRow) > 0 {
		currentValue = e.table.insertRow[col]
	} else {
		currentValue = e.table.GetCell(row, col)
	}
	currentText := ""
	if currentValue != nil {
		currentText, _ = formatCellValue(currentValue, tcell.StyleDefault)
	}
	e.enterEditModeWithInitialValue(row, col, currentText)
}

func (e *Editor) enterEditModeWithInitialValue(row, col int, initialText string) {
	// must be set before any calls to app.Draw()
	e.editing = true
	// In insert mode, allow editing the virtual insert mode row
	// The virtual row index equals the length of the data array
	// (which is shorter than records in insert mode)
	isNewRecordRow := len(e.table.insertRow) > 0 && row == e.table.GetDataLength()

	// Create textarea for editing with proper styling
	textArea := tview.NewTextArea().
		SetText(initialText, true).
		SetWrap(true).
		SetOffset(0, 0)

	textArea.SetBorder(false)

	// Store references for dynamic resizing
	var modal tview.Primitive
	// Set the table selection to track which cell is being edited
	e.table.Select(row, col)

	// Function to resize textarea based on current content
	resizeTextarea := func() {
		e.pages.RemovePage("editor")
		modal = e.createCellEditOverlay(textArea, row, col, textArea.GetText())
		e.pages.AddPage("editor", modal, true, true)
		textArea.SetOffset(0, 0)
	}

	// Handle textarea input capture for save/cancel
	textArea.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		key := event.Key()
		rune := event.Rune()
		mod := event.Modifiers()

		// Ctrl+S: execute INSERT in insert mode (save and insert)
		if (rune == 's' || rune == 19) && mod&tcell.ModCtrl != 0 {
			if len(e.table.insertRow) > 0 {
				// Save current cell first
				newText := textArea.GetText()
				e.table.insertRow[col] = newText
				e.exitEditMode()
				e.executeInsert()
				return nil
			}
		}

		switch key {
		case tcell.KeyEnter:
			// Check if Alt/Option is pressed with Enter
			if event.Modifiers()&tcell.ModAlt != 0 {
				// Alt+Enter: only allow newlines for text-compatible column types
				if e.isMultilineColumnType(col) {
					// Manually insert newline instead of relying on default handler
					currentText := textArea.GetText()
					// Simply append newline at the end for now
					newText := currentText + "\n"
					textArea.SetText(newText, true)
					resizeTextarea()
					return nil
				}
				// For other types, treat as regular Enter (save and exit)
				newText := textArea.GetText()
				e.updateCell(row, col, newText)
				return nil
			}
			// Plain Enter: save and exit
			newText := textArea.GetText()
			e.updateCell(row, col, newText)
			return nil
		case tcell.KeyTab:
			// Tab: save and move to next cell
			newText := textArea.GetText()
			e.updateCell(row, col, newText)
			// Move selection right
			if col < len(e.table.GetHeaders())-1 {
				e.table.Select(row, col+1)
			} else if row < len(e.buffer)-1 {
				e.table.Select(row+1, 0)
			}
			return nil
		case tcell.KeyEscape:
			e.exitEditMode()
			return nil
		}
		return event
	})
	// Position the textarea to align with the cell
	modal = e.createCellEditOverlay(textArea, row, col, initialText)
	// Set editMode early to prevent resize handler from running during AddPage
	e.pages.AddPage("editor", modal, true, true)

	// Set up dynamic resizing on text changes and update SQL preview AFTER initial page add
	textArea.SetChangedFunc(func() {
		resizeTextarea()
		e.updateEditPreview(textArea.GetText())
	})
	// Set up native cursor positioning using terminal escapes
	e.app.SetAfterDrawFunc(func(screen tcell.Screen) {
		screen.SetCursorStyle(tcell.CursorStyleBlinkingBar)
	})
	e.app.SetFocus(textArea)

	// Set palette mode based on whether we're in insert mode or update mode
	if isNewRecordRow {
		e.setPaletteMode(PaletteModeInsert, false)
		e.updateEditPreview(initialText)
		e.updateStatusForEditMode(col)
	} else {
		e.setPaletteMode(PaletteModeUpdate, false)
		e.updateStatusForEditMode(col)
	}
}

// enterEditModeWithSelection enters edit mode with optional text selection
// selectAll=true: select all text (for vim 'i' mode)
// selectAll=false: cursor at end (for vim 'a' mode)
func (e *Editor) enterEditModeWithSelection(row, col int, selectAll bool) {
	// Check if column is editable (for views)
	if e.relation != nil && e.relation.IsView {
		colIdx, ok := e.relation.ColumnIndex[e.table.GetHeaders()[col].Name]
		if !ok || !e.relation.IsColumnEditable(colIdx) {
			e.SetStatusMessage("Column is not editable")
			return
		}
	}

	var currentValue any
	if len(e.table.insertRow) > 0 {
		currentValue = e.table.insertRow[col]
	} else {
		currentValue = e.table.GetCell(row, col)
	}
	currentText := ""
	if currentValue != nil {
		currentText, _ = formatCellValue(currentValue, tcell.StyleDefault)
	}

	// must be set before any calls to app.Draw()
	e.editing = true
	// In insert mode, allow editing the virtual insert mode row
	// The virtual row index equals the length of the data array
	// (which is shorter than records in insert mode)
	isNewRecordRow := len(e.table.insertRow) > 0 && row == e.table.GetDataLength()

	// Create textarea for editing with proper styling
	// Set cursor position: true for end, false for beginning
	textArea := tview.NewTextArea().
		SetText(currentText, !selectAll). // If selectAll, start at beginning; otherwise at end
		SetWrap(true).
		SetOffset(0, 0)

	textArea.SetBorder(false)

	// Store references for dynamic resizing
	var modal tview.Primitive
	// Set the table selection to track which cell is being edited
	e.table.Select(row, col)

	// Function to resize textarea based on current content
	resizeTextarea := func() {
		e.pages.RemovePage("editor")
		modal = e.createCellEditOverlay(textArea, row, col, textArea.GetText())
		e.pages.AddPage("editor", modal, true, true)
		textArea.SetOffset(0, 0)
	}

	// Handle textarea input capture for save/cancel
	textArea.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		key := event.Key()
		rune := event.Rune()
		mod := event.Modifiers()

		// Ctrl+S: execute INSERT in insert mode (save and insert)
		if (rune == 's' || rune == 19) && mod&tcell.ModCtrl != 0 {
			if len(e.table.insertRow) > 0 {
				// Save current cell first
				newText := textArea.GetText()
				e.table.insertRow[col] = newText
				e.exitEditMode()
				e.executeInsert()
				return nil
			}
		}

		switch key {
		case tcell.KeyEnter:
			// Check if Alt/Option is pressed with Enter
			if event.Modifiers()&tcell.ModAlt != 0 {
				// Alt+Enter: only allow newlines for text-compatible column types
				if e.isMultilineColumnType(col) {
					// Manually insert newline instead of relying on default handler
					currentText := textArea.GetText()
					// Simply append newline at the end for now
					newText := currentText + "\n"
					textArea.SetText(newText, true)
					resizeTextarea()
					return nil
				}
				// For other types, treat as regular Enter (save and exit)
				newText := textArea.GetText()
				e.updateCell(row, col, newText)
				return nil
			}
			// Plain Enter: save and exit
			newText := textArea.GetText()
			e.updateCell(row, col, newText)
			return nil
		case tcell.KeyTab:
			// Tab: save and move to next cell
			newText := textArea.GetText()
			e.updateCell(row, col, newText)
			// Move selection right
			if col < len(e.table.GetHeaders())-1 {
				e.table.Select(row, col+1)
			} else if row < len(e.buffer)-1 {
				e.table.Select(row+1, 0)
			}
			return nil
		case tcell.KeyEscape:
			e.exitEditMode()
			return nil
		}
		return event
	})
	// Position the textarea to align with the cell
	modal = e.createCellEditOverlay(textArea, row, col, currentText)
	// Set editMode early to prevent resize handler from running during AddPage
	e.pages.AddPage("editor", modal, true, true)

	// Set up dynamic resizing on text changes and update SQL preview AFTER initial page add
	textArea.SetChangedFunc(func() {
		resizeTextarea()
		e.updateEditPreview(textArea.GetText())
	})
	// Set up native cursor positioning and select all text if needed
	// Use a closure variable to ensure Select only runs once after the first draw
	selectTextOnce := selectAll && len(currentText) > 0
	textLen := len(currentText) // Capture length in closure
	e.app.SetAfterDrawFunc(func(screen tcell.Screen) {
		screen.SetCursorStyle(tcell.CursorStyleBlinkingBar)
		if selectTextOnce {
			selectTextOnce = false // Disable future selections
			textArea.Select(0, textLen)
		}
	})
	e.app.SetFocus(textArea)

	// Set palette mode based on whether we're in insert mode or update mode
	if isNewRecordRow {
		e.setPaletteMode(PaletteModeInsert, false)
		e.updateEditPreview(currentText)
		e.updateStatusForEditMode(col)
	} else {
		e.setPaletteMode(PaletteModeUpdate, false)
		e.updateStatusForEditMode(col)
	}
}

func (e *Editor) createCellEditOverlay(textArea *tview.TextArea, row, col int,
	currentText string) tview.Primitive {
	// Get the current text to calculate minimum size
	currentText, _ = formatCellValue(currentText, tcell.StyleDefault)
	if currentText == dblib.NullGlyph {
		textArea.SetTextStyle(textArea.GetTextStyle().Italic(true))
	} else {
		textArea.SetTextStyle(textArea.GetTextStyle().Italic(false))
	}

	// Calculate the position where the cell content appears on screen
	// Layout structure: picker bar (row 0) + table at row 1
	// TableView structure: top border (row 0) + header (row 1) + separator (row 2) + data rows (3+)
	tableRow := row + 3       // Convert data row to table display row
	screenRow := tableRow + 1 // Offset by 1 for picker bar

	// Calculate horizontal position: left border + previous columns + cell padding
	leftOffset := 1 // Left table border "│"
	for i := 0; i < col; i++ {
		leftOffset += e.table.GetColumnWidth(i) + 2 + 1 // width + " │ " padding + separator
	}
	leftOffset += 1 // Cell padding (space after "│ ")
	leftOffset -= 1 // Move overlay one position to the left

	// Account for viewport horizontal scrolling
	leftOffset -= e.table.viewport.GetScrollX()

	// Calculate vertical position relative to screen (accounting for picker bar offset)
	topOffset := screenRow

	// Calculate minimum textarea size based on column width
	cellWidth := e.table.GetColumnWidth(col)

	// Calculate total table width for maximum textarea width
	totalTableWidth := 0
	for i := 0; i < len(e.table.GetHeaders()); i++ {
		totalTableWidth += e.table.GetColumnWidth(i)
	}
	// Add space for borders and separators: left border + (n-1 separators * 3) + right border
	totalTableWidth += 1 + (len(e.table.GetHeaders())-1)*3 + 1

	// Calculate the maximum available width for the textarea
	maxAvailableWidth := totalTableWidth - leftOffset + 1 // Account for right border

	// First, try with cell width to see if content fits
	textLines := strings.Split(currentText, "\n")
	longestLine := 0
	for _, line := range textLines {
		if len(line) > longestLine {
			longestLine = len(line)
		}
	}

	// Calculate desired width based on content
	desiredWidth := max(cellWidth, longestLine) + 2
	textAreaWidth := min(desiredWidth, maxAvailableWidth)
	// If we're using the capped width, recalculate text lines with the actual textarea width
	// if textAreaWidth < desiredWidth {
	// 	textLines = splitTextToLines(currentText, textAreaWidth-2) // Account for padding
	// }

	// Minimum height: number of lines needed, minimum 1
	textAreaHeight := max(len(textLines), 1)

	// Create positioned overlay that aligns text with the original cell
	leftPadding := tview.NewBox()
	return tview.NewFlex().
		AddItem(nil, leftOffset, 0, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, topOffset, 0, false).
			AddItem(tview.NewFlex().
				AddItem(leftPadding, 1, 0, false).           // Left padding
				AddItem(textArea, textAreaWidth-1, 0, true), // Text area
				textAreaHeight, 0, true).
			AddItem(nil, 0, 1, false), textAreaWidth, 0, true).
		AddItem(nil, 0, 1, false)
}

func (e *Editor) setCursorStyle(style int) {
	fmt.Printf("\033[%d q", style)
}

func (e *Editor) exitEditMode() {
	if e.editing {
		e.pages.RemovePage("editor")
		e.app.SetAfterDrawFunc(nil) // Clear the cursor function
		e.setCursorStyle(0)         // Reset to default cursor style
		e.app.SetFocus(e.table)
		e.editing = false
		// Return palette to default mode after editing
		if e.table.insertRow == nil {
			e.setPaletteMode(PaletteModeDefault, false)
			// Trigger status update via selection change callback
			row, col := e.table.GetSelection()
			if e.table.selectionChangeFunc != nil {
				e.table.selectionChangeFunc(row, col)
			}
		} else {
			preview := e.relation.BuildInsertPreview(e.table.insertRow, e.table.GetHeaders())
			e.commandPalette.SetPlaceholder(preview)
			e.updateStatusForInsertMode()
		}
	}
}

func (e *Editor) updateStatusForInsertMode() {
	e.SetStatusMessage("Ctrl+S to insert · Esc to cancel")
}

// Update the command palette to show a SQL UPDATE/INSERT preview while editing
func (e *Editor) updateEditPreview(newText string) {
	if e.relation == nil || !e.editing {
		return
	}

	row, col := e.table.GetSelection()
	// Check if we're in insert mode
	isNewRecordRow := len(e.table.insertRow) > 0 && row == e.table.GetDataLength()

	var preview string
	colName := e.table.GetHeaders()[col].Name
	if isNewRecordRow {
		// Show INSERT preview and set palette mode to Insert
		e.setPaletteMode(PaletteModeInsert, false)
		newRecordRow := make([]any, len(e.table.insertRow))
		copy(newRecordRow, e.table.insertRow)
		newRecordRow[col] = newText
		preview = e.relation.BuildInsertPreview(newRecordRow, e.table.GetHeaders())
	} else {
		// Show UPDATE preview and set palette mode to Update
		e.setPaletteMode(PaletteModeUpdate, false)
		// Convert records to [][]any for BuildUpdatePreview
		recordsData := make([][]any, len(e.buffer))
		for i := range e.buffer {
			recordsData[i] = e.buffer[i].data
		}
		preview = e.relation.BuildUpdatePreview(recordsData, row, colName, newText)
	}
	e.commandPalette.SetPlaceholderStyle(e.commandPalette.GetPlaceholderStyle())
	e.commandPalette.SetPlaceholder(preview)

	// Update status bar with foreign key preview if this column has a reference (only for UPDATE)
	if !isNewRecordRow {
		e.updateForeignKeyPreview(row, col, newText)
	}
}

func (e *Editor) updateCell(row, col int, newValue string) {
	// Check if this is an insert mode row
	isNewRecordRow := len(e.table.insertRow) > 0 && row == e.table.GetDataLength()

	if isNewRecordRow {
		// Update the insert mode row with the new value
		if newValue == dblib.NullGlyph {
			e.table.insertRow[col] = nil
		} else {
			e.table.insertRow[col] = newValue
		}
		e.exitEditMode()
		return
	}

	// Basic bounds check against current data
	if row < 0 || row >= len(e.buffer) || col < 0 || col >= len(e.buffer[row].data) {
		e.exitEditMode()
		return
	}

	// Save old row data for comparison
	ptr := (row + e.pointer) % len(e.buffer)
	oldRow := make([]any, len(e.buffer[ptr].data))
	copy(oldRow, e.buffer[ptr].data)

	// Delegate DB work to database.go
	// Convert records to [][]any for UpdateDBValue
	recordsData := make([][]any, len(e.buffer))
	for i := range e.buffer {
		recordsData[i] = e.buffer[i].data
	}
	updated, err := e.relation.UpdateDBValue(recordsData, row, e.table.GetHeaders()[col].Name, newValue)
	if err != nil {
		e.exitEditMode()
		return
	}

	// Check if key or sort column values changed
	// Note: sortCol is currently nil in most cases, but we handle it for future support
	if e.hasKeyOrSortChanged(oldRow, updated, nil) {
		// Extract updated key values
		updatedKeys := e.extractKeys(updated)
		if updatedKeys == nil {
			// Fallback: update in place if we can't extract keys
			e.buffer[ptr] = Row{state: RowStateNormal, data: updated}
			e.renderData()
			e.exitEditMode()
			return
		}

		// First, scan current buffer to see if the updated row is still visible
		foundInBuffer := false
		foundRow := -1
		for i := 0; i < len(e.buffer); i++ {
			bufIdx := (e.pointer + i) % len(e.buffer)
			if e.buffer[bufIdx].data == nil {
				break // Hit nil sentinel, stop scanning
			}
			bufKeys := e.extractKeys(e.buffer[bufIdx].data)
			if keysEqual(bufKeys, updatedKeys) {
				foundInBuffer = true
				foundRow = i
				break
			}
		}

		if foundInBuffer {
			// Row is still in viewport, just update it and select it
			bufIdx := (e.pointer + foundRow) % len(e.buffer)
			e.buffer[bufIdx] = Row{state: RowStateNormal, data: updated}
			e.renderData()
			e.table.Select(foundRow, col)
			e.exitEditMode()
			return
		}
		// Row not in buffer, use database query to determine position
		firstKeys, lastKeys := e.getViewportBoundaryKeys()
		if firstKeys == nil || lastKeys == nil {
			// Fallback: can't determine position, load from bottom
			if err := e.loadFromRowId(updatedKeys, false, col); err != nil {
				e.SetStatusErrorWithSentry(err)
				e.exitEditMode()
				return
			}
			// Select last row
			if e.buffer[e.lastRowIdx()].data == nil {
				e.table.Select(len(e.buffer)-2, col)
			} else {
				e.table.Select(len(e.buffer)-1, col)
			}
			e.exitEditMode()
			return
		}

		// Query database to compare row positions
		isAbove, isBelow, err := dblib.CompareRowPosition(e.db, e.relation, nil, updatedKeys, firstKeys, lastKeys)
		if err != nil {
			// On error, fall back to loading from bottom
			e.SetStatusErrorWithSentry(err)
			e.exitEditMode()
			return
		}

		// Reposition viewport based on comparison result
		if isAbove {
			// Row is above viewport, load from top
			if err := e.loadFromRowId(updatedKeys, true, col); err != nil {
				e.SetStatusErrorWithSentry(err)
				e.exitEditMode()
				return
			}
			// Select first row
			e.table.Select(0, col)
			e.exitEditMode()
			return
		} else if isBelow {
			// Row is below viewport, load from bottom
			if err := e.loadFromRowId(updatedKeys, false, col); err != nil {
				e.SetStatusErrorWithSentry(err)
				e.exitEditMode()
				return
			}
			// Select last row
			if e.buffer[e.lastRowIdx()].data == nil {
				e.table.Select(len(e.buffer)-2, col)
			} else {
				e.table.Select(len(e.buffer)-1, col)
			}
			e.exitEditMode()
			return
		} else {
			// Both false or both true - row might be in viewport after all
			// Load from bottom as fallback
			if err := e.loadFromRowId(updatedKeys, false, col); err != nil {
				e.SetStatusErrorWithSentry(err)
				e.exitEditMode()
				return
			}
			// Select last row
			if e.buffer[e.lastRowIdx()].data == nil {
				e.table.Select(len(e.buffer)-2, col)
			} else {
				e.table.Select(len(e.buffer)-1, col)
			}
			e.exitEditMode()
			return
		}
	}

	// Key/sort columns didn't change, update in place (original behavior)
	e.buffer[ptr] = Row{state: RowStateNormal, data: updated}
	e.renderData()
	e.exitEditMode()
}

// executeInsert executes the INSERT statement for the new record row
func (e *Editor) executeInsert() error {
	if len(e.table.insertRow) == 0 {
		return fmt.Errorf("no new record to insert")
	}

	// Execute the insert
	insertedRow, err := e.relation.InsertDBRecord(e.table.insertRow)
	if err != nil {
		e.SetStatusErrorWithSentry(err)
		return err
	}

	// Exit insert mode
	e.table.ClearInsertRow()
	// TODO focus on inserted row
	e.SetStatusMessage("Record inserted successfully")

	// Extract the key values from the inserted row
	keyVals := make([]any, len(e.relation.Key))
	for i, keyIdx := range e.relation.Key {
		if keyIdx >= len(insertedRow) || insertedRow == nil {
			// Fallback: load from bottom without specific row
			e.loadFromRowId(nil, false, 0)
			e.table.Select(len(e.buffer)-2, 0)
			return nil
		}
		keyVals[i] = insertedRow[keyIdx]
	}

	// Load from the inserted row (from bottom)
	if err := e.loadFromRowId(keyVals, false, e.table.selectedCol); err != nil {
		e.SetStatusErrorWithSentry(err)
		return err
	}

	// Select the first row (which should be the newly inserted one)
	if e.buffer[e.lastRowIdx()].data == nil {
		e.table.Select(len(e.buffer)-2, e.table.selectedCol)
	} else {
		e.table.Select(len(e.buffer)-1, e.table.selectedCol)
	}
	return nil
}

// hasKeyOrSortChanged checks if any key columns or sort column values changed
func (e *Editor) hasKeyOrSortChanged(oldRow, newRow []any, sortCol *dblib.SortColumn) bool {
	if oldRow == nil || newRow == nil {
		return false
	}

	// Check if any key column values changed
	for _, keyIdx := range e.relation.Key {
		if keyIdx < len(oldRow) && keyIdx < len(newRow) {
			if oldRow[keyIdx] != newRow[keyIdx] {
				return true
			}
		}
	}

	// Check if sort column value changed (if sorting is applied)
	if sortCol != nil {
		sortIdx, ok := e.relation.ColumnIndex[sortCol.Name]
		if ok && sortIdx < len(oldRow) && sortIdx < len(newRow) {
			if oldRow[sortIdx] != newRow[sortIdx] {
				return true
			}
		}
	}

	return false
}

func (e *Editor) isMultilineColumnType(col int) bool {
	if e.relation == nil || col < 0 || col >= len(e.table.GetHeaders()) {
		return false
	}

	column := e.table.GetHeaders()[col]
	attrType := ""
	if colIdx, ok := e.relation.ColumnIndex[column.Name]; ok && colIdx < len(e.relation.Columns) {
		attrType = e.relation.Columns[colIdx].Type
	}

	// Normalize type for consistent matching
	attrType = strings.ToLower(attrType)

	// Check for text-compatible data types that support multiline content
	return strings.Contains(attrType, "char") ||
		strings.Contains(attrType, "varchar") ||
		strings.Contains(attrType, "text") ||
		attrType == "json" // json but not jsonb
}
