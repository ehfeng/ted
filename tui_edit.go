package main

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

func (e *Editor) enterEditMode(row, col int) {
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
			if col < len(e.columns)-1 {
				e.table.Select(row, col+1)
			} else if row < len(e.records)-1 {
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
			if col < len(e.columns)-1 {
				e.table.Select(row, col+1)
			} else if row < len(e.records)-1 {
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
	if currentText == NullGlyph {
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
	for i := 0; i < len(e.columns); i++ {
		totalTableWidth += e.table.GetColumnWidth(i)
	}
	// Add space for borders and separators: left border + (n-1 separators * 3) + right border
	totalTableWidth += 1 + (len(e.columns)-1)*3 + 1

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
			preview := e.relation.BuildInsertPreview(e.table.insertRow, e.columns)
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
	colName := e.columns[col].Name
	if isNewRecordRow {
		// Show INSERT preview and set palette mode to Insert
		e.setPaletteMode(PaletteModeInsert, false)
		newRecordRow := make([]any, len(e.table.insertRow))
		copy(newRecordRow, e.table.insertRow)
		newRecordRow[col] = newText
		preview = e.relation.BuildInsertPreview(newRecordRow, e.columns)
	} else {
		// Show UPDATE preview and set palette mode to Update
		e.setPaletteMode(PaletteModeUpdate, false)
		// Convert records to [][]any for BuildUpdatePreview
		recordsData := make([][]any, len(e.records))
		for i := range e.records {
			recordsData[i] = e.records[i].data
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
		if newValue == NullGlyph {
			e.table.insertRow[col] = nil
		} else {
			e.table.insertRow[col] = newValue
		}
		e.exitEditMode()
		return
	}

	// Basic bounds check against current data
	if row < 0 || row >= len(e.records) || col < 0 || col >= len(e.records[row].data) {
		e.exitEditMode()
		return
	}

	// Delegate DB work to database.go
	// Convert records to [][]any for UpdateDBValue
	recordsData := make([][]any, len(e.records))
	for i := range e.records {
		recordsData[i] = e.records[i].data
	}
	updated, err := e.relation.UpdateDBValue(recordsData, row, e.columns[col].Name, newValue)
	if err != nil {
		e.exitEditMode()
		return
	}
	// Update in-memory data and refresh table
	ptr := (row + e.pointer) % len(e.records)
	e.records[ptr] = Row{state: RowStateNormal, data: updated}

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
	keyVals := make([]any, len(e.relation.key))
	for i, keyCol := range e.relation.key {
		keyIdx, ok := e.relation.attributeIndex[keyCol]
		if !ok || insertedRow == nil {
			// Fallback: load from bottom without specific row
			e.loadFromRowId(nil, false, 0, false)
			e.table.Select(len(e.records)-2, 0)
			return nil
		}
		keyVals[i] = insertedRow[keyIdx]
	}

	// Load from the inserted row (from bottom)
	if err := e.loadFromRowId(keyVals, false, e.table.selectedCol, false); err != nil {
		e.SetStatusErrorWithSentry(err)
		return err
	}

	// Select the first row (which should be the newly inserted one)
	if e.records[e.lastRowIdx()].data == nil {
		e.table.Select(len(e.records)-2, e.table.selectedCol)
	} else {
		e.table.Select(len(e.records)-1, e.table.selectedCol)
	}
	return nil
}

func (e *Editor) isMultilineColumnType(col int) bool {
	if e.relation == nil || col < 0 || col >= len(e.columns) {
		return false
	}

	column := e.columns[col]
	attrType := ""
	if attr, ok := e.relation.attributes[column.Name]; ok {
		attrType = attr.Type
	}

	// Normalize type for consistent matching
	attrType = strings.ToLower(attrType)

	// Check for text-compatible data types that support multiline content
	return strings.Contains(attrType, "char") ||
		strings.Contains(attrType, "varchar") ||
		strings.Contains(attrType, "text") ||
		attrType == "json" // json but not jsonb
}
