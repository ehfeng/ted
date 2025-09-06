package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type Editor struct {
	app          *tview.Application
	pages        *tview.Pages
	table        *HeaderTable
	data         *Table
	db           *sql.DB
	dbType       DatabaseType
	config       *Config
	tableSpec    string
	currentRow   int
	currentCol   int
	editMode     bool
	selectedRows map[int]bool
	selectedCols map[int]bool
}

func runEditor(config *Config, tableSpec string) error {
	db, dbType, err := config.connect()
	if err != nil {
		return err
	}
	defer db.Close()

	data, err := loadTable(db, dbType, tableSpec)
	if err != nil {
		return err
	}

	editor := &Editor{
		app:          tview.NewApplication(),
		pages:        tview.NewPages(),
		table:        NewHeaderTable(),
		data:         data,
		db:           db,
		dbType:       dbType,
		config:       config,
		tableSpec:    tableSpec,
		selectedRows: make(map[int]bool),
		selectedCols: make(map[int]bool),
	}

	editor.setupTable()
	editor.setupKeyBindings()

	editor.pages.AddPage("table", editor.table, true, true)

	if err := editor.app.SetRoot(editor.pages, true).EnableMouse(true).Run(); err != nil {
		return err
	}

	return nil
}

func (e *Editor) setupTable() {
	// Create headers for HeaderTable
	headers := make([]HeaderColumn, len(e.data.Columns))
	for i, col := range e.data.Columns {
		headers[i] = HeaderColumn{
			Name:  col.Name,
			Width: col.Width,
		}
	}

	// Configure the table
	e.table.SetHeaders(headers).SetData(e.data.Rows).SetSelectable(true)
}

func (e *Editor) setupKeyBindings() {
	e.table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		key := event.Key()
		rune := event.Rune()
		mod := event.Modifiers()

		row, col := e.table.GetSelection()

		switch {
		case key == tcell.KeyEnter:
			e.enterEditMode(row, col)
			return nil
		case key == tcell.KeyEscape:
			e.exitEditMode()
			return nil
		case key == tcell.KeyTab:
			e.navigateTab(false)
			return nil
		case key == tcell.KeyBacktab:
			e.navigateTab(true)
			return nil
		case key == tcell.KeyHome:
			e.table.Select(row, 0)
			return nil
		case key == tcell.KeyEnd:
			e.table.Select(row, len(e.data.Columns)-1)
			return nil
		case key == tcell.KeyPgUp:
			newRow := max(0, row-10)
			e.table.Select(newRow, col)
			return nil
		case key == tcell.KeyPgDn:
			newRow := min(len(e.data.Rows)-1, row+10)
			e.table.Select(newRow, col)
			return nil
		case rune == ' ' && mod&tcell.ModShift != 0:
			e.toggleRowSelection(row)
			return nil
		case rune == ' ' && mod&tcell.ModCtrl != 0:
			e.toggleColSelection(col)
			return nil
		case rune == 'r' && mod&tcell.ModCtrl != 0:
			e.refreshData()
			return nil
		case rune == 'f' && mod&tcell.ModCtrl != 0:
			return nil
		case key == tcell.KeyUp && mod&tcell.ModAlt != 0:
			e.moveRow(row, -1)
			return nil
		case key == tcell.KeyDown && mod&tcell.ModAlt != 0:
			e.moveRow(row, 1)
			return nil
		case key == tcell.KeyLeft && mod&tcell.ModAlt != 0:
			e.moveColumn(col, -1)
			return nil
		case key == tcell.KeyRight && mod&tcell.ModAlt != 0:
			e.moveColumn(col, 1)
			return nil
		case key == tcell.KeyLeft && mod&tcell.ModCtrl != 0:
			e.adjustColumnWidth(col, -2)
			return nil
		case key == tcell.KeyRight && mod&tcell.ModCtrl != 0:
			e.adjustColumnWidth(col, 2)
			return nil
		case key == tcell.KeyLeft && mod&tcell.ModMeta != 0:
			e.table.Select(row, 0)
			return nil
		case key == tcell.KeyRight && mod&tcell.ModMeta != 0:
			e.table.Select(row, len(e.data.Columns)-1)
			return nil
		case key == tcell.KeyUp && mod&tcell.ModMeta != 0:
			e.table.Select(0, col)
			return nil
		case key == tcell.KeyDown && mod&tcell.ModMeta != 0:
			e.table.Select(len(e.data.Rows)-1, col)
			return nil
		}

		return event
	})
}

func (e *Editor) navigateTab(reverse bool) {
	row, col := e.table.GetSelection()

	if reverse {
		if col > 0 {
			e.table.Select(row, col-1)
		} else if row > 0 {
			e.table.Select(row-1, len(e.data.Columns)-1)
		}
	} else {
		if col < len(e.data.Columns)-1 {
			e.table.Select(row, col+1)
		} else if row < len(e.data.Rows)-1 {
			e.table.Select(row+1, 0)
		}
	}
}

func (e *Editor) enterEditMode(row, col int) {
	// Get current value from data
	currentValue := e.table.GetCell(row, col)
	currentText := formatCellValue(currentValue)

	// Create textarea for editing with proper styling
	textArea := tview.NewTextArea().
		SetText(currentText, false).
		SetWrap(true)

	textArea.SetBorder(false).
		SetBackgroundColor(tcell.ColorWhite)

	// Move cursor to end of text by setting the text again with moveCursor=true
	if len(currentText) > 0 {
		// Set text with cursor at the end
		textArea.SetText(currentText, true)
	}

	// Handle textarea input capture for save/cancel
	textArea.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEnter:
			newText := textArea.GetText()
			e.updateCell(row, col, newText)
			return nil
		case tcell.KeyEscape:
			e.exitEditMode()
			return nil
		}
		return event
	})

	// Position the textarea to align with the cell
	modal := e.createCellEditOverlay(textArea, row, col)
	e.pages.AddPage("editor", modal, true, true)

	// Set up native cursor positioning using terminal escapes
	e.app.SetAfterDrawFunc(func(screen tcell.Screen) {
		if !e.editMode {
			return
		}

		// Hide the default tcell cursor first
		screen.HideCursor()
		screen.SetCursorStyle(tcell.CursorStyleBlinkingBar)

		// Get cursor position from textarea
		_, _, toRow, toCol := textArea.GetCursor()

		// Calculate screen position based on original cell position
		// Use the same calculation as createCellEditOverlay
		tableRow := row + 3 // Convert data row to table display row
		leftOffset := 1     // Left table border "│"
		for i := 0; i < col; i++ {
			leftOffset += e.table.GetColumnWidth(i) + 2 + 1 // width + " │ " padding + separator
		}
		leftOffset += 1 // Cell padding (space after "│ ")

		// Calculate cursor position relative to the cell content area
		cursorX := leftOffset + toCol
		cursorY := tableRow + toRow

		screen.ShowCursor(cursorX, cursorY)
	})

	// Set cursor to bar style (style 5 = blinking bar)
	e.setCursorStyle(5)

	e.app.SetFocus(textArea)
	e.editMode = true
}

func (e *Editor) createCellEditOverlay(textArea tview.Primitive, row, col int) tview.Primitive {
	// Get the current text to calculate minimum size
	currentValue := e.table.GetCell(row, col)
	currentText := formatCellValue(currentValue)

	// Calculate the position where the cell content appears on screen
	// HeaderTable structure: top border (row 0) + header (row 1) + separator (row 2) + data rows (3+)
	tableRow := row + 3 // Convert data row to table display row

	// Calculate horizontal position: left border + previous columns + cell padding
	leftOffset := 1 // Left table border "│"
	for i := 0; i < col; i++ {
		leftOffset += e.table.GetColumnWidth(i) + 2 + 1 // width + " │ " padding + separator
	}
	leftOffset += 1 // Cell padding (space after "│ ")

	// Calculate vertical position relative to table
	topOffset := tableRow

	// Calculate minimum textarea size based on content
	cellWidth := e.table.GetColumnWidth(col)

	// Calculate minimum width needed for the text content
	textLines := splitTextToLines(currentText, cellWidth)
	longestLine := 0
	for _, line := range textLines {
		if len(line) > longestLine {
			longestLine = len(line)
		}
	}

	// Minimum width: max of cell width or longest line length, with some padding
	minWidth := max(cellWidth, longestLine) + 2 // Add small padding
	textAreaWidth := min(minWidth, cellWidth*2) // Cap at 2x cell width

	// Minimum height: number of lines needed, minimum 1
	textAreaHeight := max(len(textLines), 1)

	// Create positioned overlay that aligns text with the original cell
	return tview.NewFlex().
		AddItem(nil, leftOffset, 0, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, topOffset, 0, false).
			AddItem(textArea, textAreaHeight, 0, true).
			AddItem(nil, 0, 1, false), textAreaWidth, 0, true).
		AddItem(nil, 0, 1, false)
}

func splitTextToLines(text string, maxWidth int) []string {
	if text == "" {
		return []string{""}
	}

	// Split by newlines first
	paragraphs := strings.Split(text, "\n")
	var lines []string

	for _, paragraph := range paragraphs {
		if paragraph == "" {
			lines = append(lines, "")
			continue
		}

		// Wrap lines that are too long
		for len(paragraph) > maxWidth {
			// Find a good break point (prefer spaces)
			breakPoint := maxWidth
			for i := maxWidth - 1; i > maxWidth/2 && i < len(paragraph); i-- {
				if paragraph[i] == ' ' {
					breakPoint = i
					break
				}
			}

			lines = append(lines, paragraph[:breakPoint])
			paragraph = strings.TrimLeft(paragraph[breakPoint:], " ")
		}

		if len(paragraph) > 0 {
			lines = append(lines, paragraph)
		}
	}

	return lines
}

func (e *Editor) setCursor(x, y int) {
	fmt.Printf("\033[%d;%dH", y+1, x+1)
}

func (e *Editor) setCursorStyle(style int) {
	fmt.Printf("\033[%d q", style)
}

func (e *Editor) exitEditMode() {
	if e.editMode {
		e.pages.RemovePage("editor")
		e.app.SetAfterDrawFunc(nil) // Clear the cursor function
		e.setCursorStyle(0)         // Reset to default cursor style
		e.app.SetFocus(e.table)
		e.editMode = false
	}
}

func (e *Editor) updateCell(row, col int, newValue string) {
	if row >= 0 && row < len(e.data.Rows) && col >= 0 && col < len(e.data.Rows[row]) {
		e.data.Rows[row][col] = newValue
		e.table.UpdateCell(row, col, newValue)
	}

	e.exitEditMode()
}

func (e *Editor) toggleRowSelection(row int) {
	if row == 0 {
		return
	}

	if e.selectedRows[row] {
		delete(e.selectedRows, row)
		e.unhighlightRow(row)
	} else {
		e.selectedRows[row] = true
		e.highlightRow(row)
	}
}

func (e *Editor) toggleColSelection(col int) {
	if e.selectedCols[col] {
		delete(e.selectedCols, col)
		e.unhighlightColumn(col)
	} else {
		e.selectedCols[col] = true
		e.highlightColumn(col)
	}
}

func (e *Editor) highlightRow(row int) {
	// HeaderTable handles selection highlighting internally
	// This is a placeholder for future row highlighting implementation
}

func (e *Editor) unhighlightRow(row int) {
	// HeaderTable handles selection highlighting internally
	// This is a placeholder for future row highlighting implementation
}

func (e *Editor) highlightColumn(col int) {
	// HeaderTable handles selection highlighting internally
	// This is a placeholder for future column highlighting implementation
}

func (e *Editor) unhighlightColumn(col int) {
	// HeaderTable handles selection highlighting internally
	// This is a placeholder for future column highlighting implementation
}

func (e *Editor) refreshData() {
	data, err := loadTable(e.db, e.dbType, e.tableSpec)
	if err != nil {
		return
	}

	e.data = data
	e.setupTable()
}

func (e *Editor) moveRow(row, direction int) {
	if row == 0 || row < 1 || row > len(e.data.Rows) {
		return
	}

	dataIdx := row - 1
	newIdx := dataIdx + direction

	if newIdx < 0 || newIdx >= len(e.data.Rows) {
		return
	}

	e.data.Rows[dataIdx], e.data.Rows[newIdx] = e.data.Rows[newIdx], e.data.Rows[dataIdx]
	e.setupTable()
	e.table.Select(row+direction, e.currentCol)
}

func (e *Editor) moveColumn(col, direction int) {
	if col < 0 || col >= len(e.data.Columns) {
		return
	}

	newIdx := col + direction
	if newIdx < 0 || newIdx >= len(e.data.Columns) {
		return
	}

	e.data.Columns[col], e.data.Columns[newIdx] = e.data.Columns[newIdx], e.data.Columns[col]

	for i := range e.data.Rows {
		e.data.Rows[i][col], e.data.Rows[i][newIdx] = e.data.Rows[i][newIdx], e.data.Rows[i][col]
	}

	e.setupTable()
	e.table.Select(e.currentRow, col+direction)
}

func (e *Editor) adjustColumnWidth(col, delta int) {
	if col < 0 || col >= len(e.data.Columns) {
		return
	}

	newWidth := max(3, e.data.Columns[col].Width+delta)
	e.data.Columns[col].Width = newWidth

	// Update the table column width and re-render
	e.table.SetColumnWidth(col, newWidth)
}

func formatCellValue(value interface{}) string {
	if value == nil {
		return "\\N"
	}

	switch v := value.(type) {
	case []byte:
		return string(v)
	case string:
		if v == "" {
			return ""
		}
		return v
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", value)
	}
}
