package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"syscall"
	"unsafe"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type Editor struct {
	app          *tview.Application
	pages        *tview.Pages
	table        *HeaderTable
	data         *Sheet
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

func runEditor(config *Config, tablename string, columns []string) error {
	db, dbType, err := config.connect()
	if err != nil {
		return err
	}
	defer db.Close()

	// Get terminal height for optimal data loading
	terminalHeight := getTerminalHeight()

	data, err := loadTable(db, dbType, tablename, columns, terminalHeight)
	if err != nil {
		return err
	}

	// Register cleanup functions for signal handling
	cleanupFunc := func() {
		if data.filePath != "" {
			if err := os.Remove(data.filePath); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to cleanup temp file %s: %v\n", data.filePath, err)
			}
		}
		if data.StopChan != nil {
			select {
			case data.StopChan <- struct{}{}:
			default:
			}
		}
	}

	addCleanup(cleanupFunc)

	// Also ensure cleanup on normal function exit
	defer cleanupFunc()

	editor := &Editor{
		app:          tview.NewApplication(),
		pages:        tview.NewPages(),
		table:        NewHeaderTable(),
		data:         data,
		db:           db,
		dbType:       dbType,
		config:       config,
		tableSpec:    tablename,
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
	e.table.SetHeaders(headers).SetData(e.data.records).SetSelectable(true)
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
			newRow := min(len(e.data.records)-1, row+10)
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
			e.table.Select(len(e.data.records)-1, col)
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
		} else if row < len(e.data.records)-1 {
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
		SetText(currentText, true).
		SetWrap(true).
		SetOffset(0, 0)

	textArea.SetBorder(false)

	// Wrap textarea in a box with dark blue background
	textAreaBox := tview.NewFlex().
		AddItem(textArea, 0, 1, true)

	// Store references for dynamic resizing
	var modal tview.Primitive

	// Function to resize textarea based on current content
	resizeTextarea := func() {
		currentText := textArea.GetText()

		e.pages.RemovePage("editor")
		modal = e.createCellEditOverlayWithText(textAreaBox, row, col, currentText)
		e.pages.AddPage("editor", modal, true, true)

		// Log the calculated dimensions from the overlay
		if flex, ok := modal.(*tview.Flex); ok {
			_, _, width, height := flex.GetRect()
			log.Printf("Modal dimensions - width: %d, height: %d", width, height)
		}
	}

	// Handle textarea input capture for save/cancel
	textArea.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
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
					// return event
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
		case tcell.KeyEscape:
			e.exitEditMode()
			return nil
		}
		return event
	})

	// Set up dynamic resizing on text changes
	textArea.SetChangedFunc(func() {
		resizeTextarea()
		textArea.SetOffset(0, 0)
	})

	// Position the textarea to align with the cell
	modal = e.createCellEditOverlay(textAreaBox, row, col)
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
		offsetRow, offsetColumn := textArea.GetOffset()

		// Calculate screen position based on original cell position
		// Use the same calculation as createCellEditOverlay
		tableRow := row + 3 // Convert data row to table display row
		leftOffset := 1     // Left table border "│"
		for i := 0; i < col; i++ {
			leftOffset += e.table.GetColumnWidth(i) + 2 + 1 // width + " │ " padding + separator
		}
		leftOffset += 1 // Cell padding (space after "│ ")

		// Calculate cursor position relative to the cell content area
		cursorX := leftOffset + toCol - offsetColumn
		cursorY := tableRow + toRow - offsetRow

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

	return e.createCellEditOverlayInternal(textArea, row, col, currentText)
}

func (e *Editor) createCellEditOverlayWithText(textArea tview.Primitive, row, col int, currentText string) tview.Primitive {
	return e.createCellEditOverlayInternal(textArea, row, col, currentText)
}

func (e *Editor) createCellEditOverlayInternal(textArea tview.Primitive, row, col int, currentText string) tview.Primitive {
	// Calculate the position where the cell content appears on screen
	// HeaderTable structure: top border (row 0) + header (row 1) + separator (row 2) + data rows (3+)
	tableRow := row + 3 // Convert data row to table display row

	// Calculate horizontal position: left border + previous columns + cell padding
	leftOffset := 1 // Left table border "│"
	for i := 0; i < col; i++ {
		leftOffset += e.table.GetColumnWidth(i) + 2 + 1 // width + " │ " padding + separator
	}
	leftOffset += 1 // Cell padding (space after "│ ")
	leftOffset -= 1 // Move overlay one position to the left

	// Calculate vertical position relative to table
	topOffset := tableRow

	// Calculate minimum textarea size based on content
	cellWidth := e.table.GetColumnWidth(col)

	// Calculate total table width for maximum textarea width
	totalTableWidth := 0
	for i := 0; i < len(e.data.Columns); i++ {
		totalTableWidth += e.table.GetColumnWidth(i)
	}
	// Add space for borders and separators: left border + (n-1 separators * 3) + right border
	totalTableWidth += 1 + (len(e.data.Columns)-1)*3 + 1

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
	if textAreaWidth < desiredWidth {
		textLines = splitTextToLines(currentText, textAreaWidth-2) // Account for padding
	}

	// Minimum height: number of lines needed, minimum 1
	textAreaHeight := max(len(textLines), 1)

	// Create positioned overlay that aligns text with the original cell
	leftPadding := tview.NewBox()
	rightPadding := tview.NewBox()

	return tview.NewFlex().
		AddItem(nil, leftOffset, 0, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, topOffset, 0, false).
			AddItem(tview.NewFlex().
				AddItem(leftPadding, 1, 0, false).           // Left padding (red)
				AddItem(textArea, textAreaWidth-2, 0, true). // Text area
				AddItem(rightPadding, 1, 0, false),          // Right padding (green)
				textAreaHeight, 0, true).
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
	if row >= 0 && row < len(e.data.records) && col >= 0 && col < len(e.data.records[row]) {
		e.data.records[row][col] = newValue
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
	terminalHeight := getTerminalHeight()
	columns := []string{}
	for _, col := range e.data.Columns {
		columns = append(columns, col.Name)
	}
	data, err := loadTable(e.db, e.dbType, e.tableSpec, columns, terminalHeight)
	if err != nil {
		return
	}

	e.data = data
	e.setupTable()
}

func (e *Editor) moveRow(row, direction int) {
	if row == 0 || row < 1 || row > len(e.data.records) {
		return
	}

	dataIdx := row - 1
	newIdx := dataIdx + direction

	if newIdx < 0 || newIdx >= len(e.data.records) {
		return
	}

	e.data.records[dataIdx], e.data.records[newIdx] = e.data.records[newIdx], e.data.records[dataIdx]
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

	for i := range e.data.records {
		e.data.records[i][col], e.data.records[i][newIdx] = e.data.records[i][newIdx], e.data.records[i][col]
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

func (e *Editor) isMultilineColumnType(col int) bool {
	if e.data == nil || col < 0 || col >= len(e.data.Columns) {
		return false
	}

	columnType := strings.ToLower(e.data.Columns[col].Type)

	// Check for text-compatible data types that support multiline content
	return strings.Contains(columnType, "char") ||
		strings.Contains(columnType, "varchar") ||
		strings.Contains(columnType, "text") ||
		columnType == "json" // json but not jsonb
}

// getTerminalHeight returns the height of the terminal in rows
func getTerminalHeight() int {
	type winsize struct {
		Row    uint16
		Col    uint16
		Xpixel uint16
		Ypixel uint16
	}

	ws := &winsize{}
	retCode, _, errno := syscall.Syscall(syscall.SYS_IOCTL,
		uintptr(syscall.Stdin),
		uintptr(syscall.TIOCGWINSZ),
		uintptr(unsafe.Pointer(ws)))

	if int(retCode) == -1 {
		// If we can't get terminal size, return a reasonable default
		_ = errno // avoid unused variable error
		return 24 // Standard terminal height
	}
	return int(ws.Row)
}
