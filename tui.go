package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"unsafe"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const (
	DefaultColumnWidth = 8
)

type Editor struct {
	app                     *tview.Application
	pages                   *tview.Pages
	table                   *HeaderTable
	columns                 []Column
	sheet                   *Sheet
	config                  *Config
	currentRow              int
	currentCol              int
	editMode                bool
	selectedRows            map[int]bool
	selectedCols            map[int]bool
	statusBar               *tview.TextView
	commandPalette          *tview.InputField
	layout                  *tview.Flex
	paletteMode             PaletteMode
	preEditMode             PaletteMode
	originalEditText        string
	placeholderStyleDefault tcell.Style
	placeholderStyleItalic  tcell.Style
	kittySequenceActive     bool
	kittySequenceBuffer     string
}

// PaletteMode represents the current mode of the command palette
type PaletteMode int

const (
	PaletteModeDefault PaletteMode = iota
	PaletteModeCommand
	PaletteModeSQL
	PaletteModeFind
	PaletteModeUpdate
)

func (m PaletteMode) Glyph() string {
	switch m {
	case PaletteModeDefault:
		return "# "
	case PaletteModeCommand:
		return "> "
	case PaletteModeSQL:
		return "` "
	case PaletteModeFind:
		return "/ "
	case PaletteModeUpdate:
		return "= "
	default:
		return "> "
	}
}

func runEditor(config *Config, tablename string) error {
	tview.Styles.ContrastBackgroundColor = tcell.ColorBlack

	db, dbType, err := config.connect()
	if err != nil {
		return err
	}
	defer db.Close()

	// Get terminal height for optimal data loading
	terminalHeight := getTerminalHeight()

	var configColumns []Column // TODO derive from config
	sheet, err := NewSheet(db, dbType, tablename, configColumns, terminalHeight, config.Where, config.OrderBy, config.Limit)
	if err != nil {
		return err
	}

	// Register cleanup functions for signal handling
	cleanupFunc := func() {
		if sheet.filePath != "" {
			if err := os.Remove(sheet.filePath); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to cleanup temp file %s: %v\n", sheet.filePath, err)
			}
		}
		if sheet.StopChan != nil {
			select {
			case sheet.StopChan <- struct{}{}:
			default:
			}
		}
	}
	addCleanup(cleanupFunc)

	columns := make([]Column, len(sheet.attributes))
	for i, attr := range sheet.attributes {
		columns[i] = Column{
			Name:  attr.Name,
			Width: DefaultColumnWidth,
		}
	}

	editor := &Editor{
		app:          tview.NewApplication(),
		pages:        tview.NewPages(),
		table:        NewHeaderTable(),
		columns:      columns,
		sheet:        sheet,
		config:       config,
		selectedRows: make(map[int]bool),
		selectedCols: make(map[int]bool),
		paletteMode:  PaletteModeDefault,
		preEditMode:  PaletteModeDefault,
	}

	editor.setupTable()
	editor.setupKeyBindings()
	editor.setupStatusBar()
	editor.setupCommandPalette()
	editor.setupLayout()

	editor.pages.AddPage("table", editor.layout, true, true)

	if err := editor.app.SetRoot(editor.pages, true).EnableMouse(true).Run(); err != nil {
		return err
	}

	return nil
}

func (e *Editor) setupTable() {
	// Create headers for HeaderTable
	headers := make([]HeaderColumn, len(e.columns))
	for i, col := range e.columns {
		headers[i] = HeaderColumn{
			Name:  col.Name,
			Width: col.Width,
		}
	}

	// Configure the table
	e.table.SetHeaders(headers).SetData(e.sheet.records).SetSelectable(true)
}

func (e *Editor) setupStatusBar() {
	e.statusBar = tview.NewTextView().
		SetDynamicColors(true).
		SetRegions(true).
		SetWrap(false)

	e.statusBar.SetBackgroundColor(tcell.ColorLightGray)
	e.statusBar.SetTextColor(tcell.ColorBlack)
	e.statusBar.SetText("Ready")
}

func (e *Editor) setupCommandPalette() {
	inputField := tview.NewInputField()
	e.commandPalette = inputField.
		SetLabel("").
		SetFieldBackgroundColor(tcell.ColorBlack).
		SetFieldTextColor(tcell.ColorWhite)

	e.commandPalette.SetBackgroundColor(tcell.ColorBlack)
	e.placeholderStyleDefault = e.commandPalette.GetPlaceholderStyle()
	e.placeholderStyleItalic = e.placeholderStyleDefault.Italic(true)

	// Default palette mode shows keybinding help
	e.setPaletteMode(PaletteModeDefault, false)

	e.commandPalette.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		key := event.Key()
		rune := event.Rune()
		mod := event.Modifiers()

		if e.consumeKittyCSI(key, rune, mod) {
			return nil
		}
		if !e.kittySequenceActive {
			if key == tcell.KeyRune && mod&tcell.ModCtrl != 0 && rune == '`' {
				e.kittySequenceBuffer = "ctrl+`"
			} else {
				e.kittySequenceBuffer = ""
			}
		}
		if !e.kittySequenceActive && e.kittySequenceBuffer == "ctrl+`" {
			e.kittySequenceBuffer = ""
			e.setPaletteMode(PaletteModeSQL, true)
			return nil
		}

		switch {
		case (rune == 'f' || rune == 6) && mod&tcell.ModCtrl != 0:
			e.setPaletteMode(PaletteModeFind, true)
			return nil
		case (rune == 'p' || rune == 16) && mod&tcell.ModCtrl != 0:
			e.setPaletteMode(PaletteModeCommand, true)
			return nil
		case (rune == '`' || rune == 0) && mod&tcell.ModCtrl != 0:
			e.setPaletteMode(PaletteModeSQL, true)
			return nil
		}

		switch event.Key() {
		case tcell.KeyEnter:
			command := e.commandPalette.GetText()
			switch e.getPaletteMode() {
			case PaletteModeCommand:
				e.executeCommand(command)
			case PaletteModeSQL:
				// Placeholder for future SQL execution
				if strings.TrimSpace(command) != "" {
					e.SetStatusLog("SQL: " + strings.TrimSpace(command))
				}
			case PaletteModeFind:
				// Placeholder for future find implementation
				if strings.TrimSpace(command) != "" {
					e.SetStatusLog("Find: " + strings.TrimSpace(command))
				}
			}
			e.setPaletteMode(PaletteModeDefault, false)
			e.app.SetFocus(e.table)
			return nil
		case tcell.KeyEscape:
			e.setPaletteMode(PaletteModeDefault, false)
			e.app.SetFocus(e.table)
			return nil
		}
		return event
	})
}

func (e *Editor) setupLayout() {
	e.layout = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(e.table, 0, 1, true).
		AddItem(e.statusBar, 1, 0, false).
		AddItem(e.commandPalette, 1, 0, false)
}

func (e *Editor) setupKeyBindings() {
	e.table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		key := event.Key()
		rune := event.Rune()
		mod := event.Modifiers()
		row, col := e.table.GetSelection()

		if e.consumeKittyCSI(key, rune, mod) {
			return nil
		}
		if !e.kittySequenceActive {
			if key == tcell.KeyRune && mod&tcell.ModCtrl != 0 && rune == '`' {
				e.kittySequenceBuffer = "ctrl+`"
			} else {
				e.kittySequenceBuffer = ""
			}
		}

		if !e.kittySequenceActive && e.kittySequenceBuffer == "ctrl+`" {
			e.kittySequenceBuffer = ""
			e.setPaletteMode(PaletteModeSQL, true)
			return nil
		}

		switch {
		case key == tcell.KeyEnter:
			e.enterEditMode(row, col)
			return nil
		case key == tcell.KeyEscape:
			if e.app.GetFocus() == e.commandPalette {
				e.setPaletteMode(PaletteModeDefault, false)
				e.app.SetFocus(e.table)
				return nil
			}
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
			e.table.Select(row, len(e.columns)-1)
			return nil
		case key == tcell.KeyPgUp:
			newRow := max(0, row-10)
			e.table.Select(newRow, col)
			return nil
		case key == tcell.KeyPgDn:
			newRow := min(len(e.sheet.records)-1, row+10)
			e.table.Select(newRow, col)
			return nil
		case rune == ' ' && mod&tcell.ModShift != 0:
			e.toggleRowSelection(row)
			return nil
		case rune == ' ' && mod&tcell.ModCtrl != 0:
			e.toggleColSelection(col)
			return nil
		case (rune == 'r' || rune == 18) && mod&tcell.ModCtrl != 0: // Ctrl+R sends DC2 (18) or 'r' depending on terminal
			e.refreshData()
			return nil
		case (rune == 'f' || rune == 6) && mod&tcell.ModCtrl != 0: // Ctrl+F sends ACK (6) or 'f' depending on terminal
			e.setPaletteMode(PaletteModeFind, true)
			return nil
		case (rune == 'p' || rune == 16) && mod&tcell.ModCtrl != 0: // Ctrl+P sends DLE (16) or 'p' depending on terminal
			e.setPaletteMode(PaletteModeCommand, true)
			return nil
		case (rune == '`' || rune == 0) && mod&tcell.ModCtrl != 0: // Ctrl+` sends BEL (0) or '`' depending on terminal
			e.setPaletteMode(PaletteModeSQL, true)
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
			e.table.Select(row, len(e.columns)-1)
			return nil
		case key == tcell.KeyUp && mod&tcell.ModMeta != 0:
			e.table.Select(0, col)
			return nil
		case key == tcell.KeyDown && mod&tcell.ModMeta != 0:
			e.table.Select(len(e.sheet.records)-1, col)
			return nil
		default:
			if key == tcell.KeyRune && rune != 0 &&
				mod&(tcell.ModAlt|tcell.ModCtrl|tcell.ModMeta) == 0 {
				e.enterEditModeWithInitialValue(row, col, string(rune))
				return nil
			}
		}

		return event
	})
}

func (e *Editor) consumeKittyCSI(key tcell.Key, r rune, mod tcell.ModMask) bool {
	if e.kittySequenceActive {
		if key != tcell.KeyRune {
			e.kittySequenceActive = false
			e.kittySequenceBuffer = ""
			return false
		}

		if r == 'u' {
			seq := e.kittySequenceBuffer
			e.kittySequenceActive = false
			e.kittySequenceBuffer = ""
			parts := strings.SplitN(seq, ";", 2)
			if len(parts) == 2 {
				codepoint, err1 := strconv.Atoi(parts[0])
				modifier, err2 := strconv.Atoi(parts[1])
				if err1 == nil && err2 == nil {
					mask := modifier - 1
					if mask&4 != 0 && codepoint == 96 {
						e.setPaletteMode(PaletteModeSQL, true)
					}
				}
			}
			return true
		}

		e.kittySequenceBuffer += string(r)
		return true
	}

	if key == tcell.KeyRune && r == '[' {
		e.kittySequenceActive = true
		e.kittySequenceBuffer = ""
		return true
	}

	return false
}

func (e *Editor) navigateTab(reverse bool) {
	row, col := e.table.GetSelection()

	if reverse {
		if col > 0 {
			e.table.Select(row, col-1)
		} else if row > 0 {
			e.table.Select(row-1, len(e.columns)-1)
		}
	} else {
		if col < len(e.columns)-1 {
			e.table.Select(row, col+1)
		} else if row < len(e.sheet.records)-1 {
			e.table.Select(row+1, 0)
		}
	}
}

func (e *Editor) enterEditMode(row, col int) {
	currentValue := e.table.GetCell(row, col)
	currentText := formatCellValue(currentValue)
	e.enterEditModeWithInitialValue(row, col, currentText)
}

func (e *Editor) enterEditModeWithInitialValue(row, col int, initialText string) {
	if row < 0 || row >= len(e.sheet.records) {
		return
	}
	if col < 0 || col >= len(e.columns) || col >= len(e.sheet.records[row]) {
		return
	}

	// Remember the palette mode so we can restore it after editing
	e.preEditMode = e.getPaletteMode()
	originalValue := e.table.GetCell(row, col)
	e.originalEditText = formatCellValue(originalValue)

	// Create textarea for editing with proper styling
	textArea := tview.NewTextArea().
		SetText(initialText, true).
		SetWrap(true).
		SetOffset(0, 0)

	textArea.SetBorder(false)

	// Store references for dynamic resizing
	var modal tview.Primitive
	e.currentRow = row
	e.currentCol = col

	// Function to resize textarea based on current content
	resizeTextarea := func() {
		e.pages.RemovePage("editor")
		modal = e.createCellEditOverlay(textArea, row, col, textArea.GetText())
		e.pages.AddPage("editor", modal, true, true)
		textArea.SetOffset(0, 0)
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

	// Set up dynamic resizing on text changes and update SQL preview
	textArea.SetChangedFunc(func() {
		resizeTextarea()
		e.updateEditPreview(textArea.GetText())
	})

	// Position the textarea to align with the cell
	modal = e.createCellEditOverlay(textArea, row, col, initialText)
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

	e.setPaletteMode(PaletteModeUpdate, false)
}

func (e *Editor) createCellEditOverlay(textArea *tview.TextArea, row, col int, currentText string) tview.Primitive {
	// Get the current text to calculate minimum size
	currentText = formatCellValue(currentText)

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
	if e.editMode {
		e.pages.RemovePage("editor")
		e.app.SetAfterDrawFunc(nil) // Clear the cursor function
		e.setCursorStyle(0)         // Reset to default cursor style
		e.app.SetFocus(e.table)
		e.editMode = false
		// Return palette to default mode after editing
		e.setPaletteMode(PaletteModeDefault, false)
		e.originalEditText = ""
	}
}

// Mode management helpers
func (e *Editor) getPaletteMode() PaletteMode {
	return e.paletteMode
}

func (e *Editor) setPaletteMode(mode PaletteMode, focus bool) {
	e.paletteMode = mode
	e.commandPalette.SetLabel(mode.Glyph())
	// Clear input when switching modes
	e.commandPalette.SetText("")
	style := e.placeholderStyleItalic
	e.commandPalette.SetPlaceholderStyle(style)
	if e.editMode {
		// Editing contexts manage their own placeholder text
		e.commandPalette.SetPlaceholder("UPDATE preview... (Esc to exit)")
		if focus {
			e.app.SetFocus(e.commandPalette)
		}
		return
	} else {
		switch mode {
		case PaletteModeDefault:
			e.commandPalette.SetPlaceholder("Ctrl+P: Command   Ctrl+`: SQL   Ctrl+F: Find")
		case PaletteModeCommand:
			e.commandPalette.SetPlaceholder("Command… (Esc to exit)")
		case PaletteModeSQL:
			e.commandPalette.SetPlaceholder("SQL… (Esc to exit)")
		case PaletteModeFind:
			e.commandPalette.SetPlaceholder("Find… (Esc to exit)")
		case PaletteModeUpdate:
			// No placeholder in update mode
			e.commandPalette.SetPlaceholder("")
		}
	}
	if focus {
		e.app.SetFocus(e.commandPalette)
	}
}

// Update the command palette to show a SQL UPDATE preview while editing
func (e *Editor) updateEditPreview(newText string) {
	if e.sheet == nil || !e.editMode {
		return
	}
	colName := e.columns[e.currentCol].Name
	preview := e.sheet.BuildUpdatePreview(e.currentRow, colName, newText)
	e.commandPalette.SetPlaceholderStyle(e.placeholderStyleDefault)
	e.commandPalette.SetPlaceholder(preview)
}

func (e *Editor) updateCell(row, col int, newValue string) {
	// Basic bounds check against current data
	if row < 0 || row >= len(e.sheet.records) || col < 0 || col >= len(e.sheet.records[row]) {
		e.exitEditMode()
		return
	}

	// Delegate DB work to database.go
	updated, err := e.sheet.UpdateDBValue(row, e.columns[col].Name, newValue)
	if err != nil {
		e.exitEditMode()
		return
	}
	// Update in-memory data and refresh table
	copy(e.sheet.records[row], updated)
	e.table.SetData(e.sheet.records)
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
	panic("not implemented")
	// terminalHeight := getTerminalHeight()
	// data, err := NewSheet(e.db, e.sheet.DBType, e.sheet.table, e.columns, terminalHeight)
	// if err != nil {
	// 	return
	// }

	// e.sheet = data
	// e.setupTable()
}

func (e *Editor) moveRow(row, direction int) {
	if row == 0 || row < 1 || row > len(e.sheet.records) {
		return
	}

	dataIdx := row - 1
	newIdx := dataIdx + direction

	if newIdx < 0 || newIdx >= len(e.sheet.records) {
		return
	}

	e.sheet.records[dataIdx], e.sheet.records[newIdx] = e.sheet.records[newIdx], e.sheet.records[dataIdx]
	e.setupTable()
	e.table.Select(row+direction, e.currentCol)
}

func (e *Editor) moveColumn(col, direction int) {
	if col < 0 || col >= len(e.columns) {
		return
	}

	newIdx := col + direction
	if newIdx < 0 || newIdx >= len(e.columns) {
		return
	}

	e.columns[col], e.columns[newIdx] = e.columns[newIdx], e.columns[col]

	for i := range e.sheet.records {
		e.sheet.records[i][col], e.sheet.records[i][newIdx] = e.sheet.records[i][newIdx], e.sheet.records[i][col]
	}

	e.setupTable()
	e.table.Select(e.currentRow, col+direction)
}

func (e *Editor) adjustColumnWidth(col, delta int) {
	if col < 0 || col >= len(e.columns) {
		return
	}

	newWidth := max(3, e.columns[col].Width+delta)
	e.columns[col].Width = newWidth

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
	if e.sheet == nil || col < 0 || col >= len(e.columns) {
		return false
	}

	column := e.columns[col]
	var attrType string
	for _, attr := range e.sheet.attributes {
		if attr.Name == column.Name {
			attrType = attr.Type
			break
		}
	}

	// Normalize type for consistent matching
	attrType = strings.ToLower(attrType)

	// Check for text-compatible data types that support multiline content
	return strings.Contains(attrType, "char") ||
		strings.Contains(attrType, "varchar") ||
		strings.Contains(attrType, "text") ||
		attrType == "json" // json but not jsonb
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

// Status bar API methods
func (e *Editor) SetStatusMessage(message string) {
	if e.statusBar != nil {
		e.statusBar.SetText(message)
		e.app.Draw()
	}
}

func (e *Editor) SetStatusError(message string) {
	if e.statusBar != nil {
		e.statusBar.SetText("[red]ERROR: " + message + "[white]")
		e.app.Draw()
	}
}

func (e *Editor) SetStatusLog(message string) {
	if e.statusBar != nil {
		e.statusBar.SetText("[blue]LOG: " + message + "[white]")
		e.app.Draw()
	}
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
	case "refresh", "r":
		e.SetStatusMessage("Refreshing data...")
		e.refreshData()
		e.SetStatusMessage("Data refreshed")
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
