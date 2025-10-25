package main

import (
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const (
	DefaultColumnWidth   = 8
	RowsTimerInterval    = 300 * time.Millisecond
	RefreshTimerInterval = RowsTimerInterval
)

// this is a config concept
// width = 0 means Column is hidden but selected
type Column struct {
	Name  string
	Width int
}

// columns are display, relation.attributes stores the database attributes
// lookup key is unique: it's always selected
// if a multicolumn reference is selected, all columns in the reference are selected
type Editor struct {
	app      *tview.Application
	pages    *tview.Pages
	table    *TableView
	columns  []Column
	relation *Relation
	config   *Config

	statusBar               *tview.TextView
	commandPalette          *tview.InputField
	layout                  *tview.Flex
	paletteMode             PaletteMode
	preEditMode             PaletteMode
	placeholderStyleDefault tcell.Style
	placeholderStyleItalic  tcell.Style
	kittySequenceActive     bool
	kittySequenceBuffer     string

	// selection
	currentRow   int
	currentCol   int
	editMode     bool
	selectedRows map[int]bool
	selectedCols map[int]bool

	// data, records is a circular buffer
	nextQuery *sql.Rows // nextRows
	prevQuery *sql.Rows // prevRows
	pointer   int       // pointer to the current record
	records   [][]any   // columns are keyed off e.columns

	// timer for auto-closing rows
	rowsTimer      *time.Timer
	rowsTimerReset chan struct{}

	// timer for auto-refreshing data
	refreshTimer     *time.Timer
	refreshTimerStop chan struct{}
}

// PaletteMode represents the current mode of the command palette
type PaletteMode int

const (
	PaletteModeDefault PaletteMode = iota
	PaletteModeCommand
	PaletteModeSQL
	PaletteModeGoto
	PaletteModeUpdate
	PaletteModeInsert
)

func (m PaletteMode) Glyph() string {
	switch m {
	case PaletteModeDefault:
		return "⌃ "
	case PaletteModeCommand:
		return "> "
	case PaletteModeSQL:
		return "` "
	case PaletteModeGoto:
		return "↪ "
	case PaletteModeUpdate:
		return "` "
	case PaletteModeInsert:
		return "` "
	default:
		return "> "
	}
}

func runEditor(config *Config, dbname, tablename string) error {
	tview.Styles.ContrastBackgroundColor = tcell.ColorBlack

	db, dbType, err := config.connect()
	if err != nil {
		return err
	}
	defer db.Close()

	// Get terminal height for optimal data loading
	terminalHeight := getTerminalHeight()
	tableDataHeight := terminalHeight - 5 // 5 lines for header, status bar, command palette

	relation, err := NewRelation(db, dbType, tablename)
	if err != nil {
		return err
	}

	// force key to be first column(s)
	columns := make([]Column, 0, len(relation.attributeOrder))
	for _, name := range relation.key {
		columns = append(columns, Column{Name: name, Width: 4})
	}
	for _, name := range relation.attributeOrder {
		if !slices.Contains(relation.key, name) {
			columns = append(columns, Column{Name: name, Width: DefaultColumnWidth})
		}
	}

	editor := &Editor{
		app: tview.NewApplication().SetTitle(fmt.Sprintf("ted %s/%s %s",
			dbname, tablename, databaseIcons[dbType])).EnableMouse(true),
		pages:        tview.NewPages(),
		table:        NewTableView(),
		columns:      columns,
		relation:     relation,
		config:       config,
		selectedRows: make(map[int]bool),
		selectedCols: make(map[int]bool),
		paletteMode:  PaletteModeDefault,
		preEditMode:  PaletteModeDefault,
		pointer:      0,
		records:      make([][]any, tableDataHeight),
	}

	editor.setupTable()
	editor.setupKeyBindings()
	editor.setupStatusBar()
	editor.setupCommandPalette()
	editor.setupLayout()
	editor.refreshData()
	editor.table.Select(0, 0)
	editor.pages.AddPage("table", editor.layout, true, true)

	if err := editor.app.SetRoot(editor.pages, true).Run(); err != nil {
		return err
	}
	return nil
}

func (e *Editor) setupTable() {
	// Create headers for TableView
	headers := make([]HeaderColumn, len(e.columns))
	for i, col := range e.columns {
		headers[i] = HeaderColumn{
			Name:  col.Name,
			Width: col.Width,
		}
	}

	// Configure the table
	e.table.SetHeaders(headers).
		SetData(e.records).
		SetSelectable(true).
		SetDoubleClickFunc(func(row, col int) {
			// Double-click on a cell opens edit mode
			e.enterEditMode(row, col)
		}).
		SetSingleClickFunc(func(row, col int) {
			// Single-click exits edit mode without saving
			if e.editMode {
				e.exitEditMode()
			}
		})

	// Set up mouse scroll handling
	e.table.SetMouseCapture(func(action tview.MouseAction,
		event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
		switch action {
		case tview.MouseScrollUp:
			reachedEnd, err := e.prevRows(1)
			if err != nil {
				return action, nil
			}
			if !reachedEnd {
				e.table.Select(e.table.selectedRow-1, e.table.selectedCol)
			}
			go e.app.Draw()
			return action, nil
		case tview.MouseScrollDown:
			reachedEnd, err := e.nextRows(1)
			if err != nil {
				return action, nil
			}
			if !reachedEnd {
				e.table.Select(e.table.selectedRow-1, e.table.selectedCol)
			}
			go e.app.Draw()
			return action, nil
		}
		// Pass through other mouse events
		return action, event
	})
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
		// Ctrl+G sends BEL (7) or 'g' depending on terminal
		case (rune == 'g' || rune == 7) && mod&tcell.ModCtrl != 0:
			e.setPaletteMode(PaletteModeGoto, true)
			return nil
		// case (rune == 'p' || rune == 16) && mod&tcell.ModCtrl != 0:
		// 	e.setPaletteMode(PaletteModeCommand, true)
		// 	return nil
		case (rune == '`' || rune == 0) && mod&tcell.ModCtrl != 0:
			e.setPaletteMode(PaletteModeSQL, true)
			return nil
		}

		switch event.Key() {
		case tcell.KeyEnter:
			command := e.commandPalette.GetText()
			mode := e.getPaletteMode()
			switch mode {
			case PaletteModeCommand:
				e.executeCommand(command)
			case PaletteModeSQL:
				if strings.TrimSpace(command) != "" {
					e.executeSQL(command)
					e.refreshData()
				}
			case PaletteModeGoto:
				e.executeGoto(command)
			}

			// For Goto mode, keep the palette open with text selected
			if mode == PaletteModeGoto {
				return nil
			}

			e.setPaletteMode(PaletteModeDefault, false)
			e.app.SetFocus(e.table)
			return nil
		case tcell.KeyEscape:
			if e.paletteMode == PaletteModeInsert && e.editMode {
				return event
			}
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

		// Ctrl+S: execute INSERT in insert mode (save and insert)
		if (rune == 's' || rune == 19) && mod&tcell.ModAlt != 0 {
			if len(e.table.insertRow) > 0 && !e.editMode {
				e.executeInsert()
				return nil
			}
		}

		// Alt+0: set cell to null for nullable columns in insert mode
		if rune == '0' && mod&tcell.ModAlt != 0 {
			if len(e.table.insertRow) > 0 && !e.editMode {
				// Check if the selected column is nullable
				colName := e.columns[col].Name
				if attr, ok := e.relation.attributes[colName]; ok && attr.Nullable {
					e.table.insertRow[col] = nil
					e.renderData()
				}
				return nil
			}
		}

		switch {
		// Alt+Enter: execute INSERT in insert mode
		case key == tcell.KeyEnter:
			if len(e.table.insertRow) > 0 && !e.editMode && mod&tcell.ModAlt != 0 {
				e.executeInsert()
				return nil
			}
			// Enter: enter edit mode
			e.enterEditMode(row, col)
			return nil
		case key == tcell.KeyEscape:
			if e.app.GetFocus() == e.commandPalette {
				e.setPaletteMode(PaletteModeDefault, false)
				e.app.SetFocus(e.table)
				return nil
			}

			if len(e.table.insertRow) > 0 {
				// Exit insert mode and select last real row
				e.table.ClearInsertRow()

				// Find the last non-nil record
				lastIdx := len(e.records) - 1
				for lastIdx >= 0 && e.records[lastIdx] == nil {
					lastIdx--
				}

				e.renderData()
				_, col := e.table.GetSelection()
				e.table.Select(lastIdx, col)
				e.SetStatusMessage("")
				return nil
			}
			e.exitEditMode()
			return nil
		case key == tcell.KeyTab:
			// Ctrl+I sends Tab, so check for Ctrl modifier
			if mod&tcell.ModCtrl != 0 {
				// Ctrl+I: Jump to end and enable insert mode
				e.loadFromRowId(nil, false, 0)

				// Scroll forward by 1 row to make room for the insert mode row
				e.nextRows(1)

				e.table.SetupInsertRow()
				e.updateStatusForInsertMode()
				e.renderData()

				// Select the insert mode row (which is at index len(data))
				_, col := e.table.GetSelection()
				e.table.Select(e.table.GetDataLength(), col)

				return nil
			}
			e.navigateTab(false)
			return nil
		case key == tcell.KeyBacktab:
			e.navigateTab(true)
			return nil
		case key == tcell.KeyHome:
			if mod&tcell.ModCtrl != 0 {
				if len(e.table.insertRow) > 0 {
					return nil // Disable vertical navigation in insert mode
				}
				// Ctrl+Home: jump to first row
				e.loadFromRowId(nil, true, col)
				e.table.Select(0, col)
				return nil
			}
			e.table.Select(row, 0)
			return nil
		case key == tcell.KeyEnd:
			if mod&tcell.ModCtrl != 0 {
				if len(e.table.insertRow) > 0 {
					return nil // Disable vertical navigation in insert mode
				}
				// Ctrl+End: jump to last row
				e.loadFromRowId(nil, false, col)
				e.table.Select(len(e.records)-2, col)
				return nil
			}
			e.table.Select(row, len(e.columns)-1)
			return nil
		case key == tcell.KeyPgUp:
			if len(e.table.insertRow) > 0 {
				return nil // Disable vertical navigation in insert mode
			}
			// Page up: scroll data backward while keeping selection in same visual position
			pageSize := max(1, e.table.BodyRowsAvailable-1)
			e.prevRows(pageSize)
			return nil
		case key == tcell.KeyPgDn:
			if len(e.table.insertRow) > 0 {
				return nil // Disable vertical navigation in insert mode
			}
			// Page down: scroll data forward while keeping selection in same visual position
			pageSize := max(1, e.table.BodyRowsAvailable-1)
			// Keep selection at same position, just fetch next rows
			e.nextRows(pageSize)
			return nil
		case rune == ' ' && mod&tcell.ModShift != 0:
			e.toggleRowSelection(row)
			return nil
		case rune == ' ' && mod&tcell.ModCtrl != 0:
			e.toggleColSelection(col)
			return nil
		// Ctrl+R sends DC2 (18) or 'r' depending on terminal
		case (rune == 'r' || rune == 18) && mod&tcell.ModCtrl != 0:
			e.refreshData()
			return nil
		// Ctrl+G sends BEL (7) or 'g' depending on terminal
		case (rune == 'g' || rune == 7) && mod&tcell.ModCtrl != 0:
			e.setPaletteMode(PaletteModeGoto, true)
			return nil
		// Ctrl+P sends DLE (16) or 'p' depending on terminal
		case (rune == 'p' || rune == 16) && mod&tcell.ModCtrl != 0:
			e.setPaletteMode(PaletteModeCommand, true)
			return nil
		// Ctrl+` sends BEL (0) or '`' depending on terminal
		case (rune == '`' || rune == 0) && mod&tcell.ModCtrl != 0:
			e.setPaletteMode(PaletteModeSQL, true)
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
		case key == tcell.KeyUp:
			if len(e.table.insertRow) > 0 {
				return nil // Disable vertical navigation in insert mode
			}
			if mod&tcell.ModMeta != 0 {
				e.table.Select(0, col)
				return nil
			} else {
				if row == 0 {
					e.prevRows(1)
				} else {
					e.table.Select(row-1, col)
				}
				return nil
			}
		case key == tcell.KeyDown:
			if len(e.table.insertRow) > 0 {
				return nil // Disable vertical navigation in insert mode
			}
			if mod&tcell.ModMeta != 0 {
				if len(e.records[len(e.records)-1]) == 0 {
					e.table.Select(len(e.records)-2, col)
				} else {
					e.table.Select(len(e.records)-1, col)
				}
				return nil
			} else {
				if row == len(e.records)-1 {
					e.nextRows(1)
				} else {
					if len(e.records[row+1]) == 0 {
						e.table.Select(row+2, col)
					} else {
						e.table.Select(row+1, col)
					}
				}
				return nil
			}
		case key == tcell.KeyBackspace || key == tcell.KeyBackspace2 || key == tcell.KeyDEL || key == tcell.KeyDelete:
			// Backspace or Delete: start editing with empty string
			e.enterEditModeWithInitialValue(row, col, "")
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
					} else if mask&4 != 0 && codepoint == 105 {
						// Ctrl+I: Jump to end and enable insert mode
						e.loadFromRowId(nil, false, 0)

						e.table.SetupInsertRow()
						e.updateStatusForInsertMode()
						e.renderData()
						// Select the insert mode row (which is at index len(data))
						_, col := e.table.GetSelection()
						e.table.Select(e.table.GetDataLength(), col)
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
		} else if row < len(e.records)-1 {
			e.table.Select(row+1, 0)
		}
	}
}

func (e *Editor) enterEditMode(row, col int) {
	var currentValue any
	if len(e.table.insertRow) > 0 {
		currentValue = e.table.insertRow[col]
		currentText := ""
		if currentValue != nil {
			currentText, _ = formatCellValue(currentValue, tcell.StyleDefault)
		}
		e.enterEditModeWithInitialValue(row, col, currentText)
	} else {
		currentValue = e.table.GetCell(row, col)
		currentText, _ := formatCellValue(currentValue, tcell.StyleDefault)
		e.enterEditModeWithInitialValue(row, col, currentText)
	}
}

func (e *Editor) enterEditModeWithInitialValue(row, col int, initialText string) {
	// In insert mode, allow editing the virtual insert mode row
	// The virtual row index equals the length of the data array
	// (which is shorter than records in insert mode)
	isNewRecordRow := len(e.table.insertRow) > 0 && row == e.table.GetDataLength()

	// TODO no need to remember palette mode, it should just return to default mode after editing
	// Remember the palette mode so we can restore it after editing
	e.preEditMode = e.getPaletteMode()

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
	// TableView structure: top border (row 0) + header (row 1) + separator (row 2) + data rows (3+)
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
		if e.table.insertRow == nil {
			e.setPaletteMode(PaletteModeDefault, false)
			e.SetStatusMessage("Ready")
		} else {
			preview := e.relation.BuildInsertPreview(e.table.insertRow, e.columns)
			e.commandPalette.SetPlaceholder(preview)
			e.updateStatusForInsertMode()
		}
	}
}

func (e *Editor) updateStatusForInsertMode() {
	e.SetStatusMessage("Alt+S to insert · Esc to cancel")
}

// updateStatusForEditMode sets helpful status bar text based on column type and constraints
func (e *Editor) updateStatusForEditMode(col int) {
	if e.relation == nil || col < 0 || col >= len(e.columns) {
		e.SetStatusMessage("Editing...")
		return
	}

	colName := e.columns[col].Name
	attr, ok := e.relation.attributes[colName]
	if !ok {
		e.SetStatusMessage("Editing...")
		return
	}

	var parts []string

	// Enum or custom type information takes priority
	if len(attr.EnumValues) > 0 {
		// Format enum values
		enumStr := formatEnumValues(attr.EnumValues)
		if attr.CustomTypeName != "" {
			parts = append(parts, fmt.Sprintf("ENUM %s: %s", attr.CustomTypeName, enumStr))
		} else {
			parts = append(parts, fmt.Sprintf("ENUM: %s", enumStr))
		}
	} else if attr.CustomTypeName != "" {
		// Custom type without enum values
		parts = append(parts, fmt.Sprintf("Custom type: %s", attr.CustomTypeName))
	} else {
		// Standard column type information
		typeHint := getTypeHint(attr.Type)
		if typeHint != "" {
			parts = append(parts, typeHint)
		}
	}

	// Nullability constraint
	if !attr.Nullable {
		parts = append(parts, "NOT NULL")
	} else {
		// Show Alt+0 hint for nullable columns
		parts = append(parts, "Alt+0 for null")
	}

	// Foreign key reference
	if attr.Reference >= 0 && attr.Reference < len(e.relation.references) {
		refTable := e.relation.references[attr.Reference].ForeignTable.name
		parts = append(parts, fmt.Sprintf("→ %s", refTable))
	}

	// Multiline hint
	if e.isMultilineColumnType(col) {
		parts = append(parts, "Alt+Enter for newline")
	}

	// Enter to save hint
	parts = append(parts, "Enter to save · Esc to cancel")

	e.SetStatusMessage(strings.Join(parts, " · "))
}

// updateForeignKeyPreview updates the status bar with a preview of the referenced row
func (e *Editor) updateForeignKeyPreview(col int, newText string) {
	if e.relation == nil || col < 0 || col >= len(e.columns) {
		return
	}

	colName := e.columns[col].Name
	attr, ok := e.relation.attributes[colName]
	if !ok || attr.Reference < 0 || attr.Reference >= len(e.relation.references) {
		return // Not a foreign key column
	}

	// Look up the referenced row
	// refPreview := e.relation.LookupReferencedRow(colName, newText)

	// Rebuild status message with foreign key preview
	var parts []string

	// Enum or custom type information takes priority
	if len(attr.EnumValues) > 0 {
		// Format enum values, highlight if current value matches
		enumStr := formatEnumValuesWithHighlight(attr.EnumValues, newText)
		if attr.CustomTypeName != "" {
			parts = append(parts, fmt.Sprintf("ENUM %s: %s", attr.CustomTypeName, enumStr))
		} else {
			parts = append(parts, fmt.Sprintf("ENUM: %s", enumStr))
		}
	} else if attr.CustomTypeName != "" {
		// Custom type without enum values
		parts = append(parts, fmt.Sprintf("Custom type: %s", attr.CustomTypeName))
	} else {
		// Standard column type information
		typeHint := getTypeHint(attr.Type)
		if typeHint != "" {
			parts = append(parts, typeHint)
		}
	}

	// Nullability constraint
	if !attr.Nullable {
		parts = append(parts, "NOT NULL")
	}

	ref := e.relation.references[attr.Reference]
	if newText == "" || newText == NullGlyph {
		parts = append(parts, fmt.Sprintf("→ %s", ref.ForeignTable.name))
	} else {
		// Foreign key reference with preview
		foreignKeys := make(map[string]any)
		for attrIdx, foreignCol := range ref.ForeignColumns {
			if attrIdx == col {
				foreignKeys[foreignCol] = newText
			} else {
				foreignKeys[foreignCol] = e.records[e.currentRow][attrIdx]
			}
		}
		// TODO pass config columns if available
		preview, err := getForeignRow(e.relation.DB, ref.ForeignTable, foreignKeys, nil)
		if err != nil {
			e.SetStatusError(err.Error())
			return
		}
		previewStr := ""
		previewStrs := make([]string, 0, len(preview))
		for col, val := range preview {
			previewStrs = append(previewStrs, fmt.Sprintf("%s: %v", col, val))
		}
		previewStr = strings.Join(previewStrs, ", ")
		if preview == nil {
			parts = append(parts, fmt.Sprintf("[blueviolet]→ %s: not found[black]",
				ref.ForeignTable.name))
		} else {
			parts = append(parts, fmt.Sprintf("[darkgreen]→ %s: %s[black]",
				ref.ForeignTable.name, previewStr))
		}
	}

	// Multiline hint
	if e.isMultilineColumnType(col) {
		parts = append(parts, "Alt+Enter for newline")
	}

	// Enter to save hint
	parts = append(parts, "Enter to save · Esc to cancel")
	e.SetStatusMessage(strings.Join(parts, " · "))
}

// formatEnumValues formats enum values for display in the status bar
// Shows first few values, with "..." if there are too many
func formatEnumValues(values []string) string {
	if len(values) == 0 {
		return ""
	}

	maxDisplay := 5
	maxLength := 60 // Max total length

	var parts []string
	totalLen := 0

	for i, val := range values {
		if i >= maxDisplay {
			parts = append(parts, "...")
			break
		}

		// Truncate individual values if too long
		displayVal := val
		if len(displayVal) > 20 {
			displayVal = displayVal[:17] + "..."
		}

		quoted := "'" + displayVal + "'"
		if totalLen+len(quoted)+2 > maxLength && i > 0 {
			parts = append(parts, "...")
			break
		}

		parts = append(parts, quoted)
		totalLen += len(quoted) + 2 // +2 for ", "
	}

	return strings.Join(parts, ", ")
}

// formatEnumValuesWithHighlight formats enum values and highlights the matching one
func formatEnumValuesWithHighlight(values []string, currentValue string) string {
	if len(values) == 0 {
		return ""
	}

	maxDisplay := 5
	maxLength := 60

	var parts []string
	totalLen := 0
	foundMatch := false

	for i, val := range values {
		if i >= maxDisplay {
			parts = append(parts, "...")
			break
		}

		// Truncate individual values if too long
		displayVal := val
		if len(displayVal) > 20 {
			displayVal = displayVal[:17] + "..."
		}

		// Highlight if it matches current value
		var formatted string
		if val == currentValue {
			formatted = "[green]'" + displayVal + "'[white]"
			foundMatch = true
		} else {
			formatted = "'" + displayVal + "'"
		}

		// Estimate length without color codes for truncation
		plainLen := len("'" + displayVal + "'")
		if totalLen+plainLen+2 > maxLength && i > 0 {
			parts = append(parts, "...")
			break
		}

		parts = append(parts, formatted)
		totalLen += plainLen + 2
	}

	// If value doesn't match any enum, show warning
	if !foundMatch && currentValue != "" && currentValue != NullGlyph {
		// Check if it's a partial match
		hasPartial := false
		for _, val := range values {
			if strings.HasPrefix(val, currentValue) {
				hasPartial = true
				break
			}
		}
		if hasPartial {
			return strings.Join(parts, ", ") + " [yellow](typing...)[white]"
		}
		return strings.Join(parts, ", ") + " [yellow](invalid)[white]"
	}

	return strings.Join(parts, ", ")
}

// getTypeHint returns a user-friendly hint for a database column type
func getTypeHint(dbType string) string {
	t := strings.ToLower(dbType)

	switch {
	case strings.Contains(t, "bool"):
		return "Boolean (true/false, 1/0, t/f)"
	case strings.Contains(t, "tinyint"):
		return "Integer (-128 to 127)"
	case strings.Contains(t, "smallint"):
		return "Integer (-32768 to 32767)"
	case t == "int" || strings.Contains(t, "integer"):
		return "Integer"
	case strings.Contains(t, "bigint"):
		return "Large integer"
	case strings.Contains(t, "real") || strings.Contains(t, "float"):
		return "Decimal number"
	case strings.Contains(t, "double"):
		return "Decimal number (high precision)"
	case strings.Contains(t, "decimal") || strings.Contains(t, "numeric"):
		return "Exact decimal number"
	case strings.Contains(t, "char") || strings.Contains(t, "varchar"):
		return "Text"
	case strings.Contains(t, "text") || strings.Contains(t, "clob"):
		return "Text (unlimited)"
	case strings.Contains(t, "date"):
		return "Date (YYYY-MM-DD)"
	case strings.Contains(t, "time") && !strings.Contains(t, "stamp"):
		return "Time (HH:MM:SS)"
	case strings.Contains(t, "timestamp"):
		return "Timestamp (ISO 8601)"
	case strings.Contains(t, "json"):
		return "JSON"
	case strings.Contains(t, "uuid"):
		return "UUID"
	case strings.Contains(t, "blob") || strings.Contains(t, "bytea") || strings.Contains(t, "binary"):
		return "Binary data"
	default:
		return ""
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
	if e.table.insertRow != nil {
		e.commandPalette.SetPlaceholder("INSERT preview… (Esc to exit)")
	}
	if e.editMode {
		// Editing contexts manage their own placeholder text
		switch e.paletteMode {
		case PaletteModeUpdate:
			e.commandPalette.SetPlaceholder("UPDATE preview… (Esc to exit)")
		case PaletteModeInsert:
			e.commandPalette.SetPlaceholder("INSERT preview… (Esc to exit)")
		}
		if focus {
			e.app.SetFocus(e.commandPalette)
		}
		return
	} else {
		switch mode {
		case PaletteModeDefault:
			e.commandPalette.SetPlaceholder("Ctrl+… I: Insert · `: SQL · G: Goto · C: Exit")
		case PaletteModeCommand:
			e.commandPalette.SetPlaceholder("Command… (Esc to exit)")
		case PaletteModeSQL:
			e.commandPalette.SetPlaceholder("Execute SQL… (Esc to exit)")
		case PaletteModeGoto:
			e.commandPalette.SetPlaceholder("Goto next row matching value… (Esc to exit)")
			e.SetStatusMessage("hi")
		case PaletteModeUpdate:
			// No placeholder in update mode
			e.commandPalette.SetPlaceholder("")
		case PaletteModeInsert:
			// No placeholder in insert mode
			e.commandPalette.SetPlaceholder("")
		}
	}
	if focus {
		e.app.SetFocus(e.commandPalette)
	}
}

// Update the command palette to show a SQL UPDATE/INSERT preview while editing
func (e *Editor) updateEditPreview(newText string) {
	if e.relation == nil || !e.editMode {
		return
	}

	// Check if we're in insert mode
	isNewRecordRow := len(e.table.insertRow) > 0 && e.currentRow == e.table.GetDataLength()

	var preview string
	colName := e.columns[e.currentCol].Name
	if isNewRecordRow {
		// Show INSERT preview and set palette mode to Insert
		e.setPaletteMode(PaletteModeInsert, false)
		newRecordRow := make([]any, len(e.table.insertRow))
		copy(newRecordRow, e.table.insertRow)
		newRecordRow[e.currentCol] = newText
		preview = e.relation.BuildInsertPreview(newRecordRow, e.columns)
	} else {
		// Show UPDATE preview and set palette mode to Update
		e.setPaletteMode(PaletteModeUpdate, false)
		preview = e.relation.BuildUpdatePreview(e.records, e.currentRow, colName, newText)
	}
	e.commandPalette.SetPlaceholderStyle(e.placeholderStyleDefault)
	e.commandPalette.SetPlaceholder(preview)

	// Update status bar with foreign key preview if this column has a reference (only for UPDATE)
	if !isNewRecordRow {
		e.updateForeignKeyPreview(e.currentCol, newText)
	}
}

// e.table.SetData based on e.records and e.pointer
func (e *Editor) renderData() {
	// When in insert mode, we need to reserve one slot for the insert mode row
	// that will be rendered by TableView
	dataCount := len(e.records)
	if len(e.table.insertRow) > 0 {
		// Find the last non-nil record
		lastIdx := len(e.records) - 1
		for lastIdx >= 0 && e.records[lastIdx] == nil {
			lastIdx--
		}
		dataCount = lastIdx + 1 // Only pass real data, TableView will add insert mode row
	}

	normalizedRecords := make([][]any, dataCount)
	for i := 0; i < dataCount; i++ {
		ptr := (i + e.pointer) % len(e.records)
		// insert mode row needs "space" at the top to still be able to render
		// the last db row
		if len(e.table.insertRow) > 0 && i == 0 {
			ptr++
		}
		normalizedRecords[i] = e.records[ptr]
	}
	if len(e.table.insertRow) > 0 {
		normalizedRecords = slices.Delete(normalizedRecords, 0, 1)
	}
	e.table.SetData(normalizedRecords)
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
	if row < 0 || row >= len(e.records) || col < 0 || col >= len(e.records[row]) {
		e.exitEditMode()
		return
	}

	// Delegate DB work to database.go
	updated, err := e.relation.UpdateDBValue(e.records, row, e.columns[col].Name, newValue)
	if err != nil {
		e.exitEditMode()
		return
	}
	// Update in-memory data and refresh table
	ptr := (row + e.pointer) % len(e.records)
	e.records[ptr] = updated

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
		e.SetStatusError(err.Error())
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
			e.loadFromRowId(nil, false, 0)
			e.renderData()
			e.table.Select(len(e.records)-2, 0)
			return nil
		}
		keyVals[i] = insertedRow[keyIdx]
	}

	// Load from the inserted row (from bottom)
	if err := e.loadFromRowId(keyVals, false, e.table.selectedCol); err != nil {
		e.SetStatusError(err.Error())
		return err
	}

	// Select the first row (which should be the newly inserted one)
	if e.records[e.lastRowIdx()] == nil {
		e.table.Select(len(e.records)-2, e.table.selectedCol)
	} else {
		e.table.Select(len(e.records)-1, e.table.selectedCol)
	}
	return nil
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
	// TableView handles selection highlighting internally
	// This is a placeholder for future row highlighting implementation
}

func (e *Editor) unhighlightRow(row int) {
	// TableView handles selection highlighting internally
	// This is a placeholder for future row highlighting implementation
}

func (e *Editor) highlightColumn(col int) {
	// TableView handles selection highlighting internally
	// This is a placeholder for future column highlighting implementation
}

func (e *Editor) unhighlightColumn(col int) {
	// TableView handles selection highlighting internally
	// This is a placeholder for future column highlighting implementation
}

// TODO rework this, currently used for initial load, refresh does not account for e.pointer
func (e *Editor) refreshData() {
	if e.nextQuery != nil || e.relation == nil || e.relation.DB == nil {
		return
	}

	colCount := len(e.columns)
	if colCount == 0 || len(e.records) == 0 {
		return
	}

	selectCols := make([]string, colCount)
	for i, col := range e.columns {
		selectCols[i] = quoteIdent(e.relation.DBType, col.Name)
	}

	var rows *sql.Rows
	var err error
	if len(e.records) == 0 || e.records[e.pointer] == nil {
		rows, err = e.relation.QueryRows(selectCols, nil, nil, true, true)
		if err != nil {
			if e.statusBar != nil {
				e.statusBar.SetText("[red]ERROR: " + err.Error() + "[white]")
			}
			return
		}
	} else {
		rows, err = e.relation.QueryRows(selectCols, nil, e.records[e.pointer][:len(e.relation.key)], true, true)
		if err != nil {
			if e.statusBar != nil {
				e.statusBar.SetText("[red]ERROR: " + err.Error() + "[white]")
			}
			return
		}
	}

	e.pointer = 0 // Reset pointer for fresh load

	// Load initial rows up to the preallocated size
	rowsLoaded := 0
	for i := 0; i < len(e.records) && rows.Next(); i++ {
		e.records[i] = make([]any, len(e.columns))
		scanTargets := make([]any, len(e.columns))
		for j := 0; j < len(e.columns); j++ {
			scanTargets[j] = &e.records[i][j]
		}
		if err := rows.Scan(scanTargets...); err != nil {
			rows.Close()
			if e.statusBar != nil {
				e.statusBar.SetText("[red]ERROR: " + err.Error() + "[white]")
			}
			return
		}
		rowsLoaded++
	}

	// If we didn't fill the buffer, mark the end with nil
	if err := rows.Close(); err != nil {
		panic(err)
	}
	if rowsLoaded < len(e.records) {
		e.records = e.records[:rowsLoaded-1]
		e.records = append(e.records, nil)
	}
	e.renderData()
}

func (e *Editor) loadFromRowId(id []any, fromTop bool, focusColumn int) error {
	if e.relation == nil || e.relation.DB == nil {
		return fmt.Errorf("no database connection available")
	}

	// Stop refresh timer when starting a new query
	e.stopRefreshTimer()

	selectCols := make([]string, len(e.columns))
	for i, col := range e.columns {
		selectCols[i] = col.Name
	}

	colCount := len(e.columns)
	if colCount == 0 {
		return nil
	}

	var rows *sql.Rows
	var err error
	if fromTop {
		// Load from top: use QueryRows with inclusive true, scrollDown true
		rows, err = e.relation.QueryRows(selectCols, nil, id, true, true)
		if err != nil {
			return err
		}
		e.nextQuery = rows
		e.startRowsTimer()

		// Scan rows into e.records starting from pointer
		e.pointer = 0
		rowsLoaded := 0
		scanTargets := make([]any, colCount)
		for i := 0; i < len(e.records) && rows.Next(); i++ {
			row := make([]any, colCount)
			for j := 0; j < colCount; j++ {
				scanTargets[j] = &row[j]
			}
			if err := rows.Scan(scanTargets...); err != nil {
				return err
			}
			e.records[i] = row
			rowsLoaded++
		}

		// Mark end with nil if we didn't fill the buffer
		if rowsLoaded < len(e.records) {
			e.records[rowsLoaded] = nil
		}
	} else {
		// Load from bottom: use QueryRows with inclusive true, scrollDown false
		rows, err = e.relation.QueryRows(selectCols, nil, id, true, false)
		if err != nil {
			return err
		}
		e.prevQuery = rows
		e.startRowsTimer()

		// Scan rows into e.records in reverse, starting from end of buffer
		e.pointer = 0
		scanTargets := make([]any, colCount)
		rowsLoaded := 0
		tempRows := make([][]any, 0, len(e.records))

		// First collect all rows
		for rows.Next() {
			row := make([]any, colCount)
			for j := 0; j < colCount; j++ {
				scanTargets[j] = &row[j]
			}
			if err := rows.Scan(scanTargets...); err != nil {
				return err
			}
			tempRows = append(tempRows, row)
			rowsLoaded++
			if rowsLoaded >= len(e.records)-1 {
				break
			}
		}

		// Reverse the rows and place them at the end of the buffer
		for i := len(tempRows) - 1; i >= 0; i-- {
			idx := len(tempRows) - 1 - i
			e.records[idx] = tempRows[i]
		}
		e.records[len(e.records)-1] = nil

		// Mark end with nil if we didn't fill the buffer
		if rowsLoaded < len(e.records) {
			e.records[rowsLoaded] = nil
		}
		e.records[len(e.records)-1] = nil
		e.pointer = 0
	}
	e.renderData()

	// Focus on the specified column
	if fromTop {
		e.table.Select(0, focusColumn)
	} else {
		e.table.Select(len(e.records)-1, focusColumn)
	}

	return nil
}

// nextRows fetches the next i rows from e.rows and resets the auto-close timer
// when there are no more rows, adds a nil sentinel to mark the end
// returns bool, err. bool if the edge of table is reached
func (e *Editor) nextRows(i int) (bool, error) {
	// Check if we're already at the end (last record is nil)
	if len(e.records) == 0 || e.records[e.lastRowIdx()] == nil {
		return false, nil // No-op, already at end of data
	}

	if e.nextQuery == nil {
		// Stop refresh timer when starting a new query
		e.stopRefreshTimer()

		params := make([]any, len(e.relation.key))
		lastRecordIdx := (e.pointer - 1 + len(e.records)) % len(e.records)
		if e.records[lastRecordIdx] == nil {
			return false, nil // Can't query from nil record
		}
		for i := range e.relation.key {
			params[i] = e.records[lastRecordIdx][i]
		}
		selectCols := make([]string, len(e.columns))
		for i, col := range e.columns {
			selectCols[i] = col.Name
		}
		var err error
		e.nextQuery, err = e.relation.QueryRows(selectCols, nil, params, false, true)
		if err != nil {
			return false, err
		}
		e.startRowsTimer()
	}

	// Signal timer reset
	if e.rowsTimerReset != nil {
		select {
		case e.rowsTimerReset <- struct{}{}:
		default:
		}
	}

	colCount := len(e.columns)
	if colCount == 0 {
		return false, nil
	}

	scanTargets := make([]any, colCount)

	rowsFetched := 0
	for ; rowsFetched < i && e.nextQuery.Next(); rowsFetched++ {
		row := make([]any, colCount)
		for j := 0; j < colCount; j++ {
			scanTargets[j] = &row[j]
		}
		if err := e.nextQuery.Scan(scanTargets...); err != nil {
			return false, err
		}
		e.records[e.pointer+rowsFetched] = row
	}
	// new pointer position
	e.incrPtr(rowsFetched)
	// If we fetched fewer rows than requested, we've reached the end
	if rowsFetched < i {
		e.incrPtr(1)
		e.records[e.lastRowIdx()] = nil // Mark end of data
	}
	e.renderData()
	return rowsFetched < i, e.nextQuery.Err()
}

func (e *Editor) lastRowIdx() int {
	return (e.pointer + len(e.records) - 1) % len(e.records)
}

func (e *Editor) incrPtr(n int) {
	e.pointer = (e.pointer + n) % len(e.records)
}

// prevRows fetches the previous i rows (scrolling backwards in the circular buffer)
// returns whether rows were moved (false means you'ved the edge)
func (e *Editor) prevRows(i int) (bool, error) {
	if e.prevQuery == nil {
		// Stop refresh timer when starting a new query
		e.stopRefreshTimer()
		if len(e.records) == 0 || e.records[e.pointer] == nil {
			return false, nil // Can't query from nil or empty records
		}
		params := make([]any, len(e.relation.key))
		for i := range e.relation.key {
			params[i] = e.records[e.pointer][i]
		}
		selectCols := make([]string, len(e.columns))
		for i, col := range e.columns {
			selectCols[i] = col.Name
		}
		var err error
		e.prevQuery, err = e.relation.QueryRows(selectCols, nil, params, false, false)
		if err != nil {
			return false, err
		}
		e.startRowsTimer()
	}

	// Signal timer reset
	if e.rowsTimerReset != nil {
		select {
		case e.rowsTimerReset <- struct{}{}:
		default:
		}
	}

	colCount := len(e.columns)
	if colCount == 0 {
		return false, nil
	}

	scanTargets := make([]any, colCount)

	rowsFetched := 0
	for ; rowsFetched < i && e.prevQuery.Next(); rowsFetched++ {
		row := make([]any, colCount)
		for j := 0; j < colCount; j++ {
			scanTargets[j] = &row[j]
		}
		if err := e.prevQuery.Scan(scanTargets...); err != nil {
			return false, err
		}
		e.pointer = e.lastRowIdx() // Move pointer backwards in the circular buffer
		e.records[e.pointer] = row
	}

	e.renderData()
	return rowsFetched < i, e.prevQuery.Err()
}

// startRowsTimer starts or restarts the timer for auto-closing queries
func (e *Editor) startRowsTimer() {
	// Stop existing timer if any
	if e.rowsTimer != nil {
		e.rowsTimer.Stop()
	}
	if e.rowsTimerReset != nil {
		close(e.rowsTimerReset)
	}

	// Start new timer
	e.rowsTimerReset = make(chan struct{})
	e.rowsTimer = time.NewTimer(RowsTimerInterval)

	// Timer goroutine to handle resets
	go func() {
		resetChan := e.rowsTimerReset
		timer := e.rowsTimer
		for {
			select {
			case <-resetChan:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(RowsTimerInterval)
			case <-timer.C:
				e.stopRowsTimer()
				return
			}
		}
	}()
}

// stopRowsTimer stops the timer and closes the rows if active
func (e *Editor) stopRowsTimer() {
	if e.rowsTimer != nil {
		e.rowsTimer.Stop()
		e.rowsTimer = nil
	}
	if e.rowsTimerReset != nil {
		close(e.rowsTimerReset)
		e.rowsTimerReset = nil
	}
	if e.nextQuery != nil {
		if err := e.nextQuery.Close(); err != nil {
			panic(err)
		}
		e.nextQuery = nil
	}
	if e.prevQuery != nil {
		if err := e.prevQuery.Close(); err != nil {
			panic(err)
		}
		e.prevQuery = nil
	}

	// Start refresh timer after closing queries
	e.refreshData()
	e.startRefreshTimer()
}

// startRefreshTimer starts a timer that refreshes data every 300ms
func (e *Editor) startRefreshTimer() {
	if e.app == nil {
		return
	}

	// Stop existing refresh timer if any
	e.stopRefreshTimer()

	// Start new refresh timer
	e.refreshTimerStop = make(chan struct{})
	e.refreshTimer = time.NewTimer(RefreshTimerInterval)

	// Refresh timer goroutine
	go func() {
		stopChan := e.refreshTimerStop
		timer := e.refreshTimer
		app := e.app
		if stopChan == nil || timer == nil {
			return
		}
		for {
			select {
			case <-stopChan:
				return
			case <-timer.C:
				if app != nil && e.relation != nil && e.relation.DB != nil {
					app.QueueUpdateDraw(func() {
						e.refreshData()
					})
				}
				timer.Reset(RefreshTimerInterval)
			}
		}
	}()
}

// stopRefreshTimer stops the refresh timer if active
func (e *Editor) stopRefreshTimer() {
	if e.refreshTimer != nil {
		e.refreshTimer.Stop()
		e.refreshTimer = nil
	}
	if e.refreshTimerStop != nil {
		close(e.refreshTimerStop)
		e.refreshTimerStop = nil
	}
	if e.prevQuery != nil {
		if err := e.prevQuery.Close(); err != nil {
			panic(err)
		}
		e.prevQuery = nil
	}
	if e.nextQuery != nil {
		if err := e.nextQuery.Close(); err != nil {
			panic(err)
		}
		e.nextQuery = nil
	}
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

	for i := range e.records {
		e.records[i][col], e.records[i][newIdx] = e.records[i][newIdx], e.records[i][col]
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

func formatCellValue(value any, cellStyle tcell.Style) (string, tcell.Style) {
	if value == EmptyCellValue {
		return "", cellStyle
	}
	if value == nil {
		return NullDisplay, cellStyle.Italic(true).Foreground(tcell.ColorGray)
	}

	switch v := value.(type) {
	case []byte:
		return string(v), cellStyle
	case string:
		if v == "" {
			return "", cellStyle
		}
		if v == "null" {
			return "null", cellStyle
		}
		return v, cellStyle
	case int64:
		return strconv.FormatInt(v, 10), cellStyle
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), cellStyle
	case bool:
		if v {
			return "true", cellStyle
		}
		return "false", cellStyle
	case time.Time:
		// Format timestamps in ISO 8601 format
		// Use RFC3339 format for timestamps with time zones
		// or date-only format for dates without time component
		if v.Hour() == 0 && v.Minute() == 0 && v.Second() == 0 && v.Nanosecond() == 0 {
			return v.Format("2006-01-02"), cellStyle
		}
		return v.Format(time.RFC3339), cellStyle
	default:
		return fmt.Sprintf("%v", value), cellStyle
	}
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

// Status bar API methods
func (e *Editor) SetStatusMessage(message string) {
	if e.statusBar != nil {
		e.statusBar.SetText(message)
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

// SQL execution
func (e *Editor) executeSQL(query string) {
	if e.relation == nil || e.relation.DB == nil {
		e.SetStatusError("No database connection available")
		return
	}

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

// Goto execution
func (e *Editor) executeGoto(gotoValue string) {
	if e.relation == nil {
		e.SetStatusError("No database connection available")
		return
	}

	row, col := e.table.GetSelection()
	if row < 0 || row >= len(e.records) || e.records[row] == nil {
		e.SetStatusError("Invalid current row")
		return
	}

	// Get current row's key values
	currentKeys := make([]any, len(e.relation.key))
	for i := range e.relation.key {
		currentKeys[i] = e.records[row][i]
	}

	// Find the column index in relation.attributeOrder
	gotoCol := -1
	for i, name := range e.relation.attributeOrder {
		if name == e.columns[col].Name {
			gotoCol = i
			break
		}
	}

	if gotoCol == -1 {
		e.SetStatusError("Column not found in relation")
		return
	}

	// Use FindNextRow to search for the next matching row
	foundKeys, foundBelow, err := e.relation.FindNextRow(gotoCol, gotoValue, nil, nil, currentKeys)
	if err != nil {
		e.SetStatusError(err.Error())
		return
	}

	if foundKeys == nil {
		e.SetStatusMessage("No match found")
		return
	}

	// Check if the found row is within the current record window
	foundInWindow := false
	var foundRow int

	for i := 0; i < len(e.records); i++ {
		if e.records[i] == nil {
			break
		}
		// Compare key values
		match := true
		for j := range e.relation.key {
			if e.records[i][j] != foundKeys[j] {
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
				e.SetStatusError(err.Error())
				return
			}
		} else {
			// Found row wrapped around to before current window, load from top
			if err := e.loadFromRowId(foundKeys, true, col); err != nil {
				e.SetStatusError(err.Error())
				return
			}
		}
		e.SetStatusMessage("Match found")
	}
}
