package main

import (
	"database/sql"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const (
	DefaultColumnWidth   = 8
	RowsTimerInterval    = 100 * time.Millisecond
	RefreshTimerInterval = 300 * time.Millisecond
	pagePicker           = "picker"
	pageTable            = "table"
	chromeHeight         = 6
)

// this is a config concept
// width = 0 means Column is hidden but selected
type Column struct {
	Name  string
	Width int
}

type RowState int

const (
	RowStateNormal RowState = iota
	RowStateNew
	RowStateDeleted
)

type Row struct {
	state    RowState
	data     []any
	modified []int // indices of columns that were modified in the last refresh
}

// columns are display, relation.attributes stores the database attributes
// lookup key is unique: it's always selected
// if a multicolumn reference is selected, all columns in the reference are selected
type Editor struct {
	app      *tview.Application
	pages    *tview.Pages
	table    *TableView
	columns  []Column // TODO move this into Config
	relation *Relation
	config   *Config

	// Database connection (stored separately to support table switching when relation is nil)
	db     *sql.DB
	dbType DatabaseType

	// references to key components
	tablePicker    *FuzzySelector // Table picker at the top
	statusBar      *tview.TextView
	commandPalette *tview.InputField
	layout         *tview.Flex

	paletteMode         PaletteMode
	kittySequenceActive bool
	kittySequenceBuffer string

	// selection
	editing bool

	// data, records is a circular buffer
	nextQuery *sql.Rows // nextRows
	prevQuery *sql.Rows // prevRows
	pointer   int       // pointer to the current record
	records   []Row     // columns are keyed off e.columns

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
	PaletteModeDelete
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
	case PaletteModeDelete:
		return "✗ "
	default:
		return "> "
	}
}

// mouseActionString converts tview.MouseAction to a human-readable string
func mouseActionString(action tview.MouseAction) string {
	switch action {
	case tview.MouseScrollUp:
		return "ScrollUp"
	case tview.MouseScrollDown:
		return "ScrollDown"
	case tview.MouseLeftClick:
		return "LeftClick"
	case tview.MouseRightClick:
		return "RightClick"
	case tview.MouseMiddleClick:
		return "MiddleClick"
	case tview.MouseMove:
		return "Move"
	case tview.MouseLeftDoubleClick:
		return "LeftDoubleClick"
	default:
		return fmt.Sprintf("Unknown(%d)", action)
	}
}

func runEditor(config *Config, dbname, tablename string) error {
	tview.Styles.ContrastBackgroundColor = tcell.ColorBlack

	db, dbType, err := config.connect()
	if err != nil {
		CaptureError(err)
		return err
	}
	defer db.Close()

	// Get terminal height for optimal data loading
	terminalHeight := getTerminalHeight()
	tableDataHeight := terminalHeight - chromeHeight // 3 lines for picker bar, status bar, command palette

	var relation *Relation
	var columns []Column

	// Only load table if tablename is provided
	if tablename != "" {
		var err error
		relation, err = NewRelation(db, dbType, tablename)
		if err != nil {
			CaptureError(err)
			return err
		}

		// force key to be first column(s)
		columns = make([]Column, 0, len(relation.attributeOrder))
		for _, name := range relation.key {
			columns = append(columns, Column{Name: name, Width: 4})
		}
		for _, name := range relation.attributeOrder {
			if !slices.Contains(relation.key, name) {
				columns = append(columns, Column{Name: name, Width: DefaultColumnWidth})
			}
		}
	} else {
		// No table specified - create empty state
		columns = []Column{}
	}

	// Get available tables for the picker
	tables, err := config.GetTables()
	if err != nil {
		CaptureError(err)
		return err
	}

	app := tview.NewApplication().SetTitle(fmt.Sprintf("ted %s %s",
		dbname, databaseIcons[dbType])).EnableMouse(true)

	editor := &Editor{
		app:         app,
		pages:       tview.NewPages(),
		table:       nil, // Will be initialized after we have access to all editor fields
		columns:     columns,
		relation:    relation,
		config:      config,
		db:          db,
		dbType:      dbType,
		tablePicker: nil, // Will be initialized after editor is created
		paletteMode: PaletteModeDefault,
		pointer:     0,
		records:     make([]Row, tableDataHeight),
	}

	// Create selector with callback now that editor exists
	editor.tablePicker = NewFuzzySelector(tables, tablename, editor.selectTableFromPicker, func() {
		// Close callback: hide picker and return focus to table
		editor.pages.HidePage(pagePicker)
		editor.app.SetFocus(editor.table)
		editor.app.SetAfterDrawFunc(nil) // Clear cursor style function
		editor.setCursorStyle(0)         // Reset to default cursor style
	})

	// Initialize table with configuration
	headers := make([]HeaderColumn, len(editor.columns))
	for i, col := range editor.columns {
		headers[i] = HeaderColumn{
			Name:  col.Name,
			Width: col.Width,
		}
	}

	keyColumnCount := 0
	if editor.relation != nil {
		keyColumnCount = len(editor.relation.key)
	}

	editor.table = NewTableView(tableDataHeight, &TableViewConfig{
		Headers:        headers,
		KeyColumnCount: keyColumnCount,
		DoubleClickFunc: func(row, col int) {
			editor.enterEditMode(row, col)
		},
		SingleClickFunc: func(row, col int) {
			if editor.editing {
				editor.exitEditMode()
			}
		},
		TableNameClickFunc: func() {
			editor.pages.ShowPage(pagePicker)
			editor.app.SetFocus(editor.tablePicker)
			editor.app.SetAfterDrawFunc(func(screen tcell.Screen) {
				screen.SetCursorStyle(tcell.CursorStyleBlinkingBar)
			})
		},
		MouseScrollFunc: func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse) {
			// Record mouse event in breadcrumbs
			if breadcrumbs != nil && event != nil {
				actionStr := mouseActionString(action)
				breadcrumbs.RecordMouse(actionStr)
			}

			switch action {
			case tview.MouseScrollUp:
				go func() {
					reachedEnd, err := editor.prevRows(1)
					if err != nil {
						return
					}
					editor.app.QueueUpdateDraw(func() {
						if !reachedEnd {
							editor.table.Select(editor.table.selectedRow+1, editor.table.selectedCol)
						}
					})
				}()
				return tview.MouseConsumed, nil
			case tview.MouseScrollDown:
				go func() {
					reachedEnd, err := editor.nextRows(1)
					if err != nil {
						return
					}
					editor.app.QueueUpdateDraw(func() {
						if !reachedEnd {
							editor.table.Select(editor.table.selectedRow-1, editor.table.selectedCol)
						}
					})
				}()
				return tview.MouseConsumed, nil
			case tview.MouseScrollLeft:
				editor.table.viewport.ScrollLeft()
				return tview.MouseConsumed, nil
			case tview.MouseScrollRight:
				editor.table.viewport.ScrollRight()
				return tview.MouseConsumed, nil
			}
			return action, event
		},
	})

	editor.table.SetTableName(tablename)

	// Only load data if we have a table
	if tablename != "" {
		editor.loadFromRowId(nil, true, 0, false)
	}
	editor.setupKeyBindings()
	editor.setupStatusBar()
	editor.setupCommandPalette()

	// Setup layout without the selector (it will be overlaid when visible)
	editor.layout = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(editor.table, 0, 1, true).
		AddItem(editor.statusBar, 1, 0, false).
		AddItem(editor.commandPalette, 1, 0, false)

	// Setup resize handler
	editor.pages.SetChangedFunc(func() {
		// Don't handle resize when in edit mode to avoid deadlock
		if editor.editing {
			return
		}

		// Get the new terminal height
		newHeight := getTerminalHeight()
		newDataHeight := newHeight - chromeHeight // 3 lines for picker bar, status bar, command palette

		// Only resize if the height has changed significantly
		if newDataHeight != editor.table.rowsHeight && newDataHeight > 0 {
			if newDataHeight > len(editor.records) && editor.records[len(editor.records)-1].data == nil {
				// the table is smaller than the new data height, no need to fetch more rows
				return
			}
			// Create new records buffer with the new size
			newRecords := make([]Row, newDataHeight)

			// Copy existing data to the new buffer
			copyCount := min(len(editor.records), newDataHeight)
			for i := 0; i < copyCount; i++ {
				ptr := (i + editor.pointer) % len(editor.records)
				newRecords[i] = editor.records[ptr]
			}

			// If the new buffer is larger, fetch more rows to fill it
			if newDataHeight > len(editor.records) && editor.records[len(editor.records)-1].data != nil {
				// We need to fetch more rows
				oldLen := len(editor.records)
				editor.records = newRecords
				editor.pointer = 0
				// Fetch additional rows
				editor.nextRows(newDataHeight - oldLen)
			} else {
				editor.records = newRecords
				editor.pointer = 0
			}

			editor.renderData()
			go editor.app.Draw()
		}
	})

	editor.table.SetSelectionChangeFunc(func(row, col int) {
		editor.updateStatusWithCellContent()
		// Auto-scroll viewport to show the selected column
		editor.ensureColumnVisible(col)
	})

	// Add main table page
	editor.pages.AddPage(pageTable, editor.layout, true, true)

	// Create picker overlay page (centered flex with selector at top)
	pickerOverlay := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(editor.tablePicker, 8, 0, true). // Selector with height for dropdown
		AddItem(nil, 0, 1, false)                // Spacer takes rest of screen
	editor.pages.AddPage(pagePicker, pickerOverlay, true, false)

	// If no table was specified, show the picker immediately
	if tablename == "" {
		editor.pages.ShowPage(pagePicker)
		editor.app.SetFocus(editor.tablePicker)
		editor.app.SetAfterDrawFunc(func(screen tcell.Screen) {
			screen.SetCursorStyle(tcell.CursorStyleBlinkingBar)
		})
	}

	if err := editor.app.SetRoot(editor.pages, true).Run(); err != nil {
		CaptureError(err)
		return err
	}
	return nil
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
				}
			case PaletteModeGoto:
				e.executeGoto(command)
			case PaletteModeDelete:
				e.executeDelete()
			}

			// For Goto mode, keep the palette open with text selected
			if mode == PaletteModeGoto {
				return nil
			}

			e.setPaletteMode(PaletteModeDefault, false)
			e.app.SetFocus(e.table)
			return nil
		case tcell.KeyEscape:
			if e.paletteMode == PaletteModeInsert && e.editing {
				return event
			}
			if e.paletteMode == PaletteModeDelete {
				e.setPaletteMode(PaletteModeDefault, false)
				e.app.SetFocus(e.table)
				return nil
			}
			e.setPaletteMode(PaletteModeDefault, false)
			e.app.SetFocus(e.table)
			return nil
		}
		return event
	})
}

func (e *Editor) setupKeyBindings() {
	e.table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		key := event.Key()
		rune := event.Rune()
		mod := event.Modifiers()

		row, col := e.table.GetSelection()

		// Disable all selection navigation when in delete mode (except Enter to confirm and Escape to cancel)
		if e.paletteMode == PaletteModeDelete {
			switch key {
			case tcell.KeyEnter, tcell.KeyEscape:
				// Allow these to fall through for delete mode handling
			case tcell.KeyUp, tcell.KeyDown, tcell.KeyLeft, tcell.KeyRight,
				tcell.KeyHome, tcell.KeyEnd, tcell.KeyPgUp, tcell.KeyPgDn,
				tcell.KeyTab, tcell.KeyBacktab, tcell.KeyBackspace, tcell.KeyBackspace2,
				tcell.KeyDelete:
				// Block all navigation and editing keys
				return nil
			default:
				// Block any other keys except control keys
				if key == tcell.KeyRune {
					return nil
				}
			}
		}

		// Record keyboard event in breadcrumbs (but not during edit mode or command input)
		if breadcrumbs != nil && !e.editing {
			keyStr := fmt.Sprintf("%v", key)
			if key == tcell.KeyRune {
				keyStr = string(rune)
			}
			modStr := ""
			if mod&tcell.ModCtrl != 0 {
				modStr += "Ctrl+"
			}
			if mod&tcell.ModShift != 0 {
				modStr += "Shift+"
			}
			if mod&tcell.ModAlt != 0 {
				modStr += "Alt+"
			}
			breadcrumbs.RecordKeyboard(keyStr, modStr)
		}

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
		if (rune == 's' || rune == 19) && mod&tcell.ModCtrl != 0 {
			if len(e.table.insertRow) > 0 && !e.editing {
				e.executeInsert()
				return nil
			}
		}

		// Ctrl+O: open/close table picker
		if (rune == 'o' || rune == 15) && mod&tcell.ModCtrl != 0 {
			e.pages.ShowPage(pagePicker)
			// Set focus on the selector, which will forward focus to the input field
			e.app.SetFocus(e.tablePicker)
			// Set cursor style to blinking bar
			e.app.SetAfterDrawFunc(func(screen tcell.Screen) {
				screen.SetCursorStyle(tcell.CursorStyleBlinkingBar)
			})
			return nil
		}
		// Ctrl+R: Insert mode
		if (rune == 'r' || rune == 18) && mod&tcell.ModCtrl != 0 {
			// Ctrl+I: Jump to end and enable insert mode
			e.loadFromRowId(nil, false, 0, false)
			e.nextRows(1)
			e.table.SetupInsertRow()
			e.updateStatusForInsertMode()
			e.renderData()
			_, col := e.table.GetSelection()
			e.table.Select(e.table.GetDataLength(), col)
			return nil
		}

		// Alt+0: set cell to null for nullable columns in insert mode
		if rune == '0' && mod&tcell.ModAlt != 0 {
			if len(e.table.insertRow) > 0 && !e.editing && e.relation != nil {
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
		case key == tcell.KeyEnter:
			if len(e.table.insertRow) > 0 && !e.editing && mod&tcell.ModAlt != 0 {
				e.executeInsert()
				return nil
			}
			// Execute delete in delete mode
			if e.paletteMode == PaletteModeDelete {
				// Check if we're at the bottom of the table
				e.executeDelete()
				e.setPaletteMode(PaletteModeDefault, false)
				e.app.SetFocus(e.table)
				e.table.Select(row, col)
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

			if e.paletteMode == PaletteModeDelete {
				e.setPaletteMode(PaletteModeDefault, false)
				e.app.SetFocus(e.table)
				e.SetStatusMessage("Ready")
				return nil
			}

			if len(e.table.insertRow) > 0 {
				// Exit insert mode and select last real row
				e.table.ClearInsertRow()

				// Find the last non-nil record
				lastIdx := len(e.records) - 1
				for lastIdx >= 0 && e.records[lastIdx].data == nil {
					lastIdx--
				}

				e.renderData()
				_, col := e.table.GetSelection()
				e.table.Select(lastIdx, col)
				return nil
			}
			e.exitEditMode()
			return nil
		case key == tcell.KeyTab:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			e.navigateTab(false)
			return nil
		case key == tcell.KeyBacktab:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			e.navigateTab(true)
			return nil
		case key == tcell.KeyHome:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			if mod&tcell.ModCtrl != 0 {
				if len(e.table.insertRow) > 0 {
					return nil // Disable vertical navigation in insert mode
				}
				// Ctrl+Home: jump to first row
				e.loadFromRowId(nil, true, col, false)
				e.table.Select(0, col)
				return nil
			}
			e.table.Select(row, 0)
			return nil
		case key == tcell.KeyEnd:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			if mod&tcell.ModCtrl != 0 {
				if len(e.table.insertRow) > 0 {
					return nil // Disable vertical navigation in insert mode
				}
				// Ctrl+End: jump to last row
				e.loadFromRowId(nil, false, col, false)
				e.table.Select(e.lastRowIdx()-1, col) // -1 because the last row is the bottom border
				return nil
			}
			e.table.Select(row, len(e.columns)-1)
			return nil
		case key == tcell.KeyPgUp:
			if len(e.table.insertRow) > 0 || e.paletteMode == PaletteModeDelete {
				return nil // Disable vertical navigation in insert mode or delete mode
			}
			// Page up: scroll data backward while keeping selection in same visual position
			pageSize := max(1, e.table.rowsHeight-1)
			e.prevRows(pageSize)
			return nil
		case key == tcell.KeyPgDn:
			if len(e.table.insertRow) > 0 || e.paletteMode == PaletteModeDelete {
				return nil // Disable vertical navigation in insert mode or delete mode
			}
			// Page down: scroll data forward while keeping selection in same visual position
			pageSize := max(1, e.table.rowsHeight-1)
			// Keep selection at same position, just fetch next rows
			e.nextRows(pageSize)
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
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			e.moveColumn(col, -1)
			return nil
		case key == tcell.KeyRight && mod&tcell.ModAlt != 0:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			e.moveColumn(col, 1)
			return nil
		case key == tcell.KeyRune && rune == '=' && mod&tcell.ModCtrl != 0:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			e.adjustColumnWidth(col, 1)
			return nil
		case key == tcell.KeyRune && rune == '-' && mod&tcell.ModCtrl != 0:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			e.adjustColumnWidth(col, -1)
			return nil
		case key == tcell.KeyLeft && mod&tcell.ModMeta != 0:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			e.table.Select(row, 0)
			return nil
		case key == tcell.KeyRight && mod&tcell.ModMeta != 0:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			e.table.Select(row, len(e.columns)-1)
			return nil
		case key == tcell.KeyLeft:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
		case key == tcell.KeyRight:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
		case key == tcell.KeyUp:
			if len(e.table.insertRow) > 0 || e.paletteMode == PaletteModeDelete {
				return nil // Disable vertical navigation in insert mode or delete mode
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
			if len(e.table.insertRow) > 0 || e.paletteMode == PaletteModeDelete {
				return nil // Disable vertical navigation in insert mode or delete mode
			}
			if mod&tcell.ModMeta != 0 {
				if len(e.records[len(e.records)-1].data) == 0 {
					e.table.Select(len(e.records)-2, col)
				} else {
					e.table.Select(len(e.records)-1, col)
				}
				return nil
			} else {
				if row == len(e.records)-1 {
					e.nextRows(1)
				} else {
					if len(e.records[row+1].data) == 0 {
						e.table.Select(row+2, col)
					} else {
						e.table.Select(row+1, col)
					}
				}
				return nil
			}
		case key == tcell.KeyBackspace || key == tcell.KeyBackspace2 || key == tcell.KeyDEL || key == tcell.KeyDelete:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			// Backspace or Delete: start editing with empty string
			e.enterEditModeWithInitialValue(row, col, "")
			return nil
		case (rune == 'd' || rune == 4) && mod&tcell.ModCtrl != 0:
			// Ctrl+D: enter delete mode
			e.enterDeleteMode(row, col)
			return nil
		default:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return event
			}
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
					// Check if Ctrl is pressed (bit 2, value 4)
					if mask&4 != 0 {
						_, col := e.table.GetSelection()
						switch codepoint {
						case 96: // Ctrl+` (backtick)
							e.setPaletteMode(PaletteModeSQL, true)
						case 105: // Ctrl+I: Jump to end and enable insert mode
							e.table.SetupInsertRow()
							e.loadFromRowId(nil, false, 0, false)
							e.updateStatusForInsertMode()
							// Select the insert mode row (which is at index len(data))
							e.table.Select(e.table.GetDataLength(), col)
						case 61: // Ctrl+= (increase column width)
							if e.paletteMode != PaletteModeDelete {
								e.adjustColumnWidth(col, 1)
							}
						case 45: // Ctrl+- (decrease column width)
							if e.paletteMode != PaletteModeDelete {
								e.adjustColumnWidth(col, -1)
							}
						}
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
func (e *Editor) updateForeignKeyPreview(row, col int, newText string) {
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
				foreignKeys[foreignCol] = e.records[row].data[attrIdx]
			}
		}
		// TODO pass config columns if available
		preview, err := getForeignRow(e.relation.DB, ref.ForeignTable, foreignKeys, nil)
		if err != nil {
			e.SetStatusErrorWithSentry(err)
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
	// Record navigation event in breadcrumbs
	if breadcrumbs != nil && mode != e.paletteMode {
		modeStr := fmt.Sprintf("%v", mode)
		switch mode {
		case PaletteModeDefault:
			modeStr = "Default"
		case PaletteModeCommand:
			modeStr = "Command"
		case PaletteModeSQL:
			modeStr = "SQL"
		case PaletteModeGoto:
			modeStr = "Goto"
		case PaletteModeUpdate:
			modeStr = "Update"
		case PaletteModeInsert:
			modeStr = "Insert"
		case PaletteModeDelete:
			modeStr = "Delete"
		}
		breadcrumbs.RecordNavigation(modeStr, "Palette mode changed")
	}

	// Handle delete mode state changes
	wasDeleteMode := e.paletteMode == PaletteModeDelete
	isDeleteMode := mode == PaletteModeDelete

	e.paletteMode = mode
	e.commandPalette.SetLabel(mode.Glyph())
	// Clear input when switching modes
	e.commandPalette.SetText("")
	style := e.commandPalette.GetPlaceholderStyle().Italic(true)
	e.commandPalette.SetPlaceholderStyle(style)

	// Update table view delete mode state
	if e.table != nil {
		e.table.SetDeleteMode(isDeleteMode)
	}

	// Update status bar background color
	if e.statusBar != nil {
		if isDeleteMode && !wasDeleteMode {
			// Entering delete mode: set status bar to red
			e.statusBar.SetBackgroundColor(tcell.ColorRed)
		} else if !isDeleteMode && wasDeleteMode {
			// Exiting delete mode: restore status bar to light gray
			e.statusBar.SetBackgroundColor(tcell.ColorLightGray)
		}
	}
	if e.table.insertRow != nil {
		e.commandPalette.SetPlaceholder("INSERT preview… (Esc to exit)")
	}
	if e.editing {
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
			e.commandPalette.SetPlaceholder("Ctrl+… I: Insert · `: SQL · G: Goto · D: Delete · C: Exit")
		case PaletteModeCommand:
			e.commandPalette.SetPlaceholder("Command… (Esc to exit)")
		case PaletteModeSQL:
			e.commandPalette.SetPlaceholder("Execute SQL… (Esc to exit)")
		case PaletteModeGoto:
			e.commandPalette.SetPlaceholder("Goto next matching value… (Esc to exit)")
		case PaletteModeUpdate:
			// No placeholder in update mode
			e.commandPalette.SetPlaceholder("")
		case PaletteModeInsert:
			// No placeholder in insert mode
			e.commandPalette.SetPlaceholder("")
		case PaletteModeDelete:
			// No placeholder in delete mode
			e.commandPalette.SetPlaceholder("")
		}
	}
	if focus {
		e.app.SetFocus(e.commandPalette)
	}
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

// e.table.SetData based on e.records and e.pointer
func (e *Editor) renderData() {
	// When in insert mode, we need to reserve one slot for the insert mode row
	// that will be rendered by TableView
	dataCount := len(e.records)
	if len(e.table.insertRow) > 0 {
		// Find the last non-nil record
		lastIdx := len(e.records) - 1
		for lastIdx >= 0 && e.records[lastIdx].data == nil {
			lastIdx--
		}
		dataCount = lastIdx + 1 // Only pass real data, TableView will add insert mode row
	}

	normalizedRows := make([]Row, dataCount)
	for i := 0; i < dataCount; i++ {
		ptr := (i + e.pointer) % len(e.records)
		// insert mode row needs "space" at the top to still be able to render
		// the last db row
		if len(e.table.insertRow) > 0 && i == 0 {
			ptr++
		}
		normalizedRows[i] = e.records[ptr] // Reference to Row, not a copy
	}
	if len(e.table.insertRow) > 0 {
		normalizedRows = slices.Delete(normalizedRows, 0, 1)
	}
	e.table.SetDataReferences(normalizedRows)
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

// extractKeys returns a copy of the key values from a row
func (e *Editor) extractKeys(row []any) []any {
	if row == nil || len(row) < len(e.relation.key) {
		return nil
	}
	keys := make([]any, len(e.relation.key))
	copy(keys, row[0:len(e.relation.key)])
	return keys
}

// keysEqual compares two key slices for equality
func keysEqual(k1, k2 []any) bool {
	if len(k1) != len(k2) {
		return false
	}
	for i := range k1 {
		if k1[i] != k2[i] {
			return false
		}
	}
	return true
}

// diffRows compares two rows and returns the indices of columns that differ
func diffRows(oldRow, newRow []any) []int {
	if len(oldRow) != len(newRow) {
		return nil
	}
	var modified []int
	for i := range oldRow {
		if oldRow[i] != newRow[i] {
			modified = append(modified, i)
		}
	}
	return modified
}

// id can be nil, in which case load from the top or bottom
// refreshing indicates whether to apply diff highlighting logic
func (e *Editor) loadFromRowId(id []any, fromTop bool, focusColumn int, refreshing bool) error {
	debugLog("loadFromRowId: starting, fromTop=%v, focusColumn=%d, id=%v\n", fromTop, focusColumn, id)
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

		// Scan all new rows into temporary storage
		var newRows []Row
		var oldKeyMap map[string]int
		var oldKeyData map[string][]any
		var oldKeyStrings []string
		var matchedOldKeys map[string]bool

		if refreshing {
			// Build a map of old keys to check if new rows existed before
			// Also store old row data for diffing
			oldKeyMap = make(map[string]int)    // key string -> index
			oldKeyData = make(map[string][]any) // key string -> row data
			oldKeyStrings = make([]string, len(e.records))
			for i := 0; i < len(e.records); i++ {
				if e.records[i].data == nil {
					break
				}
				// Clear modified state from previous refresh
				e.records[i].modified = nil
				// Skip already deleted rows
				if e.records[i].state == RowStateDeleted {
					continue
				}
				oldKeys := e.extractKeys(e.records[i].data)
				if oldKeys != nil {
					keyStr := fmt.Sprintf("%v", oldKeys)
					oldKeyStrings[i] = keyStr
					oldKeyMap[keyStr] = i
					oldKeyData[keyStr] = e.records[i].data
				}
			}
			// Track which old keys are still present in new data
			matchedOldKeys = make(map[string]bool)
		}

		for rows.Next() {
			row := make([]any, colCount)
			for j := 0; j < colCount; j++ {
				scanTargets[j] = &row[j]
			}
			if err := rows.Scan(scanTargets...); err != nil {
				return err
			}

			newState := RowStateNormal
			var modified []int

			if refreshing {
				// Determine state by checking if key existed in old buffer
				newKeys := e.extractKeys(row)
				keyStr := fmt.Sprintf("%v", newKeys)

				if _, existedBefore := oldKeyMap[keyStr]; existedBefore {
					// This key existed before - mark it as matched
					matchedOldKeys[keyStr] = true
					// Diff old and new rows to find modified columns
					if oldData := oldKeyData[keyStr]; oldData != nil {
						modified = diffRows(oldData, row)
					}
				} else if len(oldKeyMap) > 0 {
					// This key didn't exist in old buffer (and there were old rows) - it's new
					newState = RowStateNew
				}
				// else: first load (no old rows), keep as Normal
			}

			newRows = append(newRows, Row{state: newState, data: row, modified: modified})
		}

		// Build final buffer: merge new rows with deleted old rows (or just use new rows if not refreshing)
		var finalRecords []Row
		if refreshing {
			finalRecords = make([]Row, 0, len(e.records))
			newRowIdx := 0

			for i := 0; i < len(e.records) && i < len(oldKeyStrings); i++ {
				if e.records[i].data == nil {
					break
				}

				oldKeyStr := oldKeyStrings[i]
				if oldKeyStr == "" || e.records[i].state == RowStateDeleted {
					// Skip empty or already deleted rows
					continue
				}

				if !matchedOldKeys[oldKeyStr] {
					// This old row doesn't exist in new data - mark as deleted
					e.records[i].state = RowStateDeleted
					finalRecords = append(finalRecords, e.records[i])
				} else if newRowIdx < len(newRows) {
					// Insert corresponding new row
					finalRecords = append(finalRecords, newRows[newRowIdx])
					newRowIdx++
				}
			}

			// Append any remaining new rows
			for newRowIdx < len(newRows) {
				finalRecords = append(finalRecords, newRows[newRowIdx])
				newRowIdx++
			}
		} else {
			// Not refreshing, just use new rows directly
			finalRecords = newRows
		}

		// Copy final records back to e.records
		for i := 0; i < len(finalRecords) && i < len(e.records); i++ {
			e.records[i] = finalRecords[i]
			rowsLoaded++
		}

		// If we have more final records than buffer size, truncate
		if len(finalRecords) > len(e.records) {
			rowsLoaded = len(e.records)
		}

		// Mark end with nil if we didn't fill the buffer
		if rowsLoaded < len(e.records) {
			e.records[rowsLoaded] = Row{}
			e.records = e.records[:rowsLoaded+1]
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

		// Scan all new rows into temporary storage
		var newRows []Row
		var oldKeyMap map[string]int
		var oldKeyData map[string][]any
		var oldKeyStrings []string
		var matchedOldKeys map[string]bool

		if refreshing {
			// Build a map of old keys to check if new rows existed before
			// Also store old row data for diffing
			oldKeyMap = make(map[string]int)    // key string -> index
			oldKeyData = make(map[string][]any) // key string -> row data
			oldKeyStrings = make([]string, len(e.records))
			for i := 0; i < len(e.records); i++ {
				if e.records[i].data == nil {
					break
				}
				// Clear modified state from previous refresh
				e.records[i].modified = nil
				// Skip already deleted rows
				if e.records[i].state == RowStateDeleted {
					continue
				}
				oldKeys := e.extractKeys(e.records[i].data)
				if oldKeys != nil {
					keyStr := fmt.Sprintf("%v", oldKeys)
					oldKeyStrings[i] = keyStr
					oldKeyMap[keyStr] = i
					oldKeyData[keyStr] = e.records[i].data
				}
			}
			// Track which old keys are still present in new data
			matchedOldKeys = make(map[string]bool)
		}

		for rows.Next() {
			row := make([]any, colCount)
			for j := 0; j < colCount; j++ {
				scanTargets[j] = &row[j]
			}
			if err := rows.Scan(scanTargets...); err != nil {
				return err
			}

			newState := RowStateNormal
			var modified []int

			if refreshing {
				// Determine state by checking if key existed in old buffer
				newKeys := e.extractKeys(row)
				keyStr := fmt.Sprintf("%v", newKeys)

				if _, existedBefore := oldKeyMap[keyStr]; existedBefore {
					// This key existed before - mark it as matched
					matchedOldKeys[keyStr] = true
					// Diff old and new rows to find modified columns
					if oldData := oldKeyData[keyStr]; oldData != nil {
						modified = diffRows(oldData, row)
					}
				} else if len(oldKeyMap) > 0 {
					// This key didn't exist in old buffer (and there were old rows) - it's new
					newState = RowStateNew
				}
				// else: first load (no old rows), keep as Normal
			}

			newRows = append(newRows, Row{state: newState, data: row, modified: modified})
			if len(newRows) >= len(e.records)-1 {
				break
			}
		}

		// Build final buffer: merge new rows with deleted old rows (or just use new rows if not refreshing)
		var finalRecords []Row
		if refreshing {
			finalRecords = make([]Row, 0, len(e.records))
			newRowIdx := 0

			for i := 0; i < len(e.records) && i < len(oldKeyStrings); i++ {
				if e.records[i].data == nil {
					break
				}

				oldKeyStr := oldKeyStrings[i]
				if oldKeyStr == "" || e.records[i].state == RowStateDeleted {
					// Skip empty or already deleted rows
					continue
				}

				if !matchedOldKeys[oldKeyStr] {
					// This old row doesn't exist in new data - mark as deleted
					e.records[i].state = RowStateDeleted
					finalRecords = append(finalRecords, e.records[i])
				} else if newRowIdx < len(newRows) {
					// Insert corresponding new row
					finalRecords = append(finalRecords, newRows[newRowIdx])
					newRowIdx++
				}
			}

			// Append any remaining new rows
			for newRowIdx < len(newRows) {
				finalRecords = append(finalRecords, newRows[newRowIdx])
				newRowIdx++
			}

			// Reverse the final records for fromBottom
			for i := 0; i < len(finalRecords)/2; i++ {
				j := len(finalRecords) - 1 - i
				finalRecords[i], finalRecords[j] = finalRecords[j], finalRecords[i]
			}
		} else {
			// Not refreshing, just use new rows directly and reverse them
			finalRecords = newRows
			for i := 0; i < len(finalRecords)/2; i++ {
				j := len(finalRecords) - 1 - i
				finalRecords[i], finalRecords[j] = finalRecords[j], finalRecords[i]
			}
		}

		// Copy final records back to e.records
		for i := 0; i < len(finalRecords) && i < len(e.records); i++ {
			e.records[i] = finalRecords[i]
			rowsLoaded++
		}

		// If we have more final records than buffer size, truncate
		if len(finalRecords) > len(e.records) {
			rowsLoaded = len(e.records)
		}

		e.records[len(e.records)-1] = Row{}

		// Mark end with nil if we didn't fill the buffer
		if rowsLoaded < len(e.records) {
			e.records[rowsLoaded] = Row{}
		}
		e.records[len(e.records)-1] = Row{}
		e.pointer = 0
	}
	e.renderData()

	// Focus on the specified column
	if fromTop {
		e.table.Select(e.table.selectedRow, focusColumn)
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
	if len(e.records) == 0 || e.records[e.lastRowIdx()].data == nil {
		return false, nil // No-op, already at end of data
	}

	if e.nextQuery == nil {
		// Stop refresh timer when starting a new query
		e.stopRefreshTimer()

		params := make([]any, len(e.relation.key))
		lastRecordIdx := (e.pointer - 1 + len(e.records)) % len(e.records)
		if e.records[lastRecordIdx].data == nil {
			return false, nil // Can't query from nil record
		}
		for i := range e.relation.key {
			params[i] = e.records[lastRecordIdx].data[i]
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
		e.records[(e.pointer+rowsFetched)%len(e.records)] = Row{state: RowStateNormal, data: row}
	}
	// new pointer position
	e.incrPtr(rowsFetched)
	// If we fetched fewer rows than requested, we've reached the end
	if rowsFetched < i {
		e.incrPtr(1)
		e.records[e.lastRowIdx()] = Row{} // Mark end of data
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
		if len(e.records) == 0 || e.records[e.pointer].data == nil {
			return false, nil // Can't query from nil or empty records
		}
		params := make([]any, len(e.relation.key))
		for i := range e.relation.key {
			params[i] = e.records[e.pointer].data[i]
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
		e.records[e.pointer] = Row{state: RowStateNormal, data: row}
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
		if timer == nil {
			return
		}
		for {
			select {
			case _, ok := <-resetChan:
				if !ok {
					// Channel closed, exit goroutine
					return
				}
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
			case _, ok := <-stopChan:
				if !ok {
					// Channel closed, exit goroutine
					return
				}
			case <-timer.C:
				if app != nil && e.relation != nil && e.relation.DB != nil {
					// Calculate data height before queueing the update
					terminalHeight := getTerminalHeight()
					dataHeight := terminalHeight - 3 // 3 lines for picker bar, status bar, command palette
					app.QueueUpdateDraw(func() {
						// Update table rowsHeight before loading rows from database
						e.table.UpdateRowsHeightFromRect(dataHeight)
						id := e.records[e.pointer].data[:len(e.relation.key)]
						e.loadFromRowId(id, true, e.table.selectedCol, true)
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
		e.records[i].data[col], e.records[i].data[newIdx] = e.records[i].data[newIdx], e.records[i].data[col]
	}

	// Update table headers to reflect column reordering
	headers := make([]HeaderColumn, len(e.columns))
	for i, col := range e.columns {
		headers[i] = HeaderColumn{
			Name:  col.Name,
			Width: col.Width,
		}
	}
	e.table.SetHeaders(headers)

	row, _ := e.table.GetSelection()
	e.table.Select(row, col+direction)
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

// SetStatusErrorWithSentry sets an error status and sends it to Sentry
func (e *Editor) SetStatusErrorWithSentry(err error) {
	if e.statusBar != nil {
		e.statusBar.SetText("[red]ERROR: " + err.Error() + "[white]")
		e.app.Draw()
	}
	CaptureError(err)
}

func (e *Editor) SetStatusLog(message string) {
	if e.statusBar != nil {
		e.statusBar.SetText("[blue]LOG: " + message + "[white]")
		e.app.Draw()
	}
}

// updateStatusWithCellContent displays the full text of the current cell selection in the status bar
// This is only called when not in edit mode
func (e *Editor) updateStatusWithCellContent() {
	// Don't update status when in edit mode, insert mode, or delete mode
	if e.editing || len(e.table.insertRow) > 0 || e.paletteMode == PaletteModeDelete {
		return
	}

	row, col := e.table.GetSelection()

	// Validate bounds
	if row < 0 || row >= len(e.records) || col < 0 || col >= len(e.columns) {
		return
	}

	// Get the cell value
	var cellValue any
	if col < len(e.records[row].data) {
		cellValue = e.records[row].data[col]
	}

	// Format the cell value
	cellText, _ := formatCellValue(cellValue, tcell.StyleDefault)

	// Get column name and type info
	colName := e.columns[col].Name
	var colType string
	if e.relation != nil {
		if attr, ok := e.relation.attributes[colName]; ok {
			colType = attr.Type
		}
	}

	// Build the status message with explicit colors
	var statusMsg string
	if colType != "" {
		statusMsg = fmt.Sprintf("[black]%s[darkgreen] %s", colType, cellText)
	} else {
		statusMsg = fmt.Sprintf("[darkgreen]%s", cellText)
	}

	e.SetStatusMessage(statusMsg)
}

// ensureColumnVisible adjusts the viewport to show the selected column and its borders
func (e *Editor) ensureColumnVisible(col int) {
	if col < 0 || col >= len(e.table.headers) {
		return
	}

	// Get the inner rectangle dimensions of the table
	_, _, width, _ := e.table.GetInnerRect()
	if width <= 0 {
		return
	}

	// Get the column position
	startX, endX := e.table.GetColumnPosition(col)

	// Adjust to include column borders/separators
	// For the first column, include the left table border
	// For other columns, include the separator before the column
	if col == 0 {
		startX = 0 // Include left table border
	} else {
		startX-- // Include separator before the column
	}

	// Always include the separator/border after the column
	endX++

	// Call the viewport's EnsureColumnVisible method
	// The column positions are already relative to the table content area
	e.table.viewport.EnsureColumnVisible(startX, endX, width)
}

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

// Goto execution
// enterDeleteMode enters delete mode for the current row
func (e *Editor) enterDeleteMode(row, col int) {
	if row < 0 || row >= len(e.records) || e.records[row].data == nil {
		return
	}

	// Build DELETE preview
	// Convert records to [][]any for BuildDeletePreview
	recordsData := make([][]any, len(e.records))
	for i := range e.records {
		recordsData[i] = e.records[i].data
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
	if row < 0 || row >= len(e.records) || e.records[row].data == nil {
		e.SetStatusError("Invalid row for deletion")
		return fmt.Errorf("invalid row")
	}

	// Execute the delete
	// Convert records to [][]any for DeleteDBRecord
	recordsData := make([][]any, len(e.records))
	for i := range e.records {
		recordsData[i] = e.records[i].data
	}
	err := e.relation.DeleteDBRecord(recordsData, row)
	if err != nil {
		e.SetStatusErrorWithSentry(err)
		return err
	}

	// Refresh data after deletion
	e.SetStatusMessage("Record deleted successfully")
	e.loadFromRowId(nil, e.records[e.lastRowIdx()].data != nil, col, false)
	return nil
}

func (e *Editor) executeGoto(gotoValue string) {
	if e.relation == nil {
		e.SetStatusError("No database connection available")
		return
	}

	row, col := e.table.GetSelection()
	if row < 0 || row >= len(e.records) || e.records[row].data == nil {
		e.SetStatusError("Invalid current row")
		return
	}

	// Get current row's key values
	currentKeys := make([]any, len(e.relation.key))
	for i := range e.relation.key {
		currentKeys[i] = e.records[row].data[i]
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

	for i := 0; i < len(e.records); i++ {
		if e.records[i].data == nil {
			break
		}
		// Compare key values
		match := true
		for j := range e.relation.key {
			if e.records[i].data[j] != foundKeys[j] {
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
			if err := e.loadFromRowId(foundKeys, false, col, false); err != nil {
				e.SetStatusErrorWithSentry(err)
				return
			}
		} else {
			// Found row wrapped around to before current window, load from top
			if err := e.loadFromRowId(foundKeys, true, col, false); err != nil {
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
	relation, err := NewRelation(e.db, e.dbType, tableName)
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
	e.columns = make([]Column, 0, len(e.relation.attributeOrder))
	for _, name := range e.relation.key {
		e.columns = append(e.columns, Column{Name: name, Width: 4})
	}
	for _, name := range e.relation.attributeOrder {
		if !slices.Contains(e.relation.key, name) {
			e.columns = append(e.columns, Column{Name: name, Width: 8})
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
	e.table.SetHeaders(headers).SetKeyColumnCount(len(e.relation.key)).SetTableName(tableName)

	// Reload data from the beginning
	fmt.Fprintf(os.Stderr, "[DEBUG] Loading data from beginning\n")
	e.pointer = 0
	e.records = make([]Row, e.table.rowsHeight)
	e.loadFromRowId(nil, true, 0, false)
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
