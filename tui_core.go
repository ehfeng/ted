package main

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"ted/internal/dblib"
)

const (
	DefaultColumnWidth   = 8
	RowsTimerInterval    = 100 * time.Millisecond
	RefreshTimerInterval = 300 * time.Millisecond
	pagePicker           = "picker"
	pageTable            = "table"
	chromeHeight         = 6
)

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

// relation.attributes stores the database attributes
// lookup key is unique: it's always selected
// if a multicolumn reference is selected, all columns in the reference are selected
type Editor struct {
	app      *tview.Application
	pages    *tview.Pages
	table    *TableView
	relation *dblib.Relation
	config   *Config
	vimMode  bool

	// Database connection (stored separately to support table switching when relation is nil)
	db     *sql.DB
	dbType dblib.DatabaseType

	// references to key components
	tablePicker    *FuzzySelector // Table picker at the top
	statusBar      *tview.TextView
	commandPalette *tview.InputField
	layout         *tview.Flex

	paletteMode         PaletteMode
	kittySequenceActive bool
	kittySequenceBuffer string
	lastGPress          time.Time // For detecting 'gg' in vim mode

	// selection
	editing bool

	// data, records is a circular buffer
	query      *sql.Rows  // unified query for scrolling
	scrollDown bool       // direction of current query: true = next/forward, false = prev/backward
	queryMu    sync.Mutex // protects query and scrollDown from concurrent access
	pointer    int        // pointer to the current record
	buffer     []Row      // circular buffer of rows

	// change tracking for refresh
	previousRows []Row // snapshot of rows from last refresh

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
	PaletteModeFind
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
	case PaletteModeFind:
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

func runEditor(config *Config, dbname, tablename, sqlStatement string) error {
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

	var relation *dblib.Relation
	var headers []dblib.DisplayColumn
	var displayName string

	// Load custom SQL if provided
	if sqlStatement != "" {
		var err error
		relation, err = dblib.NewRelationFromSQL(db, dbType, sqlStatement)
		if err != nil {
			CaptureError(err)
			return err
		}

		// Check if relation is keyable (has keys)
		if len(relation.Key) == 0 {
			return fmt.Errorf("custom SQL has no keyable columns and cannot be viewed")
		}

		displayName = sqlStatement

		// Build headers in database schema order
		headers = make([]dblib.DisplayColumn, 0, len(relation.Columns))
		for i, col := range relation.Columns {
			isKey := false
			for _, keyIdx := range relation.Key {
				if keyIdx == i {
					isKey = true
					break
				}
			}
			editable := relation.IsColumnEditable(i)
			headers = append(headers, dblib.DisplayColumn{Name: col.Name, Width: DefaultColumnWidth, IsKey: isKey, Editable: editable})
		}
	} else if tablename != "" {
		// Load table/view if tablename is provided
		var err error
		relation, err = dblib.NewRelation(db, dbType, tablename)
		if err != nil {
			CaptureError(err)
			return err
		}

		// Check if relation is keyable (has keys)
		if len(relation.Key) == 0 {
			return fmt.Errorf("relation %s has no keyable columns and cannot be viewed", tablename)
		}

		displayName = tablename

		// Build headers in database schema order
		headers = make([]dblib.DisplayColumn, 0, len(relation.Columns))
		for i, col := range relation.Columns {
			isKey := false
			for _, keyIdx := range relation.Key {
				if keyIdx == i {
					isKey = true
					break
				}
			}
			editable := relation.IsColumnEditable(i)
			headers = append(headers, dblib.DisplayColumn{Name: col.Name, Width: DefaultColumnWidth, IsKey: isKey, Editable: editable})
		}
	} else {
		// No table or SQL specified - create empty state
		headers = []dblib.DisplayColumn{}
		displayName = ""
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
		relation:    relation,
		config:      config,
		vimMode:     config.VimMode,
		db:          db,
		dbType:      dbType,
		tablePicker: nil, // Will be initialized after editor is created
		paletteMode: PaletteModeDefault,
		pointer:     0,
		buffer:      make([]Row, tableDataHeight),
	}

	// Create selector with callback now that editor exists
	editor.tablePicker = NewFuzzySelector(tables, tablename, editor.selectTableFromPicker, func() {
		// Close callback: hide picker and return focus to table
		editor.pages.HidePage(pagePicker)
		editor.app.SetFocus(editor.table)
		editor.app.SetAfterDrawFunc(nil) // Clear cursor style function
		editor.setCursorStyle(0)         // Reset to default cursor style
	})

	editor.table = NewTableView(tableDataHeight+1, &TableViewConfig{
		Headers: headers,
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

	editor.table.SetTableName(displayName).SetVimMode(editor.vimMode)

	// Only load data if we have a relation
	if displayName != "" {
		editor.loadFromRowId(nil, true, 0)
	}
	editor.setupKeyBindings()
	editor.setupStatusBar()
	editor.setupCommandPalette()

	// Setup layout without the selector (it will be overlaid when visible)
	editor.layout = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(editor.table, 0, 1, true).
		AddItem(editor.statusBar, 1, 0, false).
		AddItem(editor.commandPalette, 1, 0, false)

	// TODO: unclear if this even works
	// // Setup resize handler
	// editor.pages.SetChangedFunc(func() {
	// 	// Don't handle resize when in edit mode to avoid deadlock
	// 	if editor.editing {
	// 		return
	// 	}

	// 	// Get the new terminal height
	// 	newHeight := getTerminalHeight()
	// 	newDataHeight := newHeight - chromeHeight // 3 lines for picker bar, status bar, command palette

	// 	// Only resize if the height has changed significantly
	// 	if newDataHeight != editor.table.rowsHeight && newDataHeight > 0 {
	// 		if newDataHeight > len(editor.buffer) && editor.buffer[len(editor.buffer)-1].data == nil {
	// 			// the table is smaller than the new data height, no need to fetch more rows
	// 			return
	// 		}
	// 		// Create new records buffer with the new size
	// 		newRecords := make([]Row, newDataHeight)

	// 		// Copy existing data to the new buffer
	// 		copyCount := min(len(editor.buffer), newDataHeight)
	// 		for i := 0; i < copyCount; i++ {
	// 			ptr := (i + editor.pointer) % len(editor.buffer)
	// 			newRecords[i] = editor.buffer[ptr]
	// 		}

	// 		// If the new buffer is larger, fetch more rows to fill it
	// 		if newDataHeight > len(editor.buffer) && editor.buffer[len(editor.buffer)-1].data != nil {
	// 			// We need to fetch more rows
	// 			oldLen := len(editor.buffer)
	// 			editor.buffer = newRecords
	// 			editor.pointer = 0
	// 			// Fetch additional rows
	// 			go func() {
	// 				editor.app.QueueUpdateDraw(func() { editor.nextRows(newDataHeight - oldLen) })
	// 			}()
	// 		} else {
	// 			editor.buffer = newRecords
	// 			editor.pointer = 0
	// 		}

	// 		editor.renderData()
	// 		go editor.app.Draw()
	// 	}
	// })

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
		// Ctrl+F sends ACK (6) or 'f' depending on terminal
		case (rune == 'f' || rune == 6) && mod&tcell.ModCtrl != 0:
			e.setPaletteMode(PaletteModeFind, true)
			return nil
		// case (rune == 'p' || rune == 16) && mod&tcell.ModCtrl != 0:
		// 	e.setPaletteMode(PaletteModeCommand, true)
		// 	return nil
		case (rune == '`' || rune == 0) && mod&tcell.ModCtrl != 0:
			e.setPaletteMode(PaletteModeSQL, true)
			return nil
		case (rune == 'q' || rune == 17) && mod&tcell.ModCtrl != 0:
			// Ctrl+Q: quit application
			e.app.Stop()
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
			case PaletteModeFind:
				e.executeFind(command)
			case PaletteModeDelete:
				e.executeDelete()
			}

			// For Find mode, keep the palette open with text selected
			if mode == PaletteModeFind {
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
