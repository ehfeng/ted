package main

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type Editor struct {
	app         *tview.Application
	table       *tview.Table
	data        *Table
	db          *sql.DB
	dbType      DatabaseType
	config      *Config
	tableSpec   string
	currentRow  int
	currentCol  int
	editMode    bool
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
		table:        tview.NewTable(),
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

	if err := editor.app.SetRoot(editor.table, true).EnableMouse(true).Run(); err != nil {
		return err
	}

	return nil
}

func (e *Editor) setupTable() {
	e.table.SetBorder(true)
	e.table.SetTitle(fmt.Sprintf(" %s ", e.data.Name))

	for i, col := range e.data.Columns {
		cell := tview.NewTableCell(col.Name).
			SetAlign(tview.AlignLeft).
			SetBackgroundColor(tcell.ColorDarkSlateGray).
			SetTextColor(tcell.ColorWhite).
			SetSelectable(false)
		e.table.SetCell(0, i, cell)
	}

	for rowIdx, row := range e.data.Rows {
		for colIdx, value := range row {
			cellText := formatCellValue(value)
			cell := tview.NewTableCell(cellText).
				SetAlign(tview.AlignLeft).
				SetMaxWidth(e.data.Columns[colIdx].Width)
			e.table.SetCell(rowIdx+1, colIdx, cell)
		}
	}

	e.table.SetFixed(1, 0)
	e.table.SetSelectable(true, true)
	e.table.Select(1, 0)
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
			newRow := max(1, row-10)
			e.table.Select(newRow, col)
			return nil
		case key == tcell.KeyPgDn:
			newRow := min(len(e.data.Rows), row+10)
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
			e.table.Select(1, col)
			return nil
		case key == tcell.KeyDown && mod&tcell.ModMeta != 0:
			e.table.Select(len(e.data.Rows), col)
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
		} else if row > 1 {
			e.table.Select(row-1, len(e.data.Columns)-1)
		}
	} else {
		if col < len(e.data.Columns)-1 {
			e.table.Select(row, col+1)
		} else if row < len(e.data.Rows) {
			e.table.Select(row+1, 0)
		}
	}
}

func (e *Editor) enterEditMode(row, col int) {
	if row == 0 {
		return
	}
	
	cell := e.table.GetCell(row, col)
	currentText := cell.Text

	inputField := tview.NewInputField().
		SetText(currentText).
		SetFieldWidth(0)

	inputField.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			newText := inputField.GetText()
			e.updateCell(row, col, newText)
		} else if key == tcell.KeyEscape {
			e.app.SetRoot(e.table, true)
		}
	})

	e.app.SetRoot(inputField, true)
	e.editMode = true
}

func (e *Editor) exitEditMode() {
	if e.editMode {
		e.app.SetRoot(e.table, true)
		e.editMode = false
	}
}

func (e *Editor) updateCell(row, col int, newValue string) {
	dataRowIdx := row - 1
	if dataRowIdx >= 0 && dataRowIdx < len(e.data.Rows) {
		e.data.Rows[dataRowIdx][col] = newValue
		
		cell := e.table.GetCell(row, col)
		cell.SetText(formatCellValue(newValue))
	}
	
	e.app.SetRoot(e.table, true)
	e.editMode = false
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
	for col := 0; col < len(e.data.Columns); col++ {
		cell := e.table.GetCell(row, col)
		cell.SetBackgroundColor(tcell.ColorDarkBlue)
	}
}

func (e *Editor) unhighlightRow(row int) {
	for col := 0; col < len(e.data.Columns); col++ {
		cell := e.table.GetCell(row, col)
		cell.SetBackgroundColor(tcell.ColorDefault)
	}
}

func (e *Editor) highlightColumn(col int) {
	for row := 1; row <= len(e.data.Rows); row++ {
		cell := e.table.GetCell(row, col)
		cell.SetBackgroundColor(tcell.ColorDarkGreen)
	}
}

func (e *Editor) unhighlightColumn(col int) {
	for row := 1; row <= len(e.data.Rows); row++ {
		cell := e.table.GetCell(row, col)
		cell.SetBackgroundColor(tcell.ColorDefault)
	}
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
	
	for row := 0; row <= len(e.data.Rows); row++ {
		cell := e.table.GetCell(row, col)
		cell.SetMaxWidth(newWidth)
	}
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