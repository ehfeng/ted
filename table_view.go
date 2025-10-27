package main

import (
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const EmptyCellValue = '\000'

// HeaderColumn represents a table column with header information
type HeaderColumn struct {
	Name  string
	Width int
}

// TableView is a custom table component with proper header separator rendering
type TableView struct {
	*tview.Box

	// Table data
	headers []HeaderColumn
	data    [][]any

	// Display configuration
	cellPadding    int
	borderColor    tcell.Color
	headerColor    tcell.Color
	headerBgColor  tcell.Color
	separatorChar  rune
	keyColumnCount int // Number of key columns (for special separator rendering)
	bottom         bool
	insertRow      []any // if non-empty, render as insert mode row with special styling

	// Selection state
	selectedRow int
	selectedCol int
	selectable  bool

	// Callbacks
	doubleClickFunc func(row, col int)
	singleClickFunc func(row, col int)

	// Double-click tracking
	lastClickRow int
	lastClickCol int

	// Viewport information
	rowsHeight int
}

// NewTableView creates a new table view component
func NewTableView(height int) *TableView {
	tv := &TableView{
		Box:           tview.NewBox(),
		cellPadding:   1,
		borderColor:   tcell.ColorWhite,
		headerColor:   tcell.ColorWhite,
		headerBgColor: tcell.ColorDarkSlateGray,
		separatorChar: '│',
		selectedRow:   0,
		selectedCol:   0,
		selectable:    true,
		lastClickRow:  -1,
		lastClickCol:  -1,
		rowsHeight:    height,
	}

	tv.SetBorder(false) // We'll draw our own borders
	return tv
}

// SetHeaders sets the table headers
func (tv *TableView) SetHeaders(headers []HeaderColumn) *TableView {
	tv.headers = make([]HeaderColumn, len(headers))
	copy(tv.headers, headers)
	return tv
}

// SetKeyColumnCount sets the number of key columns (for special separator rendering)
func (tv *TableView) SetKeyColumnCount(count int) *TableView {
	tv.keyColumnCount = count
	return tv
}

// SetData sets the table data
func (tv *TableView) SetData(data [][]any) *TableView {
	tv.data = make([][]any, len(data))
	for i, row := range data {
		tv.data[i] = make([]any, len(row))
		copy(tv.data[i], row)
	}
	return tv
}

func (tv *TableView) SetupInsertRow() {
	tv.insertRow = make([]any, len(tv.headers))
	for i := range tv.insertRow {
		tv.insertRow[i] = EmptyCellValue
	}
}

func (tv *TableView) ClearInsertRow() {
	tv.insertRow = nil
}

// GetSelection returns the currently selected data row and column
func (tv *TableView) GetSelection() (row, col int) {
	return tv.selectedRow, tv.selectedCol
}

// GetDataLength returns the length of the data array
func (tv *TableView) GetDataLength() int {
	return len(tv.data)
}

// Select selects a data cell
func (tv *TableView) Select(row, col int) *TableView {
	maxRow := len(tv.data)
	if len(tv.insertRow) > 0 {
		maxRow = len(tv.data) + 1 // Allow selecting the virtual insert mode row
	}
	if row >= 0 && row < maxRow && col >= 0 && col < len(tv.headers) {
		tv.selectedRow = row
		tv.selectedCol = col
	}
	return tv
}

// SetSelectable sets whether the table is selectable
func (tv *TableView) SetSelectable(selectable bool) *TableView {
	tv.selectable = selectable
	return tv
}

// SetDoubleClickFunc sets the function to call when a cell is double-clicked
func (tv *TableView) SetDoubleClickFunc(handler func(row, col int)) *TableView {
	tv.doubleClickFunc = handler
	return tv
}

// SetSingleClickFunc sets the function to call when a cell is single-clicked
func (tv *TableView) SetSingleClickFunc(handler func(row, col int)) *TableView {
	tv.singleClickFunc = handler
	return tv
}

// GetCell returns the value at the specified data coordinates
func (tv *TableView) GetCell(row, col int) any {
	if row >= 0 && row < len(tv.data) && col >= 0 && col < len(tv.data[row]) {
		return tv.data[row][col]
	}
	return nil
}

// SetCell sets the value at the specified data coordinates
func (tv *TableView) SetCell(row, col int, value any) *TableView {
	if row >= 0 && row < len(tv.data) && col >= 0 && col < len(tv.data[row]) {
		tv.data[row][col] = value
	}
	return tv
}

// Draw renders the table view
func (tv *TableView) Draw(screen tcell.Screen) {
	tv.Box.DrawForSubclass(screen, tv)
	x, y, width, height := tv.GetInnerRect()

	if len(tv.headers) == 0 || width <= 0 || height <= 0 {
		return
	}

	// Calculate table dimensions
	tableWidth := tv.calculateTableWidth()
	if tableWidth > width {
		tableWidth = width
	}

	// Draw top border
	tv.drawTopBorder(screen, x, y, tableWidth)
	currentY := y + 1

	// Draw header row
	if currentY < y+height {
		tv.drawHeaderRow(screen, x, currentY)
		currentY++
	}

	// Draw header separator
	if currentY < y+height {
		tv.drawHeaderSeparator(screen, x, currentY, tableWidth)
		currentY++
	}

	// Check if we should draw the bottom border (when final slice is nil or in insert mode)
	drawBottomBorder := tv.bottom
	if len(tv.data) > 0 && len(tv.data[len(tv.data)-1]) == 0 {
		drawBottomBorder = true
	}
	if len(tv.insertRow) > 0 {
		drawBottomBorder = true
	}

	// Draw data rows
	dataRowsDrawn := 0
	maxDataRows := height - 3 // Reserve space for top border and header
	if drawBottomBorder {
		maxDataRows = height - 4 // Reserve additional space for bottom border
	}

	// When in insert mode, we need to reserve one more row for the insert mode row
	maxRegularDataRows := maxDataRows
	if len(tv.insertRow) > 0 {
		maxRegularDataRows = maxDataRows - 1
	}
	tv.rowsHeight = maxDataRows

	// Draw regular data rows
	for i := 0; i < len(tv.data) && dataRowsDrawn < maxRegularDataRows && currentY < y+height; i++ {
		if len(tv.data[i]) == 0 {
			break // Stop drawing when we hit a nil slice
		}
		tv.drawDataRow(screen, x, currentY, tableWidth, i)
		currentY++
		dataRowsDrawn++
	}

	// Draw insert mode row if enabled and there's space
	if len(tv.insertRow) > 0 && currentY < y+height {
		tv.drawDataRow(screen, x, currentY, tableWidth, len(tv.data))
		currentY++
		dataRowsDrawn++
	}

	// Draw bottom border (if enabled or if final slice is nil)
	if drawBottomBorder && currentY < y+height {
		tv.drawBottomBorder(screen, x, currentY, tableWidth)
	}
}

// calculateTableWidth calculates the total width needed for the table
func (tv *TableView) calculateTableWidth() int {
	width := 1 // Left border
	for i, header := range tv.headers {
		width += header.Width + 2*tv.cellPadding // Cell content + padding
		if i < len(tv.headers)-1 {
			width += 1 // Column separator
		}
	}
	width += 1 // Right border
	return width
}

// drawTopBorder draws the top border of the table
func (tv *TableView) drawTopBorder(screen tcell.Screen, x, y, tableWidth int) {
	// Left corner
	screen.SetContent(x, y, '┌', nil, tcell.StyleDefault.Foreground(tv.borderColor))
	pos := x + 1

	// Column sections
	for i, header := range tv.headers {
		cellWidth := header.Width + 2*tv.cellPadding

		// Horizontal line for this column
		for j := 0; j < cellWidth; j++ {
			screen.SetContent(pos+j, y, '─', nil, tcell.StyleDefault.Foreground(tv.borderColor))
		}
		pos += cellWidth

		// Junction or corner
		if i < len(tv.headers)-1 {
			// Use special junction after last key column
			junction := '┬'
			screen.SetContent(pos, y, junction, nil, tcell.StyleDefault.Foreground(tv.borderColor))
			pos++
		} else {
			screen.SetContent(pos, y, '┐', nil, tcell.StyleDefault.Foreground(tv.borderColor))
		}
	}
}

// drawHeaderRow draws the header content row
func (tv *TableView) drawHeaderRow(screen tcell.Screen, x, y int) {
	// Left border
	screen.SetContent(x, y, '│', nil, tcell.StyleDefault.Foreground(tv.borderColor))
	pos := x + 1

	// Header cells
	for i, header := range tv.headers {
		// Padding before content
		for j := 0; j < tv.cellPadding; j++ {
			screen.SetContent(pos+j, y, ' ', nil, tcell.StyleDefault.Foreground(tv.headerColor).Background(tv.headerBgColor))
		}
		pos += tv.cellPadding

		// Header text
		headerText := padCellToWidth(header.Name, header.Width)
		for j, ch := range headerText {
			screen.SetContent(pos+j, y, ch, nil, tcell.StyleDefault.Bold(true).Foreground(tv.headerColor).Background(tv.headerBgColor))
		}
		pos += header.Width

		// Padding after content
		for j := 0; j < tv.cellPadding; j++ {
			screen.SetContent(pos+j, y, ' ', nil, tcell.StyleDefault.Foreground(tv.headerColor).Background(tv.headerBgColor))
		}
		pos += tv.cellPadding

		// Column separator
		if i < len(tv.headers)-1 {
			screen.SetContent(pos, y, '│', nil, tcell.StyleDefault.Foreground(tv.borderColor))
			pos++
		}
	}

	// Right border
	screen.SetContent(pos, y, '│', nil, tcell.StyleDefault.Foreground(tv.borderColor))
}

// drawHeaderSeparator draws the heavy line separator between header and data
func (tv *TableView) drawHeaderSeparator(screen tcell.Screen, x, y, tableWidth int) {
	// Left junction
	screen.SetContent(x, y, '┝', nil, tcell.StyleDefault.Foreground(tv.borderColor))
	pos := x + 1

	// Column sections
	for i, header := range tv.headers {
		cellWidth := header.Width + 2*tv.cellPadding

		// Heavy horizontal line for this column
		for j := 0; j < cellWidth; j++ {
			screen.SetContent(pos+j, y, '━', nil, tcell.StyleDefault.Foreground(tv.borderColor))
		}
		pos += cellWidth

		// Junction or right junction
		if i < len(tv.headers)-1 {
			// Use special junction after last key column
			junction := '┿'
			if tv.keyColumnCount > 0 && i == tv.keyColumnCount-1 {
				junction = '╈' // Heavy cross junction
			}
			screen.SetContent(pos, y, junction, nil, tcell.StyleDefault.Foreground(tv.borderColor))
			pos++
		} else {
			screen.SetContent(pos, y, '┥', nil, tcell.StyleDefault.Foreground(tv.borderColor))
		}
	}
}

// drawDataRow draws a data row
func (tv *TableView) drawDataRow(screen tcell.Screen, x, y, tableWidth, rowIdx int) {
	// Check if this is the insert mode row (when newRecordRow is set and rowIdx is beyond data)
	isNewRecordRow := len(tv.insertRow) > 0 && rowIdx == len(tv.data)

	// Left border
	borderStyle := tcell.StyleDefault.Foreground(tv.borderColor)
	if isNewRecordRow {
		borderStyle = tcell.StyleDefault.Background(tcell.ColorBlack).Foreground(tcell.ColorWhite)
	}
	screen.SetContent(x, y, '│', nil, borderStyle)
	pos := x + 1

	// Data cells
	for i, header := range tv.headers {
		// Base style - use cyan background for insert mode row
		baseCellStyle := tcell.StyleDefault
		if isNewRecordRow {
			baseCellStyle = baseCellStyle.Background(tcell.ColorRoyalBlue)
		}

		// Apply selection highlight on top of base style
		cellStyle := baseCellStyle
		if tv.selectable && rowIdx == tv.selectedRow && i == tv.selectedCol {
			cellStyle = cellStyle.Background(tcell.ColorBlue).Foreground(tcell.ColorWhite)
		}

		// Padding before content
		for j := 0; j < tv.cellPadding; j++ {
			screen.SetContent(pos+j, y, ' ', nil, cellStyle)
		}
		pos += tv.cellPadding

		// Cell data
		if isNewRecordRow {
			// For insert mode row, render cell value with special styling
			// EmptyCellValue means empty (column not included in INSERT) - show as ·
			// nil means null
			if tv.insertRow[i] == EmptyCellValue {
				// Empty cell in insert mode - show repeating dots
				for k := 0; k < header.Width; k++ {
					screen.SetContent(pos+k, y, '·', nil, cellStyle)
				}
			} else {
				cellText, cellStyle := formatCellValue(tv.insertRow[i], cellStyle)
				cellText = padCellToWidth(cellText, header.Width)
				for j, ch := range cellText {
					screen.SetContent(pos+j, y, ch, nil, cellStyle)
				}
			}

		} else {
			// Normal rendering
			if rowIdx < len(tv.data) && i < len(tv.data[rowIdx]) {
				cellText, cellStyle := formatCellValue(tv.data[rowIdx][i], cellStyle)
				cellText = padCellToWidth(cellText, header.Width)
				for j, ch := range cellText {
					screen.SetContent(pos+j, y, ch, nil, cellStyle)
				}
			}
		}
		pos += header.Width

		// Padding after content
		for j := 0; j < tv.cellPadding; j++ {
			screen.SetContent(pos+j, y, ' ', nil, cellStyle)
		}
		pos += tv.cellPadding

		// Column separator
		if i < len(tv.headers)-1 {
			sepStyle := tcell.StyleDefault.Foreground(tv.borderColor)
			if isNewRecordRow {
				sepStyle = baseCellStyle
			}
			// Use thicker separator after last key column
			separator := '│'
			if tv.keyColumnCount > 0 && i == tv.keyColumnCount-1 {
				separator = '┃' // Heavy vertical line
			}
			screen.SetContent(pos, y, separator, nil, sepStyle)
			pos++
		}
	}

	// Right border
	rightBorderStyle := tcell.StyleDefault.Foreground(tv.borderColor)
	if isNewRecordRow {
		rightBorderStyle = tcell.StyleDefault.Background(tcell.ColorBlack).Foreground(tcell.ColorWhite)
	}
	screen.SetContent(pos, y, '│', nil, rightBorderStyle)
}

// drawBottomBorder draws the bottom border of the table
func (tv *TableView) drawBottomBorder(screen tcell.Screen, x, y, tableWidth int) {
	// Left corner
	screen.SetContent(x, y, '└', nil, tcell.StyleDefault.Foreground(tv.borderColor))
	pos := x + 1

	// Column sections
	for i, header := range tv.headers {
		cellWidth := header.Width + 2*tv.cellPadding

		// Horizontal line for this column
		for j := 0; j < cellWidth; j++ {
			screen.SetContent(pos+j, y, '─', nil, tcell.StyleDefault.Foreground(tv.borderColor))
		}
		pos += cellWidth

		// Junction or corner
		if i < len(tv.headers)-1 {
			// Use special junction after last key column
			junction := '┴'
			if tv.keyColumnCount > 0 && i == tv.keyColumnCount-1 {
				junction = '┸' // Heavy vertical junction
			}
			screen.SetContent(pos, y, junction, nil, tcell.StyleDefault.Foreground(tv.borderColor))
			pos++
		} else {
			screen.SetContent(pos, y, '┘', nil, tcell.StyleDefault.Foreground(tv.borderColor))
		}
	}
}

// InputHandler handles keyboard input for navigation and selection
func (tv *TableView) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return tv.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		if !tv.selectable {
			return
		}

		key := event.Key()

		switch key {
		case tcell.KeyUp:
			if len(tv.insertRow) > 0 {
				return // Disable vertical navigation in insert mode
			}
			if tv.selectedRow > 0 {
				tv.selectedRow--
			}
		case tcell.KeyDown:
			if len(tv.insertRow) > 0 {
				return // Disable vertical navigation in insert mode
			}
			if tv.selectedRow < len(tv.data)-1 {
				tv.selectedRow++
			}
		case tcell.KeyLeft:
			if tv.selectedCol > 0 {
				tv.selectedCol--
			}
		case tcell.KeyRight:
			if tv.selectedCol < len(tv.headers)-1 {
				tv.selectedCol++
			}
		case tcell.KeyHome:
			tv.selectedCol = 0
		case tcell.KeyEnd:
			tv.selectedCol = len(tv.headers) - 1
		}
	})
}

// MouseHandler handles mouse events for the table
func (tv *TableView) MouseHandler() func(action tview.MouseAction, event *tcell.EventMouse, setFocus func(p tview.Primitive)) (consumed bool, capture tview.Primitive) {
	return tv.WrapMouseHandler(func(action tview.MouseAction, event *tcell.EventMouse, setFocus func(p tview.Primitive)) (consumed bool, capture tview.Primitive) {
		// Get mouse position
		x, y := event.Position()

		// Check if click is within our bounds
		if !tv.InRect(x, y) {
			return false, nil
		}

		switch action {
		case tview.MouseLeftDown:
			// Set focus when clicked
			setFocus(tv)
			consumed = true
			if tv.selectable {
				// Convert screen coordinates to cell coordinates
				row, col := tv.GetCellAtPosition(x, y)
				if row >= 0 && col >= 0 {
					tv.selectedRow = row
					tv.selectedCol = col
					// tv.lastClickRow = row
					// tv.lastClickCol = col
					consumed = true
				}
			}
		case tview.MouseLeftClick:
			row, col := tv.GetCellAtPosition(x, y)
			if tv.singleClickFunc != nil && row >= 0 && col >= 0 {
				tv.singleClickFunc(row, col)
			}
			tv.lastClickRow = row
			tv.lastClickCol = col
			consumed = true
			return consumed, nil
		case tview.MouseLeftDoubleClick:
			// Handle double-click on a cell - only if both clicks are on the same cell
			if tv.doubleClickFunc != nil {
				row, col := tv.GetCellAtPosition(x, y)
				if row >= 0 && col >= 0 && row == tv.lastClickRow && col == tv.lastClickCol {
					tv.doubleClickFunc(row, col)
					// reset double click tracking
					tv.lastClickRow = -1
					tv.lastClickCol = -1
					consumed = true
				}
			}
		}

		return consumed, nil
	})
}

// UpdateCell updates a cell value and refreshes the display
func (tv *TableView) UpdateCell(row, col int, value any) *TableView {
	if row >= 0 && row < len(tv.data) && col >= 0 && col < len(tv.data[row]) {
		tv.data[row][col] = value
	}
	return tv
}

// GetColumnWidth returns the width of a column
func (tv *TableView) GetColumnWidth(col int) int {
	if col >= 0 && col < len(tv.headers) {
		return tv.headers[col].Width
	}
	return 0
}

// SetColumnWidth updates a column width
func (tv *TableView) SetColumnWidth(col int, width int) *TableView {
	if col >= 0 && col < len(tv.headers) {
		tv.headers[col].Width = max(3, width) // Minimum width of 3
	}
	return tv
}

// GetCellAtPosition returns the data row and column indices for screen coordinates
// Returns (-1, -1) if the position is not within a data cell
func (tv *TableView) GetCellAtPosition(screenX, screenY int) (row, col int) {
	x, y, width, height := tv.GetInnerRect()

	// Check if click is within the table bounds
	if screenX < x || screenX >= x+width || screenY < y || screenY >= y+height {
		return -1, -1
	}

	// Calculate which row was clicked
	// Row 0: top border
	// Row 1: header
	// Row 2: header separator
	// Row 3+: data rows
	relativeY := screenY - y
	if relativeY < 3 {
		return -1, -1 // Clicked on border/header, not a data cell
	}

	dataRow := relativeY - 3
	if dataRow < 0 || dataRow >= len(tv.data) || tv.data[dataRow] == nil {
		return -1, -1 // Beyond available data
	}

	// Calculate which column was clicked
	relativeX := screenX - x
	if relativeX < 1 {
		return -1, -1 // Clicked on left border
	}

	// Walk through columns to find which one contains the click
	currentX := 1 // Start after left border
	for i, header := range tv.headers {
		cellWidth := header.Width + 2*tv.cellPadding

		// Check if click is within this column's content area
		if relativeX >= currentX && relativeX < currentX+cellWidth {
			return dataRow, i
		}

		currentX += cellWidth

		// Add separator width if not the last column
		if i < len(tv.headers)-1 {
			if relativeX == currentX {
				return -1, -1 // Clicked on separator
			}
			currentX += 1
		}
	}

	return -1, -1 // Clicked beyond table content
}

// padCellToWidth pads text to a specific width, truncating if too long
func padCellToWidth(text string, width int) string {
	if len(text) >= width {
		if width >= 3 {
			return text[:width-1] + "…"
		}
		return text[:width]
	}
	// Pad with spaces to reach desired width
	spaces := ""
	for i := 0; i < width-len(text); i++ {
		spaces += " "
	}
	return text + spaces
}
