package main

import (
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

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
	data    [][]interface{}

	// Display configuration
	cellPadding   int
	borderColor   tcell.Color
	headerColor   tcell.Color
	headerBgColor tcell.Color
	separatorChar rune
	bottom        bool

	// Selection state
	selectedRow int
	selectedCol int
	selectable  bool

	// Viewport information
	BodyRowsAvailable int
}

// NewTableView creates a new table view component
func NewTableView() *TableView {
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

// SetData sets the table data
func (tv *TableView) SetData(data [][]interface{}) *TableView {
	tv.data = make([][]interface{}, len(data))
	for i, row := range data {
		tv.data[i] = make([]interface{}, len(row))
		copy(tv.data[i], row)
	}
	return tv
}

// GetSelection returns the currently selected data row and column
func (tv *TableView) GetSelection() (row, col int) {
	return tv.selectedRow, tv.selectedCol
}

// Select selects a data cell
func (tv *TableView) Select(row, col int) *TableView {
	if row >= 0 && row < len(tv.data) && col >= 0 && col < len(tv.headers) {
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

// GetCell returns the value at the specified data coordinates
func (tv *TableView) GetCell(row, col int) interface{} {
	if row >= 0 && row < len(tv.data) && col >= 0 && col < len(tv.data[row]) {
		return tv.data[row][col]
	}
	return nil
}

// SetCell sets the value at the specified data coordinates
func (tv *TableView) SetCell(row, col int, value interface{}) *TableView {
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
		tv.drawHeaderRow(screen, x, currentY, tableWidth)
		currentY++
	}

	// Draw header separator
	if currentY < y+height {
		tv.drawHeaderSeparator(screen, x, currentY, tableWidth)
		currentY++
	}

	// Check if we should draw the bottom border (when final slice is nil)
	drawBottomBorder := tv.bottom
	if len(tv.data) > 0 && tv.data[len(tv.data)-1] == nil {
		drawBottomBorder = true
	}

	// Draw data rows
	dataRowsDrawn := 0
	maxDataRows := height - 3 // Reserve space for top border and header
	if drawBottomBorder {
		maxDataRows = height - 4 // Reserve additional space for bottom border
	}
	tv.BodyRowsAvailable = maxDataRows
	for i := 0; i < len(tv.data) && dataRowsDrawn < maxDataRows && currentY < y+height; i++ {
		if tv.data[i] == nil {
			break // Stop drawing when we hit a nil slice
		}
		tv.drawDataRow(screen, x, currentY, tableWidth, i)
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
			screen.SetContent(pos, y, '┬', nil, tcell.StyleDefault.Foreground(tv.borderColor))
			pos++
		} else {
			screen.SetContent(pos, y, '┐', nil, tcell.StyleDefault.Foreground(tv.borderColor))
		}
	}
}

// drawHeaderRow draws the header content row
func (tv *TableView) drawHeaderRow(screen tcell.Screen, x, y, tableWidth int) {
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
			screen.SetContent(pos+j, y, ch, nil, tcell.StyleDefault.Foreground(tv.headerColor).Background(tv.headerBgColor))
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
			screen.SetContent(pos, y, '┿', nil, tcell.StyleDefault.Foreground(tv.borderColor))
			pos++
		} else {
			screen.SetContent(pos, y, '┥', nil, tcell.StyleDefault.Foreground(tv.borderColor))
		}
	}
}

// drawDataRow draws a data row
func (tv *TableView) drawDataRow(screen tcell.Screen, x, y, tableWidth, rowIdx int) {
	// Left border
	screen.SetContent(x, y, '│', nil, tcell.StyleDefault.Foreground(tv.borderColor))
	pos := x + 1

	// Data cells
	for i, header := range tv.headers {
		// Cell style (highlight if selected)
		cellStyle := tcell.StyleDefault
		if tv.selectable && rowIdx == tv.selectedRow && i == tv.selectedCol {
			cellStyle = cellStyle.Background(tcell.ColorBlue).Foreground(tcell.ColorWhite)
		}

		// Padding before content
		for j := 0; j < tv.cellPadding; j++ {
			screen.SetContent(pos+j, y, ' ', nil, cellStyle)
		}
		pos += tv.cellPadding

		// Cell data
		var cellText string
		if rowIdx < len(tv.data) && i < len(tv.data[rowIdx]) {
			cellText = formatCellValue(tv.data[rowIdx][i])
		}
		cellText = padCellToWidth(cellText, header.Width)

		for j, ch := range cellText {
			screen.SetContent(pos+j, y, ch, nil, cellStyle)
		}
		pos += header.Width

		// Padding after content
		for j := 0; j < tv.cellPadding; j++ {
			screen.SetContent(pos+j, y, ' ', nil, cellStyle)
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
			screen.SetContent(pos, y, '┴', nil, tcell.StyleDefault.Foreground(tv.borderColor))
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
			if tv.selectedRow > 0 {
				tv.selectedRow--
			}
		case tcell.KeyDown:
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

// UpdateCell updates a cell value and refreshes the display
func (tv *TableView) UpdateCell(row, col int, value interface{}) *TableView {
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
