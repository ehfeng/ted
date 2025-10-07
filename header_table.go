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

// HeaderTable is a custom table component with proper header separator rendering
type HeaderTable struct {
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

	// Selection state
	selectedRow int
	selectedCol int
	selectable  bool
}

// NewHeaderTable creates a new header table component
func NewHeaderTable() *HeaderTable {
	ht := &HeaderTable{
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

	ht.SetBorder(false) // We'll draw our own borders
	return ht
}

// SetHeaders sets the table headers
func (ht *HeaderTable) SetHeaders(headers []HeaderColumn) *HeaderTable {
	ht.headers = make([]HeaderColumn, len(headers))
	copy(ht.headers, headers)
	return ht
}

// SetData sets the table data
func (ht *HeaderTable) SetData(data [][]interface{}) *HeaderTable {
	ht.data = make([][]interface{}, len(data))
	for i, row := range data {
		ht.data[i] = make([]interface{}, len(row))
		copy(ht.data[i], row)
	}
	return ht
}

// GetSelection returns the currently selected data row and column
func (ht *HeaderTable) GetSelection() (row, col int) {
	return ht.selectedRow, ht.selectedCol
}

// Select selects a data cell
func (ht *HeaderTable) Select(row, col int) *HeaderTable {
	if row >= 0 && row < len(ht.data) && col >= 0 && col < len(ht.headers) {
		ht.selectedRow = row
		ht.selectedCol = col
	}
	return ht
}

// SetSelectable sets whether the table is selectable
func (ht *HeaderTable) SetSelectable(selectable bool) *HeaderTable {
	ht.selectable = selectable
	return ht
}

// GetCell returns the value at the specified data coordinates
func (ht *HeaderTable) GetCell(row, col int) interface{} {
	if row >= 0 && row < len(ht.data) && col >= 0 && col < len(ht.data[row]) {
		return ht.data[row][col]
	}
	return nil
}

// SetCell sets the value at the specified data coordinates
func (ht *HeaderTable) SetCell(row, col int, value interface{}) *HeaderTable {
	if row >= 0 && row < len(ht.data) && col >= 0 && col < len(ht.data[row]) {
		ht.data[row][col] = value
	}
	return ht
}

// Draw renders the header table
func (ht *HeaderTable) Draw(screen tcell.Screen) {
	ht.Box.DrawForSubclass(screen, ht)
	x, y, width, height := ht.GetInnerRect()

	if len(ht.headers) == 0 || width <= 0 || height <= 0 {
		return
	}

	// Calculate table dimensions
	tableWidth := ht.calculateTableWidth()
	if tableWidth > width {
		tableWidth = width
	}

	// Draw top border
	ht.drawTopBorder(screen, x, y, tableWidth)
	currentY := y + 1

	// Draw header row
	if currentY < y+height {
		ht.drawHeaderRow(screen, x, currentY, tableWidth)
		currentY++
	}

	// Draw header separator
	if currentY < y+height {
		ht.drawHeaderSeparator(screen, x, currentY, tableWidth)
		currentY++
	}

	// Draw data rows
	dataRowsDrawn := 0
	maxDataRows := height - 4 // Reserve space for borders and header
	for i := 0; i < len(ht.data) && dataRowsDrawn < maxDataRows && currentY < y+height; i++ {
		ht.drawDataRow(screen, x, currentY, tableWidth, i)
		currentY++
		dataRowsDrawn++
	}

	// Draw bottom border
	if currentY < y+height {
		ht.drawBottomBorder(screen, x, currentY, tableWidth)
	}
}

// calculateTableWidth calculates the total width needed for the table
func (ht *HeaderTable) calculateTableWidth() int {
	width := 1 // Left border
	for i, header := range ht.headers {
		width += header.Width + 2*ht.cellPadding // Cell content + padding
		if i < len(ht.headers)-1 {
			width += 1 // Column separator
		}
	}
	width += 1 // Right border
	return width
}

// drawTopBorder draws the top border of the table
func (ht *HeaderTable) drawTopBorder(screen tcell.Screen, x, y, tableWidth int) {
	// Left corner
	screen.SetContent(x, y, '┌', nil, tcell.StyleDefault.Foreground(ht.borderColor))
	pos := x + 1

	// Column sections
	for i, header := range ht.headers {
		cellWidth := header.Width + 2*ht.cellPadding

		// Horizontal line for this column
		for j := 0; j < cellWidth; j++ {
			screen.SetContent(pos+j, y, '─', nil, tcell.StyleDefault.Foreground(ht.borderColor))
		}
		pos += cellWidth

		// Junction or corner
		if i < len(ht.headers)-1 {
			screen.SetContent(pos, y, '┬', nil, tcell.StyleDefault.Foreground(ht.borderColor))
			pos++
		} else {
			screen.SetContent(pos, y, '┐', nil, tcell.StyleDefault.Foreground(ht.borderColor))
		}
	}
}

// drawHeaderRow draws the header content row
func (ht *HeaderTable) drawHeaderRow(screen tcell.Screen, x, y, tableWidth int) {
	// Left border
	screen.SetContent(x, y, '│', nil, tcell.StyleDefault.Foreground(ht.borderColor))
	pos := x + 1

	// Header cells
	for i, header := range ht.headers {
		// Padding before content
		for j := 0; j < ht.cellPadding; j++ {
			screen.SetContent(pos+j, y, ' ', nil, tcell.StyleDefault.Foreground(ht.headerColor).Background(ht.headerBgColor))
		}
		pos += ht.cellPadding

		// Header text
		headerText := padCellToWidth(header.Name, header.Width)
		for j, ch := range headerText {
			screen.SetContent(pos+j, y, ch, nil, tcell.StyleDefault.Foreground(ht.headerColor).Background(ht.headerBgColor))
		}
		pos += header.Width

		// Padding after content
		for j := 0; j < ht.cellPadding; j++ {
			screen.SetContent(pos+j, y, ' ', nil, tcell.StyleDefault.Foreground(ht.headerColor).Background(ht.headerBgColor))
		}
		pos += ht.cellPadding

		// Column separator
		if i < len(ht.headers)-1 {
			screen.SetContent(pos, y, '│', nil, tcell.StyleDefault.Foreground(ht.borderColor))
			pos++
		}
	}

	// Right border
	screen.SetContent(pos, y, '│', nil, tcell.StyleDefault.Foreground(ht.borderColor))
}

// drawHeaderSeparator draws the heavy line separator between header and data
func (ht *HeaderTable) drawHeaderSeparator(screen tcell.Screen, x, y, tableWidth int) {
	// Left junction
	screen.SetContent(x, y, '┝', nil, tcell.StyleDefault.Foreground(ht.borderColor))
	pos := x + 1

	// Column sections
	for i, header := range ht.headers {
		cellWidth := header.Width + 2*ht.cellPadding

		// Heavy horizontal line for this column
		for j := 0; j < cellWidth; j++ {
			screen.SetContent(pos+j, y, '━', nil, tcell.StyleDefault.Foreground(ht.borderColor))
		}
		pos += cellWidth

		// Junction or right junction
		if i < len(ht.headers)-1 {
			screen.SetContent(pos, y, '┿', nil, tcell.StyleDefault.Foreground(ht.borderColor))
			pos++
		} else {
			screen.SetContent(pos, y, '┥', nil, tcell.StyleDefault.Foreground(ht.borderColor))
		}
	}
}

// drawDataRow draws a data row
func (ht *HeaderTable) drawDataRow(screen tcell.Screen, x, y, tableWidth, rowIdx int) {
	// Left border
	screen.SetContent(x, y, '│', nil, tcell.StyleDefault.Foreground(ht.borderColor))
	pos := x + 1

	// Data cells
	for i, header := range ht.headers {
		// Cell style (highlight if selected)
		cellStyle := tcell.StyleDefault
		if ht.selectable && rowIdx == ht.selectedRow && i == ht.selectedCol {
			cellStyle = cellStyle.Background(tcell.ColorBlue).Foreground(tcell.ColorWhite)
		}

		// Padding before content
		for j := 0; j < ht.cellPadding; j++ {
			screen.SetContent(pos+j, y, ' ', nil, cellStyle)
		}
		pos += ht.cellPadding

		// Cell data
		var cellText string
		if rowIdx < len(ht.data) && i < len(ht.data[rowIdx]) {
			cellText = formatCellValue(ht.data[rowIdx][i])
		}
		cellText = padCellToWidth(cellText, header.Width)

		for j, ch := range cellText {
			screen.SetContent(pos+j, y, ch, nil, cellStyle)
		}
		pos += header.Width

		// Padding after content
		for j := 0; j < ht.cellPadding; j++ {
			screen.SetContent(pos+j, y, ' ', nil, cellStyle)
		}
		pos += ht.cellPadding

		// Column separator
		if i < len(ht.headers)-1 {
			screen.SetContent(pos, y, '│', nil, tcell.StyleDefault.Foreground(ht.borderColor))
			pos++
		}
	}

	// Right border
	screen.SetContent(pos, y, '│', nil, tcell.StyleDefault.Foreground(ht.borderColor))
}

// drawBottomBorder draws the bottom border of the table
func (ht *HeaderTable) drawBottomBorder(screen tcell.Screen, x, y, tableWidth int) {
	// Left corner
	screen.SetContent(x, y, '└', nil, tcell.StyleDefault.Foreground(ht.borderColor))
	pos := x + 1

	// Column sections
	for i, header := range ht.headers {
		cellWidth := header.Width + 2*ht.cellPadding

		// Horizontal line for this column
		for j := 0; j < cellWidth; j++ {
			screen.SetContent(pos+j, y, '─', nil, tcell.StyleDefault.Foreground(ht.borderColor))
		}
		pos += cellWidth

		// Junction or corner
		if i < len(ht.headers)-1 {
			screen.SetContent(pos, y, '┴', nil, tcell.StyleDefault.Foreground(ht.borderColor))
			pos++
		} else {
			screen.SetContent(pos, y, '┘', nil, tcell.StyleDefault.Foreground(ht.borderColor))
		}
	}
}

// InputHandler handles keyboard input for navigation and selection
func (ht *HeaderTable) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return ht.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		if !ht.selectable {
			return
		}

		key := event.Key()

		switch key {
		case tcell.KeyUp:
			if ht.selectedRow > 0 {
				ht.selectedRow--
			}
		case tcell.KeyDown:
			if ht.selectedRow < len(ht.data)-1 {
				ht.selectedRow++
			}
		case tcell.KeyLeft:
			if ht.selectedCol > 0 {
				ht.selectedCol--
			}
		case tcell.KeyRight:
			if ht.selectedCol < len(ht.headers)-1 {
				ht.selectedCol++
			}
		case tcell.KeyHome:
			ht.selectedCol = 0
		case tcell.KeyEnd:
			ht.selectedCol = len(ht.headers) - 1
		}
	})
}

// UpdateCell updates a cell value and refreshes the display
func (ht *HeaderTable) UpdateCell(row, col int, value interface{}) *HeaderTable {
	if row >= 0 && row < len(ht.data) && col >= 0 && col < len(ht.data[row]) {
		ht.data[row][col] = value
	}
	return ht
}

// GetColumnWidth returns the width of a column
func (ht *HeaderTable) GetColumnWidth(col int) int {
	if col >= 0 && col < len(ht.headers) {
		return ht.headers[col].Width
	}
	return 0
}

// SetColumnWidth updates a column width
func (ht *HeaderTable) SetColumnWidth(col int, width int) *HeaderTable {
	if col >= 0 && col < len(ht.headers) {
		ht.headers[col].Width = max(3, width) // Minimum width of 3
	}
	return ht
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
