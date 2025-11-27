package main

import (
	"fmt"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"ted/internal/dblib"
)

// Viewport handles horizontal scrolling for the table
type Viewport struct {
	scrollX     int          // Current horizontal offset
	screen      tcell.Screen // Reference to the tcell screen
	tableWidth  int          // Total width of the table content
	screenWidth int          // Width of the visible area
}

// NewViewport creates a new viewport
func NewViewport() *Viewport {
	return &Viewport{
		scrollX: 0,
	}
}

// SetScreen sets the screen reference for the viewport
func (v *Viewport) SetScreen(screen tcell.Screen) {
	v.screen = screen
}

// SetDimensions sets the table and screen dimensions for scroll limiting
func (v *Viewport) SetDimensions(tableWidth, screenWidth int) {
	v.tableWidth = tableWidth
	v.screenWidth = screenWidth
	// Clamp current scroll position if needed
	if v.tableWidth > v.screenWidth {
		maxScroll := v.tableWidth - v.screenWidth
		if v.scrollX > maxScroll {
			v.scrollX = maxScroll
		}
	} else {
		v.scrollX = 0
	}
}

// SetContent calls screen.SetContent with x adjusted by scrollX
func (v *Viewport) SetContent(x, y int, ch rune, combc []rune, style tcell.Style) {
	if v.screen != nil {
		v.screen.SetContent(x-v.scrollX, y, ch, combc, style)
	}
}

// ScrollLeft scrolls the viewport left by one unit
func (v *Viewport) ScrollLeft() {
	if v.scrollX > 0 {
		v.scrollX--
	}
}

// ScrollRight scrolls the viewport right by one unit
func (v *Viewport) ScrollRight() {
	// Only scroll if there's more content to reveal
	if v.tableWidth > v.screenWidth {
		maxScroll := v.tableWidth - v.screenWidth
		if v.scrollX < maxScroll {
			v.scrollX++
		}
	}
}

// GetScrollX returns the current horizontal offset
func (v *Viewport) GetScrollX() int {
	return v.scrollX
}

// SetScrollX sets the horizontal offset directly
func (v *Viewport) SetScrollX(offset int) {
	if offset < 0 {
		v.scrollX = 0
	} else {
		v.scrollX = offset
	}
}

// EnsureColumnVisible adjusts scrollX so that a column range is visible
// startX is the left edge of the column, endX is the right edge
func (v *Viewport) EnsureColumnVisible(startX, endX, screenWidth int) {
	// endX is exclusive (one past the last character of the column)
	// We need the column to fit within the visible area: [scrollX, scrollX + screenWidth)

	if endX-startX >= screenWidth {
		// Column is wider than screen, just show from the start of the column
		v.scrollX = startX
		return
	}

	if startX < v.scrollX {
		// Column starts before visible area - scroll left
		v.scrollX = startX
	} else if endX > v.scrollX+screenWidth {
		// Column ends after visible area - scroll right
		v.scrollX = endX - screenWidth
	}

	// Clamp scrollX to valid range
	if v.scrollX < 0 {
		v.scrollX = 0
	}
}

// TableView is a custom table component with proper header separator rendering
type TableView struct {
	*tview.Box

	// Table data
	headers   []dblib.DisplayColumn
	data      []Row
	tableName string // Name of the current table to display in header

	// Display configuration
	cellPadding   int
	borderColor   tcell.Color
	headerColor   tcell.Color
	headerBgColor tcell.Color
	separatorChar rune
	bottom        bool
	insertRow     []any // if non-empty, render as insert mode row with special styling

	// Selection state
	selectedRow int
	selectedCol int
	selectable  bool
	deleteMode  bool // When true, selected row has red background
	findMode    bool // When true, selected column has dark blue background
	vimMode     bool // When true, display "vim mode" indicator

	// Callbacks
	doubleClickFunc     func(row, col int)
	singleClickFunc     func(row, col int)
	selectionChangeFunc func(row, col int)
	mouseScrollFunc     func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse)
	tableNameClickFunc  func()

	// Double-click tracking
	lastClickRow int
	lastClickCol int

	// Drag state for column resizing
	resizingColumn   int // -1 if not resizing, otherwise column index
	resizeStartX     int // Initial X position of mouse when drag started
	resizeStartWidth int // Original column width before drag

	// Viewport for horizontal scrolling
	viewport *Viewport

	// Viewport information
	rowsHeight int
}

// TableViewConfig holds configuration for creating a TableView
type TableViewConfig struct {
	Headers            []dblib.DisplayColumn
	DoubleClickFunc    func(row, col int)
	SingleClickFunc    func(row, col int)
	MouseScrollFunc    func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse)
	TableNameClickFunc func()
}

// NewTableView creates a new table view component with the given configuration
func NewTableView(height int, config *TableViewConfig) *TableView {
	tv := &TableView{
		Box:            tview.NewBox(),
		cellPadding:    1,
		borderColor:    tcell.ColorWhite,
		headerColor:    tcell.ColorWhite,
		headerBgColor:  tcell.ColorDarkSlateGray,
		separatorChar:  '│',
		selectedRow:    0,
		selectedCol:    0,
		selectable:     true,
		lastClickRow:   -1,
		lastClickCol:   -1,
		resizingColumn: -1,
		viewport:       NewViewport(),
		rowsHeight:     height,
	}

	tv.SetBorder(false) // We'll draw our own borders

	// Apply configuration if provided
	if config != nil {
		if len(config.Headers) > 0 {
			tv.SetHeaders(config.Headers)
		}
		if config.DoubleClickFunc != nil {
			tv.SetDoubleClickFunc(config.DoubleClickFunc)
		}
		if config.SingleClickFunc != nil {
			tv.SetSingleClickFunc(config.SingleClickFunc)
		}
		if config.MouseScrollFunc != nil {
			tv.SetMouseScrollFunc(config.MouseScrollFunc)
		}
		if config.TableNameClickFunc != nil {
			tv.SetTableNameClickFunc(config.TableNameClickFunc)
		}
	}

	return tv
}

// SetHeaders sets the table headers
func (tv *TableView) SetHeaders(headers []dblib.DisplayColumn) *TableView {
	tv.headers = make([]dblib.DisplayColumn, len(headers))
	copy(tv.headers, headers)
	return tv
}

// SetTableName sets the table name to display in the header row
func (tv *TableView) SetTableName(name string) *TableView {
	tv.tableName = name
	return tv
}

// SetVimMode enables or disables vim mode indicator
func (tv *TableView) SetVimMode(enabled bool) *TableView {
	tv.vimMode = enabled
	return tv
}

// SetData sets the table data
func (tv *TableView) SetData(data [][]any) *TableView {
	tv.data = make([]Row, len(data))
	for i, row := range data {
		tv.data[i].data = make([]any, len(row))
		copy(tv.data[i].data, row)
		tv.data[i].state = RowStateNormal
	}
	return tv
}

func (tv *TableView) SetDataWithStates(data [][]any, states []RowState) *TableView {
	tv.data = make([]Row, len(data))
	for i, row := range data {
		tv.data[i].data = make([]any, len(row))
		copy(tv.data[i].data, row)
		if i < len(states) {
			tv.data[i].state = states[i]
		} else {
			tv.data[i].state = RowStateNormal
		}
	}
	return tv
}

// SetDataReferences sets table data using references instead of deep copies.
// This is more efficient when the data is already owned by the caller and won't be modified.
// The data slices are stored as references to avoid unnecessary allocations.
func (tv *TableView) SetDataReferences(data []Row) *TableView {
	tv.data = data
	return tv
}

func (tv *TableView) SetupInsertRow() {
	tv.insertRow = make([]any, len(tv.headers))
	for i := range tv.insertRow {
		tv.insertRow[i] = dblib.EmptyCellValue
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
		// Only trigger callback if selection actually changed
		selectionChanged := (tv.selectedRow != row || tv.selectedCol != col)
		tv.selectedRow = row
		tv.selectedCol = col
		if selectionChanged && tv.selectionChangeFunc != nil {
			tv.selectionChangeFunc(row, col)
		}
	}
	return tv
}

// SetSelectable sets whether the table is selectable
func (tv *TableView) SetSelectable(selectable bool) *TableView {
	tv.selectable = selectable
	return tv
}

// SetDeleteMode sets whether the table is in delete mode (selected row will have red background)
func (tv *TableView) SetDeleteMode(deleteMode bool) *TableView {
	tv.deleteMode = deleteMode
	return tv
}

// SetFindMode sets whether the table is in find mode (selected column will have dark blue background)
func (tv *TableView) SetFindMode(findMode bool) *TableView {
	tv.findMode = findMode
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

// SetSelectionChangeFunc sets the function to call when the selection changes
func (tv *TableView) SetSelectionChangeFunc(handler func(row, col int)) *TableView {
	tv.selectionChangeFunc = handler
	return tv
}

// SetMouseScrollFunc sets the function to handle mouse scroll events
func (tv *TableView) SetMouseScrollFunc(handler func(action tview.MouseAction, event *tcell.EventMouse) (tview.MouseAction, *tcell.EventMouse)) *TableView {
	tv.mouseScrollFunc = handler
	return tv
}

// SetTableNameClickFunc sets the function to call when the table name is clicked
func (tv *TableView) SetTableNameClickFunc(handler func()) *TableView {
	tv.tableNameClickFunc = handler
	return tv
}

// GetCell returns the value at the specified data coordinates
func (tv *TableView) GetCell(row, col int) any {
	if row >= 0 && row < len(tv.data) && col >= 0 && col < len(tv.data[row].data) {
		return tv.data[row].data[col]
	}
	return nil
}

// SetCell sets the value at the specified data coordinates
func (tv *TableView) SetCell(row, col int, value any) *TableView {
	if row >= 0 && row < len(tv.data) && col >= 0 && col < len(tv.data[row].data) {
		tv.data[row].data[col] = value
	}
	return tv
}

// UpdateRowsHeightFromRect calculates and updates rowsHeight based on the provided height.
// This should be called before loading rows from the database to ensure the correct number
// of rows are fetched based on the current display height. This method accepts the height
// as a parameter to avoid calling GetRect() from the UI event loop which can cause deadlocks.
func (tv *TableView) UpdateRowsHeightFromRect(height int) {
	// Calculate maxDataRows based on the provided height
	// Reserve space for top border, header row, and header separator
	maxDataRows := height - 3

	// Check if we should draw the bottom border (when final slice is nil or in insert mode)
	drawBottomBorder := tv.bottom
	if len(tv.data) > 0 && len(tv.data[len(tv.data)-1].data) == 0 {
		drawBottomBorder = true
	}
	if len(tv.insertRow) > 0 {
		drawBottomBorder = true
	}

	// Reserve additional space for bottom border if needed
	if drawBottomBorder {
		maxDataRows--
	}

	// Update the rowsHeight field
	tv.rowsHeight = maxDataRows
}

// Draw renders the table view
func (tv *TableView) Draw(screen tcell.Screen) {
	tv.Box.DrawForSubclass(screen, tv)
	x, y, width, height := tv.GetInnerRect()

	if len(tv.headers) == 0 || width <= 0 || height <= 0 {
		return
	}

	// Initialize viewport with the screen
	tv.viewport.SetScreen(screen)

	// Calculate table dimensions
	tableWidth := tv.calculateTableWidth()
	displayWidth := tableWidth
	if displayWidth > width {
		displayWidth = width
	}

	// Set viewport dimensions for scroll limiting
	tv.viewport.SetDimensions(tableWidth, width)

	currentY := y

	// Draw table name header if table name is set
	if tv.tableName != "" {
		tv.drawTableNameHeader(x, currentY, tableWidth)
		currentY++
	}

	// Draw top border
	tv.drawTopBorder(x, currentY, tableWidth)
	currentY++

	// Draw header row
	if currentY < y+height {
		tv.drawHeaderRow(x, currentY)
		currentY++
	}

	// Draw header separator
	if currentY < y+height {
		tv.drawHeaderSeparator(x, currentY, tableWidth)
		currentY++
	}

	// Check if we should draw the bottom border (when final slice is nil or in insert mode)
	drawBottomBorder := tv.bottom
	if len(tv.data) > 0 && len(tv.data[len(tv.data)-1].data) == 0 {
		drawBottomBorder = true
	}
	if len(tv.insertRow) > 0 {
		drawBottomBorder = true
	}

	// Draw data rows
	dataRowsDrawn := 0
	maxDataRows := height - 3 // Reserve space for top border and header
	if drawBottomBorder {
		maxDataRows = maxDataRows - 1
	}

	// When in insert mode, we need to reserve one more row for the insert mode row
	maxRegularDataRows := maxDataRows
	if len(tv.insertRow) > 0 {
		maxRegularDataRows = maxDataRows - 1
	}

	// Draw regular data rows
	for i := 0; i < len(tv.data) && dataRowsDrawn < maxRegularDataRows && currentY < y+height; i++ {
		if tv.data[i].data == nil || len(tv.data[i].data) == 0 {
			break // Stop drawing when we hit a nil slice
		}
		tv.drawDataRow(x, currentY, tableWidth, i)
		currentY++
		dataRowsDrawn++
	}

	// Draw insert mode row if enabled and there's space
	if len(tv.insertRow) > 0 && currentY < y+height {
		tv.drawDataRow(x, currentY, tableWidth, len(tv.data))
		currentY++
		dataRowsDrawn++
	}

	// Draw bottom border (if enabled or if final slice is nil)
	if drawBottomBorder && currentY < y+height {
		tv.drawBottomBorder(x, currentY, tableWidth)
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
// drawTableNameHeader draws the table name header at the top of the table
func (tv *TableView) drawTableNameHeader(x, y, tableWidth int) {
	if tv.tableName == "" {
		return
	}

	// Format: " TableName ▾"
	headerText := fmt.Sprintf(" %s ▾", tv.tableName)
	style := tcell.StyleDefault.Foreground(tcell.ColorWhite)

	// Vim mode indicator to display on the right
	vimModeText := ""
	if tv.vimMode {
		vimModeText = "vim mode "
	}

	// Draw the header text left-aligned
	pos := x
	for _, ch := range headerText {
		tv.viewport.SetContent(pos, y, ch, nil, style)
		pos++
	}

	// Calculate where vim mode text should start (right-aligned)
	vimModeStartPos := x + tableWidth - len(vimModeText)

	// Fill the middle with spaces, stopping before vim mode text
	endPos := x + tableWidth
	if tv.vimMode {
		endPos = vimModeStartPos
	}
	for pos < endPos {
		tv.viewport.SetContent(pos, y, ' ', nil, style)
		pos++
	}

	// Draw vim mode text right-aligned
	if tv.vimMode {
		for _, ch := range vimModeText {
			tv.viewport.SetContent(pos, y, ch, nil, style)
			pos++
		}
	}
}

func (tv *TableView) drawTopBorder(x, y, tableWidth int) {
	// Left corner
	tv.viewport.SetContent(x, y, '┌', nil, tcell.StyleDefault.Foreground(tv.borderColor))
	pos := x + 1

	// Column sections
	for i, header := range tv.headers {
		cellWidth := header.Width + 2*tv.cellPadding

		// Horizontal line for this column
		for j := 0; j < cellWidth; j++ {
			tv.viewport.SetContent(pos+j, y, '─', nil, tcell.StyleDefault.Foreground(tv.borderColor))
		}
		pos += cellWidth

		// Junction or corner
		if i < len(tv.headers)-1 {
			// Use special junction after last key column
			junction := '┬'
			tv.viewport.SetContent(pos, y, junction, nil, tcell.StyleDefault.Foreground(tv.borderColor))
			pos++
		} else {
			tv.viewport.SetContent(pos, y, '┐', nil, tcell.StyleDefault.Foreground(tv.borderColor))
		}
	}
}

// drawHeaderRow draws the header content row
func (tv *TableView) drawHeaderRow(x, y int) {
	// Left border
	tv.viewport.SetContent(x, y, '│', nil, tcell.StyleDefault.Foreground(tv.borderColor))
	pos := x + 1

	// Header cells
	for i, header := range tv.headers {
		// Padding before content
		for j := 0; j < tv.cellPadding; j++ {
			if header.IsKey {
				tv.viewport.SetContent(pos+j, y, '✦', nil, tcell.StyleDefault.Foreground(tv.headerColor).Background(tv.headerBgColor))
			} else {
				tv.viewport.SetContent(pos+j, y, ' ', nil, tcell.StyleDefault.Foreground(tv.headerColor).Background(tv.headerBgColor))
			}
		}
		pos += tv.cellPadding

		// Header text
		headerText := padCellToWidth(header.Name, header.Width)
		for j, ch := range headerText {
			tv.viewport.SetContent(pos+j, y, ch, nil, tcell.StyleDefault.Bold(true).Foreground(tv.headerColor).Background(tv.headerBgColor))
		}
		pos += header.Width

		// Padding after content
		for j := 0; j < tv.cellPadding; j++ {
			tv.viewport.SetContent(pos+j, y, ' ', nil, tcell.StyleDefault.Foreground(tv.headerColor).Background(tv.headerBgColor))
		}
		pos += tv.cellPadding

		// Column separator
		if i < len(tv.headers)-1 {
			tv.viewport.SetContent(pos, y, '│', nil, tcell.StyleDefault.Foreground(tv.borderColor))
			pos++
		}
	}

	// Right border
	tv.viewport.SetContent(pos, y, '│', nil, tcell.StyleDefault.Foreground(tv.borderColor))
}

// drawHeaderSeparator draws the heavy line separator between header and data
func (tv *TableView) drawHeaderSeparator(x, y, tableWidth int) {
	// Left junction
	tv.viewport.SetContent(x, y, '┝', nil, tcell.StyleDefault.Foreground(tv.borderColor))
	pos := x + 1

	// Column sections
	for i, header := range tv.headers {
		cellWidth := header.Width + 2*tv.cellPadding

		// Heavy horizontal line for this column
		for j := 0; j < cellWidth; j++ {
			tv.viewport.SetContent(pos+j, y, '━', nil, tcell.StyleDefault.Foreground(tv.borderColor))
		}
		pos += cellWidth

		// Junction or right junction
		if i < len(tv.headers)-1 {
			tv.viewport.SetContent(pos, y, '┿', nil, tcell.StyleDefault.Foreground(tv.borderColor))
			pos++
		} else {
			tv.viewport.SetContent(pos, y, '┥', nil, tcell.StyleDefault.Foreground(tv.borderColor))
		}
	}
}

// drawDataRow draws a data row
func (tv *TableView) drawDataRow(x, y, tableWidth, rowIdx int) {
	// Check if this is the insert mode row (when newRecordRow is set and rowIdx is beyond data)
	isNewRecordRow := len(tv.insertRow) > 0 && rowIdx == len(tv.data)

	// Get the row state for background color
	var rowState RowState
	if rowIdx < len(tv.data) {
		rowState = tv.data[rowIdx].state
	}

	// Left border
	borderStyle := tcell.StyleDefault.Foreground(tv.borderColor)
	isSelectedRowInDeleteMode := tv.selectable && tv.deleteMode && rowIdx == tv.selectedRow
	if isNewRecordRow {
		borderStyle = tcell.StyleDefault.Background(tcell.ColorBlack).Foreground(tcell.ColorWhite)
	} else if isSelectedRowInDeleteMode {
		borderStyle = tcell.StyleDefault.Background(tcell.ColorRed).Foreground(tcell.ColorWhite)
	} else if rowState == RowStateNew {
		borderStyle = borderStyle.Background(tcell.ColorDarkGreen)
	} else if rowState == RowStateDeleted {
		borderStyle = borderStyle.Background(tcell.ColorDarkRed)
	}
	tv.viewport.SetContent(x, y, '│', nil, borderStyle)
	pos := x + 1

	// Data cells
	for i, header := range tv.headers {
		// Check if this specific cell is modified
		isCellModified := false
		if rowIdx < len(tv.data) && tv.data[rowIdx].modified != nil {
			for _, modIdx := range tv.data[rowIdx].modified {
				if modIdx == i {
					isCellModified = true
					break
				}
			}
		}

		// Base style - use cyan background for insert mode row
		baseCellStyle := tcell.StyleDefault
		if isNewRecordRow {
			baseCellStyle = baseCellStyle.Background(tcell.ColorRoyalBlue)
		} else if rowState == RowStateNew {
			baseCellStyle = baseCellStyle.Background(tcell.ColorDarkGreen)
		} else if rowState == RowStateDeleted {
			baseCellStyle = baseCellStyle.Background(tcell.ColorDarkRed)
		} else if isCellModified {
			// Modified cell - use dark green background
			baseCellStyle = baseCellStyle.Background(tcell.ColorDarkGreen)
		}

		// Apply selection highlight on top of base style
		cellStyle := baseCellStyle

		// In Find mode, highlight the entire selected column
		if tv.findMode && i == tv.selectedCol {
			cellStyle = cellStyle.Background(tcell.ColorDarkBlue).Foreground(tcell.ColorWhite)
		}

		if tv.selectable && rowIdx == tv.selectedRow {
			if tv.deleteMode {
				// In delete mode, make the entire selected row red
				cellStyle = cellStyle.Background(tcell.ColorRed).Foreground(tcell.ColorWhite)
			} else if i == tv.selectedCol {
				// In normal mode, only highlight the selected cell
				cellStyle = cellStyle.Background(tcell.ColorBlue).Foreground(tcell.ColorWhite)
			}
		}

		// Padding before content
		for j := 0; j < tv.cellPadding; j++ {
			tv.viewport.SetContent(pos+j, y, ' ', nil, cellStyle)
		}
		pos += tv.cellPadding

		// Cell data
		if isNewRecordRow {
			// For insert mode row, render cell value with special styling
			// dblib.EmptyCellValue means empty (column not included in INSERT) - show as ·
			// nil means null
			if tv.insertRow[i] == dblib.EmptyCellValue {
				// Empty cell in insert mode - show repeating dots
				for k := 0; k < header.Width; k++ {
					tv.viewport.SetContent(pos+k, y, '·', nil, cellStyle)
				}
			} else {
				cellText, cellStyle := formatCellValue(tv.insertRow[i], cellStyle)
				cellText = padCellToWidth(cellText, header.Width)
				for j, ch := range cellText {
					tv.viewport.SetContent(pos+j, y, ch, nil, cellStyle)
				}
			}

		} else {
			// Normal rendering
			if rowIdx < len(tv.data) && i < len(tv.data[rowIdx].data) {
				cellText, cellStyle := formatCellValue(tv.data[rowIdx].data[i], cellStyle)
				cellText = padCellToWidth(cellText, header.Width)
				for j, ch := range cellText {
					tv.viewport.SetContent(pos+j, y, ch, nil, cellStyle)
				}
			}
		}
		pos += header.Width

		// Padding after content
		for j := 0; j < tv.cellPadding; j++ {
			tv.viewport.SetContent(pos+j, y, ' ', nil, cellStyle)
		}
		pos += tv.cellPadding

		// Column separator
		if i < len(tv.headers)-1 {
			sepStyle := tcell.StyleDefault.Foreground(tv.borderColor)
			if isNewRecordRow {
				sepStyle = baseCellStyle
			} else if isSelectedRowInDeleteMode {
				sepStyle = cellStyle
			} else if rowState == RowStateNew {
				sepStyle = sepStyle.Background(tcell.ColorDarkGreen)
			} else if rowState == RowStateDeleted {
				sepStyle = sepStyle.Background(tcell.ColorDarkRed)
			}
			tv.viewport.SetContent(pos, y, '│', nil, sepStyle)
			pos++
		}
	}

	// Right border
	rightBorderStyle := tcell.StyleDefault.Foreground(tv.borderColor)
	if isNewRecordRow {
		rightBorderStyle = tcell.StyleDefault.Background(tcell.ColorBlack).Foreground(tcell.ColorWhite)
	} else if isSelectedRowInDeleteMode {
		rightBorderStyle = tcell.StyleDefault.Background(tcell.ColorRed).Foreground(tcell.ColorWhite)
	} else if rowState == RowStateNew {
		rightBorderStyle = rightBorderStyle.Background(tcell.ColorDarkGreen)
	} else if rowState == RowStateDeleted {
		rightBorderStyle = rightBorderStyle.Background(tcell.ColorDarkRed)
	}
	tv.viewport.SetContent(pos, y, '│', nil, rightBorderStyle)
}

// drawBottomBorder draws the bottom border of the table
func (tv *TableView) drawBottomBorder(x, y, tableWidth int) {
	// Left corner
	tv.viewport.SetContent(x, y, '└', nil, tcell.StyleDefault.Foreground(tv.borderColor))
	pos := x + 1

	// Column sections
	for i, header := range tv.headers {
		cellWidth := header.Width + 2*tv.cellPadding

		// Horizontal line for this column
		for j := 0; j < cellWidth; j++ {
			tv.viewport.SetContent(pos+j, y, '─', nil, tcell.StyleDefault.Foreground(tv.borderColor))
		}
		pos += cellWidth

		// Junction or corner
		if i < len(tv.headers)-1 {
			tv.viewport.SetContent(pos, y, '┴', nil, tcell.StyleDefault.Foreground(tv.borderColor))
			pos++
		} else {
			tv.viewport.SetContent(pos, y, '┘', nil, tcell.StyleDefault.Foreground(tv.borderColor))
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
				tv.Select(tv.selectedRow-1, tv.selectedCol)
			}
		case tcell.KeyDown:
			if len(tv.insertRow) > 0 {
				return // Disable vertical navigation in insert mode
			}
			if tv.selectedRow < len(tv.data)-1 {
				tv.Select(tv.selectedRow+1, tv.selectedCol)
			}
		case tcell.KeyLeft:
			if tv.selectedCol > 0 {
				tv.Select(tv.selectedRow, tv.selectedCol-1)
			}
		case tcell.KeyRight:
			if tv.selectedCol < len(tv.headers)-1 {
				tv.Select(tv.selectedRow, tv.selectedCol+1)
			}
		case tcell.KeyHome:
			tv.Select(tv.selectedRow, 0)
		case tcell.KeyEnd:
			tv.Select(tv.selectedRow, len(tv.headers)-1)
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

			// Check if clicked on column separator for drag resize
			separatorCol := tv.GetColumnSeparatorAtPosition(x, y)
			if separatorCol >= 0 {
				// Start drag resize
				tv.resizingColumn = separatorCol
				tv.resizeStartX = x
				tv.resizeStartWidth = tv.headers[separatorCol].Width
				return true, tv // Capture further mouse events
			}

			if tv.selectable {
				// Convert screen coordinates to cell coordinates
				row, col := tv.GetCellAtPosition(x, y)
				if row >= 0 && col >= 0 {
					tv.Select(row, col)
					consumed = true
				}
			}
		case tview.MouseMove:
			// Handle column resizing drag
			if tv.resizingColumn >= 0 {
				// Calculate new width based on mouse movement
				delta := x - tv.resizeStartX
				newWidth := tv.resizeStartWidth + delta
				tv.SetColumnWidth(tv.resizingColumn, newWidth)
				return true, tv // Continue capturing
			}
		case tview.MouseLeftUp:
			// End drag resize
			if tv.resizingColumn >= 0 {
				tv.resizingColumn = -1
				return true, nil // Release capture
			}
		case tview.MouseLeftClick:
			// Check if click is on table name header
			_, innerY, _, _ := tv.GetInnerRect()
			relativeY := y - innerY

			// Table name is at relativeY == 0 (if tableName is set)
			if relativeY == 0 && tv.tableName != "" && tv.tableNameClickFunc != nil {
				tv.tableNameClickFunc()
				consumed = true
				return consumed, nil
			}

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
		default:
			// Delegate scroll and other events to mouseScrollFunc if set
			if tv.mouseScrollFunc != nil {
				action, event = tv.mouseScrollFunc(action, event)
				if action == tview.MouseConsumed {
					consumed = true
				}
			}
		}

		return consumed, nil
	})
}

// UpdateCell updates a cell value and refreshes the display
func (tv *TableView) UpdateCell(row, col int, value any) *TableView {
	if row >= 0 && row < len(tv.data) && col >= 0 && col < len(tv.data[row].data) {
		tv.data[row].data[col] = value
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

// GetHeaders returns the table headers
func (tv *TableView) GetHeaders() []dblib.DisplayColumn {
	return tv.headers
}

// GetColumnPosition returns the start and end x positions of a column relative to the table
// startX is the leftmost position of the column content (including padding)
// endX is one past the rightmost position of the column content (including padding)
func (tv *TableView) GetColumnPosition(col int) (startX, endX int) {
	if col < 0 || col >= len(tv.headers) {
		return 0, 0
	}

	// Start after the left border
	pos := 1

	// Add width of all previous columns (including padding and separators)
	for i := 0; i < col; i++ {
		pos += tv.headers[i].Width + 2*tv.cellPadding
		if i < len(tv.headers)-1 {
			pos += 1 // Column separator
		}
	}

	startX = pos
	endX = pos + tv.headers[col].Width + 2*tv.cellPadding

	return startX, endX
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
	// When tableName is set:
	//   Row 0: table name header
	//   Row 1: top border
	//   Row 2: header
	//   Row 3: header separator
	//   Row 4+: data rows
	// When tableName is empty:
	//   Row 0: top border
	//   Row 1: header
	//   Row 2: header separator
	//   Row 3+: data rows
	relativeY := screenY - y
	headerOffset := 3
	if tv.tableName != "" {
		headerOffset = 4
	}
	if relativeY < headerOffset {
		return -1, -1 // Clicked on border/header, not a data cell
	}

	dataRow := relativeY - headerOffset
	if dataRow < 0 || dataRow >= len(tv.data) || tv.data[dataRow].data == nil {
		return -1, -1 // Beyond available data
	}

	// Calculate which column was clicked
	// Account for viewport scrolling - adjust screen coordinate to table coordinate
	relativeX := screenX - x + tv.viewport.GetScrollX()
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

// GetColumnSeparatorAtPosition returns the column index if the position is on a column separator,
// or -1 if not on a separator. Uses tolerance of ±1 for easier clicking.
func (tv *TableView) GetColumnSeparatorAtPosition(screenX, screenY int) int {
	x, y, width, _ := tv.GetInnerRect()

	// Check if click is within the table bounds (horizontally)
	if screenX < x || screenX >= x+width {
		return -1
	}

	// Allow clicking on header row or any data row
	// When tableName is set, header is at row 2, data starts at row 4
	// When tableName is empty, header is at row 1, data starts at row 3
	relativeY := screenY - y
	headerRow := 1
	dataStartRow := 3
	if tv.tableName != "" {
		headerRow = 2
		dataStartRow = 4
	}
	if relativeY != headerRow && relativeY < dataStartRow {
		return -1 // Not on header or data area
	}

	// Account for viewport scrolling - adjust screen coordinate to table coordinate
	relativeX := screenX - x + tv.viewport.GetScrollX()
	if relativeX < 1 {
		return -1 // Before left border
	}

	// Walk through columns to find column separators
	currentX := 1 // Start after left border
	for i, header := range tv.headers {
		cellWidth := header.Width + 2*tv.cellPadding
		currentX += cellWidth

		// Check if we're on a separator (with tolerance of ±1)
		if i < len(tv.headers)-1 {
			if relativeX >= currentX-1 && relativeX <= currentX+1 {
				return i // Return the column to the left of this separator
			}
			currentX += 1
		}
	}

	return -1 // Not on any separator
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
