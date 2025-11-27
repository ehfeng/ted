package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gdamore/tcell/v2"

	"ted/internal/dblib"
)

func (e *Editor) moveColumn(col, direction int) {
	headers := e.table.GetHeaders()
	if col < 0 || col >= len(headers) {
		return
	}

	newIdx := col + direction
	if newIdx < 0 || newIdx >= len(headers) {
		return
	}

	// Swap headers
	headers[col], headers[newIdx] = headers[newIdx], headers[col]
	e.table.SetHeaders(headers)

	// Swap buffer data
	for i := range e.buffer {
		e.buffer[i].data[col], e.buffer[i].data[newIdx] = e.buffer[i].data[newIdx], e.buffer[i].data[col]
	}

	row, _ := e.table.GetSelection()
	e.table.Select(row, col+direction)
}

func (e *Editor) adjustColumnWidth(col, delta int) {
	headers := e.table.GetHeaders()
	if col < 0 || col >= len(headers) {
		return
	}

	newWidth := max(3, headers[col].Width+delta)

	// Update the table column width (this updates the internal headers)
	e.table.SetColumnWidth(col, newWidth)
}

func formatCellValue(value any, cellStyle tcell.Style) (string, tcell.Style) {
	if value == dblib.EmptyCellValue {
		return "", cellStyle
	}
	if value == nil {
		return dblib.NullDisplay, cellStyle.Italic(true).Foreground(tcell.ColorGray)
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
