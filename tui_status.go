package main

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
)

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
