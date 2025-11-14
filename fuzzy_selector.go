package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// fuzzyMatch performs fuzzy matching and returns match status and positions.
// It matches characters from search in order within text (case-insensitive).
// Returns true if all characters in search were found, and the positions of those characters.
func fuzzyMatch(search, text string) (bool, []int) {
	search = strings.ToLower(search)
	text = strings.ToLower(text)

	var positions []int
	searchIdx := 0

	for i, char := range text {
		if searchIdx < len(search) && char == rune(search[searchIdx]) {
			positions = append(positions, i)
			searchIdx++
		}
	}

	return searchIdx == len(search), positions
}

// formatTableNameWithColor formats the table name with tview color codes highlighting for matches.
// Matched positions are highlighted in bold dark green.
func formatTableNameWithColor(table string, positions []int) string {
	if len(positions) == 0 {
		return table
	}

	// Build a map of character positions to highlight
	highlightMap := make(map[int]bool)
	for _, pos := range positions {
		highlightMap[pos] = true
	}

	// Build the formatted string with color codes
	var result strings.Builder
	runes := []rune(table)
	for i, r := range runes {
		if highlightMap[i] {
			// Bold and dark green for matches
			result.WriteString("[darkgreen::b]")
			result.WriteRune(r)
			result.WriteString("[-::-]")
		} else {
			result.WriteRune(r)
		}
	}

	return result.String()
}

// cleanTableNames removes newlines and whitespace from table names
func cleanTableNames(tables []string) []string {
	cleaned := make([]string, 0, len(tables))
	for _, table := range tables {
		cleaned_name := strings.TrimSpace(strings.ReplaceAll(table, "\n", ""))
		if cleaned_name != "" {
			cleaned = append(cleaned, cleaned_name)
		}
	}
	return cleaned
}

// FuzzySelector manages the table selection UI component at the top of the editor.
// It displays a searchable dropdown for table selection.
type FuzzySelector struct {
	*tview.Box
	items         []string          // All available tables
	searchText    string            // Current search text
	selectedIndex int               // Highlighted item in dropdown
	dropdownList  *tview.List       // Dropdown list for showing filtered tables
	maxVisible    int               // Max items to show in dropdown (6)
	inputField    *tview.InputField // Reference to the currently created input field
	innerFlex     *tview.Flex       // Inner flex container
	dropdownFlex  *tview.Flex       // Flex container for dropdown (to allow resizing)

	// Callbacks
	onSelect func(tableName string) // Called when a table is selected
	onClose  func()                 // Called when the selector should be closed
}

// NewFuzzySelector creates a new table picker bar component.
func NewFuzzySelector(tables []string, initialTable string, onSelect func(string), onClose func()) *FuzzySelector {
	cleaned := cleanTableNames(tables)
	fs := &FuzzySelector{
		Box:           tview.NewBox(),
		items:         cleaned,
		selectedIndex: 0,
		maxVisible:    6,
		onSelect:      onSelect,
		onClose:       onClose,
	}

	// Pre-initialize the layout so input field exists immediately
	filtered, matchPositions := fs.calculateFiltered("")
	fs.buildInnerLayout(filtered, matchPositions)

	return fs
}

// calculateFiltered filters the table list based on search text and returns filtered tables and match positions.
func (tp *FuzzySelector) calculateFiltered(search string) ([]string, map[int][]int) {
	filtered := []string{}
	matchPositions := make(map[int][]int)

	if search == "" {
		// No search, show all tables
		filtered = tp.items
		for i := range tp.items {
			matchPositions[i] = []int{}
		}
	} else {
		// Fuzzy search
		for _, table := range tp.items {
			matches, positions := fuzzyMatch(search, table)
			if matches {
				filtered = append(filtered, table)
				matchPositions[len(filtered)-1] = positions
			}
		}
	}

	return filtered, matchPositions
}

// Draw implements tview.Primitive and renders the fuzzy selector.
// It calculates filtered results and match positions on each frame.
func (fs *FuzzySelector) Draw(screen tcell.Screen) {
	fmt.Fprintf(os.Stderr, "[DEBUG] Drawing fuzzy selector\n")
	fs.Box.DrawForSubclass(screen, fs)

	// Calculate filtered results and match positions on each draw
	filtered, matchPositions := fs.calculateFiltered(fs.searchText)

	// Build or rebuild the inner layout if needed
	if fs.innerFlex == nil {
		fs.buildInnerLayout(filtered, matchPositions)
	} else {
		// Just update the dropdown list without rebuilding the input field
		fs.updateDropdownList(filtered, matchPositions)
	}

	// Draw the inner layout
	if fs.innerFlex != nil {
		x, y, width, height := fs.GetInnerRect()

		// Set up the inner flex with proper sizing
		fs.innerFlex.SetRect(x, y, width, height)
		fs.innerFlex.Draw(screen)
	}
}

// InputHandler returns the handler for keyboard events.
// This forwards input to the input field so it can receive keystrokes.
func (fs *FuzzySelector) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return fs.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		// Forward all input to the input field if it exists
		if fs.inputField != nil {
			if handler := fs.inputField.InputHandler(); handler != nil {
				handler(event, setFocus)
				return
			}
		}
	})
}

// Focus is called when this primitive receives focus.
func (fs *FuzzySelector) Focus(delegate func(p tview.Primitive)) {
	// Forward focus to the input field
	if fs.inputField != nil {
		delegate(fs.inputField)
	}
}

// HasFocus returns whether or not this primitive has focus.
func (fs *FuzzySelector) HasFocus() bool {
	// Check if the input field has focus
	if fs.inputField != nil {
		return fs.inputField.HasFocus()
	}
	return false
}

// buildInnerLayout builds the internal flex layout with input field and dropdown.
func (fs *FuzzySelector) buildInnerLayout(filtered []string, matchPositions map[int][]int) {
	inputField := fs.createInputField()
	fs.createDropdownListWithData(filtered, matchPositions)

	// Calculate height for dropdown
	listHeight := len(filtered)
	if listHeight == 0 {
		listHeight = 1 // Show "No results"
	}
	if listHeight > fs.maxVisible {
		listHeight = fs.maxVisible
	}

	// Inner flex: input field + dropdown list
	fs.dropdownFlex = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(inputField, 1, 0, true).
		AddItem(fs.dropdownList, listHeight, 0, false)

	// Outer flex: 1-character left padding + inner flex
	fs.innerFlex = tview.NewFlex().SetDirection(tview.FlexColumn).
		AddItem(tview.NewBox(), 1, 0, false). // 1-character left padding
		AddItem(fs.dropdownFlex, 0, 1, true)
}

// updateDropdownList updates just the dropdown list without rebuilding the input field.
func (fs *FuzzySelector) updateDropdownList(filtered []string, matchPositions map[int][]int) {
	if fs.dropdownFlex == nil {
		return
	}

	// Remove old dropdown from flex
	fs.dropdownFlex.RemoveItem(fs.dropdownList)

	// Create new dropdown with updated data
	fs.createDropdownListWithData(filtered, matchPositions)

	// Calculate height for dropdown
	listHeight := len(filtered)
	if listHeight == 0 {
		listHeight = 1 // Show "No results"
	}
	if listHeight > fs.maxVisible {
		listHeight = fs.maxVisible
	}

	// Add new dropdown to flex
	fs.dropdownFlex.AddItem(fs.dropdownList, listHeight, 0, false)
}

// createInputField creates and returns a new input field for the edit mode.
func (tp *FuzzySelector) createInputField() *tview.InputField {
	inputField := tview.NewInputField().
		SetLabel("").
		SetText(tp.searchText).
		SetPlaceholder("Search for tables...").
		SetFieldWidth(0)

	// Store reference to the input field
	tp.inputField = inputField

	// Update search text (dropdown will be updated in Draw)
	inputField.SetChangedFunc(func(text string) {
		tp.searchText = text
	})

	// Handle Enter key: select current item from dropdown
	inputField.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		fmt.Fprintf(os.Stderr, "[DEBUG] Input field capture: %v\n", event.Key())
		switch event.Key() {
		case tcell.KeyEscape:
			// Close the fuzzy selector
			if tp.onClose != nil {
				tp.onClose()
			}
			return nil // Consume the event
		case tcell.KeyDown, tcell.KeyTab:
			// Move focus to dropdown list
			filtered, _ := tp.calculateFiltered(tp.searchText)
			if tp.dropdownList != nil && len(filtered) > 0 {
				// Focus will move to dropdownList (it's next in the flex)
				return event
			}
		case tcell.KeyEnter:
			// Select the currently highlighted item in dropdown
			filtered, _ := tp.calculateFiltered(tp.searchText)
			if tp.dropdownList != nil && len(filtered) > 0 {
				if idx := tp.dropdownList.GetCurrentItem(); idx >= 0 && idx < len(filtered) {
					if tp.onSelect != nil {
						tp.onSelect(filtered[idx])
					}
				}
				return nil // Consume the event
			}
		}
		return event
	})

	return inputField
}

// GetInputField returns the most recently created input field instance.
// This is used by the Editor to set focus when the picker is opened.
func (tp *FuzzySelector) GetInputField() *tview.InputField {
	return tp.inputField
}

// createDropdownListWithData creates and populates the dropdown list with pre-calculated filtered results.
func (tp *FuzzySelector) createDropdownListWithData(filtered []string, matchPositions map[int][]int) {
	tp.dropdownList = tview.NewList().
		SetWrapAround(true).
		ShowSecondaryText(false)

	// Populate with filtered results
	if len(filtered) == 0 {
		tp.dropdownList.AddItem("No results", "", rune(0), nil)
	} else {
		for i, tableName := range filtered {
			// Get match positions and format with highlighting
			positions := matchPositions[i]
			displayText := formatTableNameWithColor(tableName, positions)

			// Capture table name in closure for selection handler
			name := tableName
			tp.dropdownList.AddItem(displayText, "", rune(0), func() {
				if tp.onSelect != nil {
					tp.onSelect(name)
				}
			})
		}
	}

	// Set current item to match selectedIndex
	if tp.selectedIndex >= 0 && tp.selectedIndex < len(filtered) {
		tp.dropdownList.SetCurrentItem(tp.selectedIndex)
	}

	// Handle Escape key in dropdown to return focus to input field
	tp.dropdownList.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape {
			// Focus will return to input field
			return event
		}
		return event
	})
}

// formatTablePickerDisplayText formats the display mode text showing the current table.
func formatTablePickerDisplayText(tableName string) string {
	return fmt.Sprintf(" %s ▾", tableName)
}
