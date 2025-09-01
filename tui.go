package main

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type Model struct {
	config     *Config
	connection *Connection
	table      *TableData
	
	focusRow    int
	focusCol    int
	scrollRow   int
	scrollCol   int
	
	editing     bool
	editValue   string
	
	width       int
	height      int
	
	statusMsg   string
	errorMsg    string
}

func NewModel(config *Config, connection *Connection) Model {
	return Model{
		config:     config,
		connection: connection,
		focusRow:   0,
		focusCol:   0,
	}
}

func (m Model) Init() tea.Cmd {
	return nil
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
		
	case tea.KeyMsg:
		if m.editing {
			return m.handleEditingKeys(msg)
		}
		return m.handleNavigationKeys(msg)
		
	case tableDataMsg:
		m.table = msg.data
		m.errorMsg = ""
		return m, nil
		
	case errorMsg:
		m.errorMsg = msg.err.Error()
		return m, nil
	}
	
	return m, nil
}

func (m Model) handleNavigationKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c", "q":
		return m, tea.Quit
		
	case "up", "k":
		if m.focusRow > 0 {
			m.focusRow--
		}
		
	case "down", "j":
		if m.table != nil && m.focusRow < len(m.table.Data)-1 {
			m.focusRow++
		}
		
	case "left", "h":
		if m.focusCol > 0 {
			m.focusCol--
		}
		
	case "right", "l":
		if m.table != nil && m.focusCol < len(m.table.Columns)-1 {
			m.focusCol++
		}
		
	case "home":
		m.focusCol = 0
		
	case "end":
		if m.table != nil {
			m.focusCol = len(m.table.Columns) - 1
		}
		
	case "page_up", "pgup":
		m.focusRow -= 10
		if m.focusRow < 0 {
			m.focusRow = 0
		}
		
	case "page_down", "pgdown":
		if m.table != nil {
			m.focusRow += 10
			if m.focusRow >= len(m.table.Data) {
				m.focusRow = len(m.table.Data) - 1
			}
		}
		
	case "enter":
		m.editing = true
		if m.table != nil && m.focusRow < len(m.table.Data) {
			if value := m.table.Data[m.focusRow][m.focusCol]; value != nil {
				m.editValue = fmt.Sprintf("%v", value)
			} else {
				m.editValue = ""
			}
		}
		
	case "tab":
		if m.table != nil {
			m.focusCol++
			if m.focusCol >= len(m.table.Columns) {
				m.focusCol = 0
				if m.focusRow < len(m.table.Data)-1 {
					m.focusRow++
				}
			}
		}
		
	case "shift+tab":
		if m.table != nil {
			m.focusCol--
			if m.focusCol < 0 {
				m.focusCol = len(m.table.Columns) - 1
				if m.focusRow > 0 {
					m.focusRow--
				}
			}
		}
		
	case "cmd+n", "ctrl+n":
		// Add new row
		if m.table != nil && m.table.TableName != "" {
			err := m.connection.InsertRow(m.table.TableName, m.table.Columns)
			if err != nil {
				m.errorMsg = err.Error()
			} else {
				// Refresh data to show new row
				return m, refreshTableData(m.connection, "SELECT * FROM " + m.table.TableName)
			}
		}
		
	case "cmd+r", "ctrl+r", "F5":
		// Refresh data
		if m.table != nil && m.table.TableName != "" {
			return m, refreshTableData(m.connection, "SELECT * FROM " + m.table.TableName)
		}
	}
	
	return m, nil
}

func (m Model) handleEditingKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.editing = false
		m.editValue = ""
		
	case "enter":
		m.editing = false
		if m.table != nil && m.table.TableName != "" {
			// Update the database
			rowData := m.table.Data[m.focusRow]
			newValue := m.editValue
			
			// Convert empty string to nil for database
			var dbValue interface{} = newValue
			if newValue == "" {
				dbValue = nil
			}
			
			err := m.connection.UpdateCell(m.table.TableName, rowData, m.focusCol, dbValue, m.table.Columns)
			if err != nil {
				m.errorMsg = err.Error()
			} else {
				// Update local data cache
				m.table.Data[m.focusRow][m.focusCol] = dbValue
				m.statusMsg = "Cell updated successfully"
			}
		}
		m.editValue = ""
		
	case "backspace":
		if len(m.editValue) > 0 {
			m.editValue = m.editValue[:len(m.editValue)-1]
		}
		
	default:
		// Filter out special keys
		if len(msg.String()) == 1 {
			m.editValue += msg.String()
		}
	}
	
	return m, nil
}

func (m Model) View() string {
	if m.table == nil {
		if m.errorMsg != "" {
			return m.renderError()
		}
		return "Loading table data..."
	}
	
	var b strings.Builder
	
	// Render table
	b.WriteString(m.renderTable())
	
	// Render status bar
	b.WriteString("\n")
	b.WriteString(m.renderStatusBar())
	
	return b.String()
}

func (m Model) renderTable() string {
	if m.table == nil || len(m.table.Data) == 0 {
		return "No data available"
	}
	
	var b strings.Builder
	
	// Calculate column widths
	colWidths := m.calculateColumnWidths()
	
	// Render header
	b.WriteString("|")
	for i, col := range m.table.Columns {
		width := colWidths[i]
		content := truncateString(col, width-2)
		if i == m.focusCol {
			content = lipgloss.NewStyle().Bold(true).Render(content)
		}
		b.WriteString(fmt.Sprintf(" %-*s |", width-2, content))
	}
	b.WriteString("\n")
	
	// Render separator
	b.WriteString("|")
	for _, width := range colWidths {
		b.WriteString(strings.Repeat("-", width-1))
		b.WriteString("|")
	}
	b.WriteString("\n")
	
	// Render data rows
	startRow := m.scrollRow
	endRow := startRow + m.height - 4 // Leave space for header, separator, and status
	if endRow > len(m.table.Data) {
		endRow = len(m.table.Data)
	}
	
	for rowIdx := startRow; rowIdx < endRow; rowIdx++ {
		row := m.table.Data[rowIdx]
		b.WriteString("|")
		
		for colIdx, value := range row {
			width := colWidths[colIdx]
			var content string
			if value == nil {
				content = ""
			} else {
				content = fmt.Sprintf("%v", value)
			}
			
			if m.editing && rowIdx == m.focusRow && colIdx == m.focusCol {
				content = m.editValue + "█"
			} else {
				content = truncateString(content, width-2)
			}
			
			if rowIdx == m.focusRow && colIdx == m.focusCol {
				if m.editing {
					content = lipgloss.NewStyle().Background(lipgloss.Color("8")).Render(content)
				} else {
					content = lipgloss.NewStyle().Background(lipgloss.Color("4")).Render(content)
				}
			}
			
			b.WriteString(fmt.Sprintf(" %-*s |", width-2, content))
		}
		b.WriteString("\n")
	}
	
	return b.String()
}

func (m Model) renderStatusBar() string {
	status := fmt.Sprintf("%s | Row %d/%d", 
		m.connection.DBName, 
		m.focusRow+1, 
		len(m.table.Data))
	
	if m.errorMsg != "" {
		status = fmt.Sprintf("Error: %s", m.errorMsg)
	}
	
	return lipgloss.NewStyle().
		Background(lipgloss.Color("8")).
		Foreground(lipgloss.Color("15")).
		Width(m.width).
		Render(status)
}

func (m Model) renderError() string {
	return fmt.Sprintf("Error: %s\n\nPress 'q' to quit.", m.errorMsg)
}

func (m Model) calculateColumnWidths() []int {
	if m.table == nil {
		return nil
	}
	
	numCols := len(m.table.Columns)
	
	// For now, use equal column widths
	widths := make([]int, numCols)
	availableWidth := m.width - numCols - 1 // Account for pipes
	colWidth := availableWidth / numCols
	
	if colWidth < 8 {
		colWidth = 8
	}
	
	for i := range widths {
		widths[i] = colWidth
	}
	
	return widths
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 1 {
		return "…"
	}
	return s[:maxLen-1] + "…"
}

// Messages
type tableDataMsg struct {
	data *TableData
}

type errorMsg struct {
	err error
}

func loadTableData(conn *Connection, query string) tea.Cmd {
	return func() tea.Msg {
		data, err := conn.QueryTable(query)
		if err != nil {
			return errorMsg{err}
		}
		
		// Try to extract table name from query
		data.TableName = extractTableName(query)
		
		return tableDataMsg{data}
	}
}

func refreshTableData(conn *Connection, query string) tea.Cmd {
	return loadTableData(conn, query)
}

func extractTableName(query string) string {
	// Simple table name extraction from SELECT queries
	query = strings.ToLower(strings.TrimSpace(query))
	if strings.HasPrefix(query, "select") {
		parts := strings.Fields(query)
		for i, part := range parts {
			if part == "from" && i+1 < len(parts) {
				tableName := parts[i+1]
				// Remove any trailing conditions or punctuation
				tableName = strings.Split(tableName, " ")[0]
				tableName = strings.Split(tableName, ";")[0]
				tableName = strings.Trim(tableName, "`;")
				return tableName
			}
		}
	}
	return ""
}