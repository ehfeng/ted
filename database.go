package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Table struct {
	Name         string
	LookupKey    []string // empty if no primary key
	Columns      []Column
	Rows         [][]interface{}
	TempFilePath string               // path to temporary file containing full dataset
	TuiChan      <-chan []interface{} // channel for TUI updates
	FileChan     <-chan []interface{} // channel for file writing
	StopChan     chan struct{}        // channel to stop streaming
}

type Column struct {
	Name     string
	Type     string
	Width    int
	Nullable bool
}

func parseTableSpec(tableSpec string) (string, []string) {
	if tableSpec == "" {
		return "", nil
	}

	parts := strings.Split(tableSpec, ".")
	if len(parts) == 1 {
		return parts[0], nil
	}

	tableName := parts[0]
	if len(parts) > 1 && parts[1] != "" {
		columns := strings.Split(parts[1], ",")
		for i, col := range columns {
			columns[i] = strings.TrimSpace(col)
		}
		return tableName, columns
	}

	return tableName, nil
}

func loadTable(db *sql.DB, dbType DatabaseType, tableSpec string, terminalHeight int) (*Table, error) {
	tableName, columns := parseTableSpec(tableSpec)

	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

	table := &Table{
		Name: tableName,
	}

	if err := loadTableSchema(db, dbType, table); err != nil {
		return nil, fmt.Errorf("failed to load table schema: %w", err)
	}

	if err := loadTableData(db, table, columns, terminalHeight); err != nil {
		return nil, fmt.Errorf("failed to load table data: %w", err)
	}

	return table, nil
}

func loadTableSchema(db *sql.DB, dbType DatabaseType, table *Table) error {
	var query string

	switch dbType {
	case SQLite:
		query = fmt.Sprintf("PRAGMA table_info(%s)", table.Name)
	case PostgreSQL:
		query = `SELECT column_name, data_type, is_nullable 
				FROM information_schema.columns 
				WHERE table_name = $1 
				ORDER BY ordinal_position`
	case MySQL:
		query = `SELECT column_name, data_type, is_nullable 
				FROM information_schema.columns 
				WHERE table_name = ? 
				ORDER BY ordinal_position`
	}

	rows, err := db.Query(query, table.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	var primaryKeyColumns []string

	for rows.Next() {
		var col Column
		var nullable string

		switch dbType {
		case SQLite:
			var cid int
			var dfltValue sql.NullString
			var pk int
			err = rows.Scan(&cid, &col.Name, &col.Type, &nullable, &dfltValue, &pk)
			col.Nullable = nullable != "1"
			if pk == 1 {
				primaryKeyColumns = append(primaryKeyColumns, col.Name)
			}
		case PostgreSQL, MySQL:
			err = rows.Scan(&col.Name, &col.Type, &nullable)
			col.Nullable = strings.ToLower(nullable) == "yes"
		}

		if err != nil {
			return err
		}

		col.Width = max(len(col.Name), 8)
		table.Columns = append(table.Columns, col)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// For PostgreSQL and MySQL, we need separate queries to get primary key info
	if dbType == PostgreSQL || dbType == MySQL {
		pkCols, err := getPrimaryKeyColumns(db, dbType, table.Name)
		if err != nil {
			return err
		}
		primaryKeyColumns = pkCols
	}

	// If no primary key found, try to find the simplest unique constraint
	if len(primaryKeyColumns) == 0 {
		uniqueCols, err := getSimplestUniqueConstraint(db, dbType, table.Name)
		if err != nil {
			return err
		}
		primaryKeyColumns = uniqueCols
	}

	table.LookupKey = primaryKeyColumns
	return nil
}

func loadTableData(db *sql.DB, table *Table, requestedColumns []string, terminalHeight int) error {
	var columnNames []string

	if len(requestedColumns) > 0 {
		columnNames = requestedColumns
		filteredCols := make([]Column, 0)
		for _, reqCol := range requestedColumns {
			for _, col := range table.Columns {
				if col.Name == reqCol {
					filteredCols = append(filteredCols, col)
					break
				}
			}
		}
		table.Columns = filteredCols
	} else {
		for _, col := range table.Columns {
			columnNames = append(columnNames, col.Name)
		}
	}

	// Create temporary file for full dataset
	tempFile, err := os.CreateTemp("", "ted_data_*.csv")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	table.TempFilePath = tempFile.Name()

	// Use passed terminal height for buffer sizing
	tuiBufferSize := max(50, terminalHeight*2) // Buffer for 2 screens worth

	// Create channels for streaming
	tuiChan := make(chan []interface{}, tuiBufferSize) // Buffered based on terminal size
	fileChan := make(chan []interface{}, 1000)         // Larger buffer for file writing
	stopChan := make(chan struct{})

	// Set up channels in table
	table.TuiChan = tuiChan
	table.FileChan = fileChan
	table.StopChan = stopChan

	// Write CSV header to temp file
	csvWriter := csv.NewWriter(tempFile)
	headers := make([]string, len(columnNames))
	copy(headers, columnNames)
	if err := csvWriter.Write(headers); err != nil {
		tempFile.Close()
		return fmt.Errorf("failed to write CSV headers: %w", err)
	}

	// Start goroutine for file writing
	go func() {
		defer func() {
			csvWriter.Flush()
			tempFile.Close()
		}()

		for row := range fileChan {
			record := make([]string, len(row))
			for i, val := range row {
				record[i] = formatCSVValue(val)
			}
			if err := csvWriter.Write(record); err != nil {
				// Log error but continue
				fmt.Fprintf(os.Stderr, "Error writing to temp file: %v\n", err)
			}
		}
	}()

	// Start goroutine for data streaming
	go func(maxTuiRows int) {
		defer func() {
			close(tuiChan)
			close(fileChan)
		}()

		query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columnNames, ", "), table.Name)

		rows, err := db.Query(query)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Query error: %v\n", err)
			return
		}
		defer rows.Close()

		rowCount := 0
		for rows.Next() {
			select {
			case <-stopChan:
				return
			default:
			}

			values := make([]interface{}, len(columnNames))
			scanArgs := make([]interface{}, len(columnNames))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			if err := rows.Scan(scanArgs...); err != nil {
				fmt.Fprintf(os.Stderr, "Scan error: %v\n", err)
				continue
			}

			// Send to both channels
			rowCopy1 := make([]interface{}, len(values))
			rowCopy2 := make([]interface{}, len(values))
			copy(rowCopy1, values)
			copy(rowCopy2, values)

			// Send to file channel (non-blocking)
			select {
			case fileChan <- rowCopy1:
			default:
				// File channel full, skip this row for file (shouldn't happen with large buffer)
			}

			// Send to TUI channel for initial screen population
			if rowCount < maxTuiRows {
				select {
				case tuiChan <- rowCopy2:
					rowCount++
				default:
					// TUI channel full, continue streaming to file only
				}
			}
		}

		if err := rows.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Rows iteration error: %v\n", err)
		}
	}(tuiBufferSize)

	// Reserve space for headers, borders, and some margin
	initialBatchSize := max(10, terminalHeight-5)

	// Load initial batch with proper timeout handling
	initialBatch := make([][]interface{}, 0, initialBatchSize)
	done := make(chan bool, 1)

	// Collect available rows up to the batch size, but don't block if fewer are available
	go func() {
		defer func() { done <- true }()

		// Use a reasonable timeout for data collection
		timeout := make(chan bool, 1)
		go func() {
			// Wait 200ms for data, then timeout
			for i := 0; i < 200; i++ {
				// 1ms delays
				for j := 0; j < 100000; j++ {
				}
			}
			timeout <- true
		}()

		// Collect rows until we have enough, the channel closes, or we timeout
		for len(initialBatch) < initialBatchSize {
			select {
			case row, ok := <-table.TuiChan:
				if !ok {
					// Channel closed, no more data
					return
				}
				initialBatch = append(initialBatch, row)
			case <-timeout:
				// Timeout reached, use whatever we have
				return
			}
		}
	}()

	<-done
	table.Rows = initialBatch

	return nil
}

func formatCSVValue(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case []byte:
		return string(v)
	case string:
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

func getPrimaryKeyColumns(db *sql.DB, dbType DatabaseType, tableName string) ([]string, error) {
	var query string
	var columns []string

	switch dbType {
	case PostgreSQL:
		query = `SELECT a.attname
				FROM pg_index i
				JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
				WHERE i.indrelid = $1::regclass AND i.indisprimary
				ORDER BY a.attnum`
	case MySQL:
		query = `SELECT column_name
				FROM information_schema.key_column_usage
				WHERE table_name = ? AND constraint_name = 'PRIMARY'
				ORDER BY ordinal_position`
	default:
		return columns, nil
	}

	rows, err := db.Query(query, tableName)
	if err != nil {
		return columns, err
	}
	defer rows.Close()

	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return columns, err
		}
		columns = append(columns, colName)
	}

	return columns, rows.Err()
}

func getSimplestUniqueConstraint(db *sql.DB, dbType DatabaseType, tableName string) ([]string, error) {
	var query string
	var columns []string

	switch dbType {
	case SQLite:
		query = fmt.Sprintf("PRAGMA index_list(%s)", tableName)
	case PostgreSQL:
		query = `SELECT i.indexname, array_agg(a.attname ORDER BY a.attnum) as columns
				FROM pg_index idx
				JOIN pg_class c ON c.oid = idx.indrelid
				JOIN pg_class i ON i.oid = idx.indexrelid
				JOIN pg_attribute a ON a.attrelid = idx.indrelid AND a.attnum = ANY(idx.indkey)
				WHERE c.relname = $1 AND idx.indisunique AND NOT idx.indisprimary
				GROUP BY i.indexname, array_length(idx.indkey, 1)
				ORDER BY array_length(idx.indkey, 1), i.indexname
				LIMIT 1`
	case MySQL:
		query = `SELECT column_name
				FROM information_schema.statistics
				WHERE table_name = ? AND non_unique = 0 AND index_name != 'PRIMARY'
				GROUP BY index_name
				ORDER BY COUNT(*), index_name
				LIMIT 1`
	default:
		return columns, nil
	}

	switch dbType {
	case SQLite:
		rows, err := db.Query(query)
		if err != nil {
			return columns, err
		}
		defer rows.Close()

		var bestIndex string
		minColumns := 999

		for rows.Next() {
			var seq int
			var name string
			var unique int
			var origin string
			var partial int
			if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
				continue
			}

			if unique == 1 {
				indexInfo, err := db.Query(fmt.Sprintf("PRAGMA index_info(%s)", name))
				if err != nil {
					continue
				}
				defer indexInfo.Close()

				var colCount int
				for indexInfo.Next() {
					colCount++
				}

				if colCount < minColumns {
					minColumns = colCount
					bestIndex = name
				}
			}
		}

		if bestIndex != "" {
			indexInfo, err := db.Query(fmt.Sprintf("PRAGMA index_info(%s)", bestIndex))
			if err != nil {
				return columns, err
			}
			defer indexInfo.Close()

			for indexInfo.Next() {
				var seqno, cid int
				var name string
				if err := indexInfo.Scan(&seqno, &cid, &name); err != nil {
					continue
				}
				columns = append(columns, name)
			}
		}

	case PostgreSQL:
		rows, err := db.Query(query, tableName)
		if err != nil {
			return columns, err
		}
		defer rows.Close()

		if rows.Next() {
			var indexName string
			var colArray string
			if err := rows.Scan(&indexName, &colArray); err != nil {
				return columns, err
			}
			// Parse PostgreSQL array format {col1,col2}
			colArray = strings.Trim(colArray, "{}")
			if colArray != "" {
				columns = strings.Split(colArray, ",")
			}
		}

	case MySQL:
		rows, err := db.Query(query, tableName)
		if err != nil {
			return columns, err
		}
		defer rows.Close()

		for rows.Next() {
			var colName string
			if err := rows.Scan(&colName); err != nil {
				continue
			}
			columns = append(columns, colName)
		}
	}

	return columns, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
