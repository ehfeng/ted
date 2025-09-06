package main

import (
	"database/sql"
	"fmt"
	"strings"
)

type Table struct {
	Name      string
	LookupKey []string // empty if no primary key
	Columns   []Column
	Rows      [][]interface{}
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

func loadTable(db *sql.DB, dbType DatabaseType, tableSpec string) (*Table, error) {
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

	if err := loadTableData(db, table, columns); err != nil {
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

func loadTableData(db *sql.DB, table *Table, requestedColumns []string) error {
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

	query := fmt.Sprintf("SELECT %s FROM %s LIMIT 1000", strings.Join(columnNames, ", "), table.Name)

	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		values := make([]interface{}, len(columnNames))
		scanArgs := make([]interface{}, len(columnNames))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}

		for i, val := range values {
			if val != nil {
				str := fmt.Sprintf("%v", val)
				if len(str) > table.Columns[i].Width {
					table.Columns[i].Width = min(len(str), 50)
				}
			}
		}

		table.Rows = append(table.Rows, values)
	}

	return rows.Err()
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
