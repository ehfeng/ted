package main

import (
	"database/sql"
	"fmt"
	"strings"
)

type Table struct {
	Name    string
	Columns []Column
	Rows    [][]interface{}
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

	for rows.Next() {
		var col Column
		var nullable string
		
		switch dbType {
		case SQLite:
			var cid int
			var dfltValue sql.NullString
			var pk int
			err = rows.Scan(&cid, &col.Name, &col.Type, &pk, &dfltValue, &nullable)
			col.Nullable = nullable != "1"
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

	return rows.Err()
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