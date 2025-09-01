package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

type Connection struct {
	DB     *sql.DB
	Driver string
	DBName string
}

func connectToDatabase(config DatabaseConfig, flags ConnectionFlags) (*Connection, error) {
	var driver, dsn string
	
	dbType := config.Type
	if flags.Database != "" {
		dbType = flags.Database
	}
	
	switch strings.ToLower(dbType) {
	case "sqlite3", "sqlite":
		driver = "sqlite3"
		dbFile := config.DBName
		if flags.Database != "" {
			dbFile = flags.Database
		}
		dsn = dbFile
		
	case "postgres", "postgresql":
		driver = "postgres"
		host := getStringValue(config.Host, flags.Host, "localhost")
		port := getStringValue(config.Port, flags.Port, "5432")
		user := getStringValue(config.User, flags.Username, os.Getenv("USER"))
		password := getStringValue("", flags.Password, "")
		dbname := getStringValue(config.DBName, "", "")
		
		var parts []string
		parts = append(parts, fmt.Sprintf("host=%s", host))
		parts = append(parts, fmt.Sprintf("port=%s", port))
		parts = append(parts, fmt.Sprintf("user=%s", user))
		if password != "" {
			parts = append(parts, fmt.Sprintf("password=%s", password))
		}
		if dbname != "" {
			parts = append(parts, fmt.Sprintf("dbname=%s", dbname))
		}
		parts = append(parts, "sslmode=disable")
		dsn = strings.Join(parts, " ")
		
	case "mysql":
		driver = "mysql"
		host := getStringValue(config.Host, flags.Host, "localhost")
		port := getStringValue(config.Port, flags.Port, "3306")
		user := getStringValue(config.User, flags.Username, "root")
		password := getStringValue("", flags.Password, "")
		dbname := getStringValue(config.DBName, "", "")
		
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, host, port, dbname)
		
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}
	
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	
	return &Connection{
		DB:     db,
		Driver: driver,
		DBName: config.DBName,
	}, nil
}

type ConnectionFlags struct {
	Database string
	Host     string
	Port     string
	Username string
	Password string
}

func getStringValue(configValue, flagValue, defaultValue string) string {
	if flagValue != "" {
		return flagValue
	}
	if configValue != "" {
		return configValue
	}
	return defaultValue
}

func (c *Connection) QueryTable(query string) (*TableData, error) {
	rows, err := c.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()
	
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}
	
	var data [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		
		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		
		data = append(data, values)
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}
	
	tableData := &TableData{
		Columns:     columns,
		ColumnTypes: columnTypes,
		Data:        data,
	}
	
	return tableData, nil
}

type TableData struct {
	Columns     []string
	ColumnTypes []*sql.ColumnType
	Data        [][]interface{}
	TableName   string
	PrimaryKeys []string
}

func (c *Connection) UpdateCell(tableName string, rowData []interface{}, columnIndex int, newValue interface{}, columns []string) error {
	if tableName == "" {
		return fmt.Errorf("cannot update: table name not available")
	}
	
	// Build WHERE clause using all current row values for safety
	var whereParts []string
	var whereArgs []interface{}
	
	for i, value := range rowData {
		if i == columnIndex {
			continue // Skip the column we're updating
		}
		
		if value == nil {
			whereParts = append(whereParts, fmt.Sprintf("%s IS NULL", columns[i]))
		} else {
			whereParts = append(whereParts, fmt.Sprintf("%s = ?", columns[i]))
			whereArgs = append(whereArgs, value)
		}
	}
	
	if len(whereParts) == 0 {
		return fmt.Errorf("cannot update: no identifying columns available")
	}
	
	// Build UPDATE query with RETURNING clause for verification
	query := fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s", 
		tableName, 
		columns[columnIndex], 
		strings.Join(whereParts, " AND "))
	
	args := append([]interface{}{newValue}, whereArgs...)
	
	result, err := c.DB.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}
	
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("could not get rows affected: %w", err)
	}
	
	if rowsAffected == 0 {
		return fmt.Errorf("no rows were updated (row may have been modified)")
	}
	
	if rowsAffected > 1 {
		return fmt.Errorf("warning: %d rows were updated (expected 1)", rowsAffected)
	}
	
	return nil
}

func (c *Connection) InsertRow(tableName string, columns []string) error {
	if tableName == "" {
		return fmt.Errorf("cannot insert: table name not available")
	}
	
	placeholders := make([]string, len(columns))
	args := make([]interface{}, len(columns))
	
	for i := range columns {
		placeholders[i] = "?"
		args[i] = nil // Default to NULL values
	}
	
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", 
		tableName, 
		strings.Join(columns, ", "), 
		strings.Join(placeholders, ", "))
	
	_, err := c.DB.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("insert failed: %w", err)
	}
	
	return nil
}