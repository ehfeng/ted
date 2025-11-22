package main

import (
	"database/sql"
	"fmt"
	"os"
	"os/user"
	"strings"

	"ted/internal/dblib"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

type Config struct {
	Database string
	Host     string
	Port     string
	Username string
	Password string
	Command  string
	Where    string
	OrderBy  string
	// DBTypeOverride allows explicitly selecting the database type via flags
	DBTypeOverride *dblib.DatabaseType
	VimMode        bool
}

var databaseIcons = map[dblib.DatabaseType]string{
	dblib.SQLite:     "🪶",
	dblib.PostgreSQL: "🐘",
	dblib.MySQL:      "🐬",
	dblib.DuckDB:     "🦆",
	dblib.Clickhouse: "⬛",
	dblib.Snowflake:  "❄️",
	dblib.Cockroach:  "🪳",
	dblib.BigQuery:   "🔍",
	dblib.Redshift:   "🟥",
}

func (c *Config) detectDatabaseType() dblib.DatabaseType {
	if c.DBTypeOverride != nil {
		return *c.DBTypeOverride
	}
	if strings.HasSuffix(c.Database, ".sqlite") || strings.HasSuffix(c.Database, ".db") {
		return dblib.SQLite
	}
	if strings.HasSuffix(c.Database, ".duckdb") {
		return dblib.DuckDB
	}
	return dblib.PostgreSQL
}

func (c *Config) buildConnectionString() (string, dblib.DatabaseType, error) {
	dbType := c.detectDatabaseType()

	switch dbType {
	case dblib.SQLite:
		if _, err := os.Stat(c.Database); os.IsNotExist(err) {
			return "", dbType, fmt.Errorf("sqlite file does not exist: %s", c.Database)
		}
		return c.Database, dbType, nil

	case dblib.PostgreSQL:
		connStr := fmt.Sprintf("dbname=%s", c.Database)

		if c.Host != "" {
			connStr += fmt.Sprintf(" host=%s", c.Host)
		}
		if c.Port != "" {
			connStr += fmt.Sprintf(" port=%s", c.Port)
		}
		if c.Username != "" {
			connStr += fmt.Sprintf(" user=%s", c.Username)
		} else {
			if currentUser, err := user.Current(); err == nil {
				connStr += fmt.Sprintf(" user=%s", currentUser.Username)
			}
		}
		if c.Password != "" {
			connStr += fmt.Sprintf(" password=%s", c.Password)
		}
		connStr += " sslmode=disable"

		return connStr, dbType, nil

	case dblib.MySQL:
		connStr := ""
		if c.Username != "" {
			connStr = c.Username
		} else {
			if currentUser, err := user.Current(); err == nil {
				connStr = currentUser.Username
			}
		}

		if c.Password != "" {
			connStr += ":" + c.Password
		}

		connStr += "@"

		if c.Host != "" && c.Port != "" {
			connStr += fmt.Sprintf("tcp(%s:%s)", c.Host, c.Port)
		} else if c.Host != "" {
			connStr += fmt.Sprintf("tcp(%s:3306)", c.Host)
		} else {
			connStr += "tcp(localhost:3306)"
		}

		connStr += "/" + c.Database

		return connStr, dbType, nil

	default:
		return "", dbType, fmt.Errorf("unsupported database type")
	}
}

func (c *Config) connect() (*sql.DB, dblib.DatabaseType, error) {
	connStr, dbType, err := c.buildConnectionString()
	if err != nil {
		return nil, dbType, err
	}

	var driverName string
	switch dbType {
	case dblib.SQLite:
		driverName = "sqlite3"
	case dblib.PostgreSQL:
		driverName = "postgres"
	case dblib.MySQL:
		driverName = "mysql"
	default:
		return nil, dbType, fmt.Errorf("unsupported database type")
	}

	db, err := sql.Open(driverName, connStr)
	if err != nil {
		return nil, dbType, fmt.Errorf("failed to connect to database: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, dbType, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, dbType, nil
}

// GetTables retrieves a list of table names from the database.
func (c *Config) GetTables() ([]string, error) {
	db, dbType, err := c.connect()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var query string
	switch dbType {
	case dblib.PostgreSQL:
		query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
	case dblib.MySQL:
		query = "SELECT table_name FROM information_schema.tables WHERE table_schema = ?"
	case dblib.SQLite:
		query = "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
	default:
		return nil, fmt.Errorf("unsupported database type for GetTables")
	}

	var rows *sql.Rows
	if dbType == dblib.MySQL {
		rows, err = db.Query(query, c.Database)
	} else {
		rows, err = db.Query(query)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tables, nil
}
