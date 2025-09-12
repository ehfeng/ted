package main

import (
	"database/sql"
	"fmt"
	"os"
	"os/user"
	"strings"

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
	Limit    int
}

type DatabaseType int

const (
	SQLite DatabaseType = iota
	PostgreSQL
	MySQL
	DuckDB
	Clickhouse
)

type databaseFeature struct {
	systemId              string
	embedded              bool
	returning             bool
	positionalPlaceholder bool
}

var databaseFeatures = map[DatabaseType]databaseFeature{
	SQLite: {
		systemId:              "rowid",
		embedded:              true,
		returning:             true,
		positionalPlaceholder: false,
	},
	PostgreSQL: {
		systemId:              "ctid",
		embedded:              false,
		returning:             true,
		positionalPlaceholder: true,
	},
	MySQL: {
		systemId:              "",
		embedded:              false,
		returning:             false,
		positionalPlaceholder: false,
	},
	DuckDB: {
		systemId:              "rowid",
		embedded:              true,
		returning:             true,
		positionalPlaceholder: false,
	},
	Clickhouse: {
		systemId:              "",
		embedded:              false,
		returning:             false,
		positionalPlaceholder: false,
	},
}

func (c *Config) detectDatabaseType() DatabaseType {
	if strings.HasSuffix(c.Database, ".sqlite") || strings.HasSuffix(c.Database, ".db") {
		return SQLite
	}
	if strings.HasSuffix(c.Database, ".duckdb") {
		return DuckDB
	}
	return PostgreSQL
}

func (c *Config) buildConnectionString() (string, DatabaseType, error) {
	dbType := c.detectDatabaseType()

	switch dbType {
	case SQLite:
		if _, err := os.Stat(c.Database); os.IsNotExist(err) {
			return "", dbType, fmt.Errorf("sqlite file does not exist: %s", c.Database)
		}
		return c.Database, dbType, nil

	case PostgreSQL:
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

	case MySQL:
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

func (c *Config) connect() (*sql.DB, DatabaseType, error) {
	connStr, dbType, err := c.buildConnectionString()
	if err != nil {
		return nil, dbType, err
	}

	var driverName string
	switch dbType {
	case SQLite:
		driverName = "sqlite3"
	case PostgreSQL:
		driverName = "postgres"
	case MySQL:
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
