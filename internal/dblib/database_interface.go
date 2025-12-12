package dblib

import (
	"database/sql"
	"fmt"
)

// DatabaseHandler defines database-specific operations for a particular database type.
// Each database backend (MySQL, PostgreSQL, SQLite, DuckDB) implements this interface
// to provide database-specific schema introspection, key selection, and utility methods.
//
// The interface enables clean separation between database-agnostic logic (in NewRelation)
// and database-specific implementation details. This makes it easier to add new database
// backends, test database-specific code, and maintain consistency across implementations.
//
// Example: Adding a new database backend
//
//	type NewDBHandler struct{}
//
//	func (h *NewDBHandler) CheckIsView(db *sql.DB, relationName string) (bool, error) {
//	    // Query database-specific system tables
//	    return false, nil
//	}
//	// ... implement remaining interface methods
//
//	// Register in NewDatabaseHandler factory
type DatabaseHandler interface {
	// Schema introspection methods

	// CheckIsView returns true if the named relation is a view, false if it's a table.
	// Returns an error if the relation doesn't exist or cannot be queried.
	CheckIsView(db *sql.DB, relationName string) (bool, error)

	// LoadColumns loads column metadata for a table or view.
	// Returns:
	//   - columns: slice of Column structs with name, type, nullable, etc.
	//   - columnIndex: map from column name to index in columns slice
	//   - error: if table doesn't exist or query fails
	LoadColumns(db *sql.DB, tableName string) ([]Column, map[string]int, error)

	// LoadForeignKeys loads foreign key constraints for a table.
	// Takes existing columns and columnIndex to update reference information.
	// Returns:
	//   - references: slice of Reference structs describing FK relationships
	//   - updated columns: columns with Reference field populated
	//   - error: if query fails
	LoadForeignKeys(db *sql.DB, dbType DatabaseType, tableName string,
		columnIndex map[string]int, columns []Column) ([]Reference, []Column, error)

	// LoadEnumAndCustomTypes fetches enum values and custom type information for columns.
	// For databases with ENUM types (PostgreSQL, MySQL), populates the Enum field.
	// For databases without native ENUMs (SQLite), returns columns unchanged.
	// Returns updated columns with Enum field populated, or error if query fails.
	LoadEnumAndCustomTypes(db *sql.DB, tableName string, columns []Column) ([]Column, error)

	// GetViewDefinition retrieves the SQL definition of a view.
	// Returns the CREATE VIEW statement or equivalent SQL text.
	// Returns an error if the view doesn't exist or cannot be accessed.
	GetViewDefinition(db *sql.DB, viewName string) (string, error)

	// Key selection methods

	// GetBestKey identifies the best key column(s) for a relation using database system tables.
	// Ranking preferences (in order of priority):
	//   1. Primary keys over unique constraints (with NOT NULL or NULLS NOT DISTINCT)
	//   2. Fewer columns over more columns
	//   3. Shorter columns over longer columns (by estimated byte width)
	//   4. Earlier columns over later columns in the table definition
	//
	// Returns the column names comprising the best key, or empty slice if no suitable key exists.
	GetBestKey(db *sql.DB, tableName string) ([]string, error)

	// GetShortestLookupKey returns the best lookup key for a table by considering
	// the primary key and all suitable unique constraints, ranking by:
	//   - fewest columns
	//   - smallest estimated total byte width
	//
	// For PostgreSQL, NULLS NOT DISTINCT is supported so nullability is not a filter.
	// For SQLite/MySQL, requires all unique index columns to be NOT NULL.
	//
	// Returns the column names comprising the shortest lookup key, or empty slice as fallback.
	GetShortestLookupKey(db *sql.DB, tableName string) ([]string, error)

	// Utility methods

	// QuoteIdent quotes an identifier (table name, column name, etc.) for safe use in SQL.
	// Different databases use different quoting characters:
	//   - MySQL: backticks `identifier`
	//   - PostgreSQL, SQLite, DuckDB: double quotes "identifier"
	QuoteIdent(ident string) string

	// Placeholder returns the parameter placeholder for position i (1-indexed).
	// Different databases use different placeholder styles:
	//   - PostgreSQL: $1, $2, $3, ... (positional, numbered)
	//   - MySQL, SQLite, DuckDB: ?, ?, ?, ... (positional, not numbered)
	//
	// Example usage:
	//   for i := 1; i <= 3; i++ {
	//       placeholders = append(placeholders, handler.Placeholder(i))
	//   }
	//   // PostgreSQL: ["$1", "$2", "$3"]
	//   // MySQL:      ["?", "?", "?"]
	Placeholder(position int) string
}

// NewDatabaseHandler creates a DatabaseHandler for the given database type.
// Returns an error if the database type is not supported or not yet implemented.
//
// Currently supported database types:
//   - MySQL
//   - PostgreSQL
//   - SQLite
//   - DuckDB
//
// Example usage:
//
//	handler, err := NewDatabaseHandler(dbType)
//	if err != nil {
//	    return fmt.Errorf("unsupported database: %w", err)
//	}
//	isView, err := handler.CheckIsView(db, "users")
func NewDatabaseHandler(dbType DatabaseType) (DatabaseHandler, error) {
	switch dbType {
	case MySQL:
		return &MySQLHandler{}, nil
	case PostgreSQL:
		return &PostgresHandler{}, nil
	case SQLite:
		return &SQLiteHandler{}, nil
	case DuckDB:
		return &DuckDBHandler{}, nil
	default:
		return nil, fmt.Errorf("unsupported database type: %v", dbType)
	}
}
