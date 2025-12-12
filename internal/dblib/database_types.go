package dblib

import (
	"database/sql"
)

// should be configurable
const NullGlyph = "\\0"
const NullDisplay = "null"
const EmptyDisplay = "·"
const EmptyCellValue = '\000'

type DatabaseType int

const (
	SQLite DatabaseType = iota
	PostgreSQL
	MySQL
	DuckDB
	Clickhouse
	Snowflake
	Cockroach
	BigQuery
	Redshift
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

// Table represents a base table referenced by a relation
type Table struct {
	Name       string
	Key        []int // index into Relation.Columns for key columns
	References []Reference
}

// Reference represents a foreign key relationship
type Reference struct {
	Table   string            // foreign table name
	Columns map[string]string // local column name -> foreign column name
}

// database: table, attribute, record
// sheet: sheet, column, row
// row id should be file line number, different than lookup key
type Relation struct {
	DB      *sql.DB
	DBType  DatabaseType
	handler DatabaseHandler // database-specific operations handler

	// Name metadata - exported for access from main package
	Name         string
	IsView       bool              // true if this is a view, false if table
	IsCustomSQL  bool              // true if this is custom SQL, false if table/view
	SQLStatement string            // stores original SQL for custom SQL display
	Tables       map[string]*Table // base tables referenced (for views)
	Columns      []Column          // ordered columns (renamed from Attribute)
	ColumnIndex  map[string]int    // column name -> column index
	Key          []int             // index into Columns for key columns
	References   []Reference       // references
}

// Column represents a column in a relation (renamed from Attribute)
type Column struct {
	Name           string
	Type           string
	Nullable       bool
	Table          string   // blank if derived/computed, base table name if passthrough
	BaseColumn     string   // original column name in base table (if passthrough)
	Reference      int      // index into Table.References, -1 if not a foreign key column
	Generated      bool     // if computed column, read-only
	EnumValues     []string // for ENUM types, stores allowed values
	CustomTypeName string   // for custom types (PostgreSQL), stores the type name
}

// Attribute is deprecated - use Column instead
// Kept for backward compatibility during migration
type Attribute = Column

type SortColumn struct {
	Name string
	Asc  bool
}

func (sc SortColumn) String(scrollDown bool) string {
	if sc.Asc {
		if scrollDown {
			return sc.Name + " ASC"
		} else {
			return sc.Name + " DESC"
		}
	} else {
		if scrollDown {
			return sc.Name + " ASC"
		} else {
			return sc.Name + " DESC"
		}
	}
}

// DisplayColumn represents a display column with name and width (for UI)
type DisplayColumn struct {
	Name     string
	Width    int
	IsKey    bool
	Editable bool
}
