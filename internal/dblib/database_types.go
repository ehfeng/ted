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

type Reference struct {
	ForeignTable   *Relation
	ForeignColumns map[int]string // attr index -> foreign column
}

// database: table, attribute, record
// sheet: sheet, column, row
// row id should be file line number, different than lookup key
type Relation struct {
	DB     *sql.DB
	DBType DatabaseType

	// Name metadata - exported for access from main package
	Name           string
	Key            []string
	Attributes     map[string]Attribute // attribute name -> attribute
	AttributeOrder []string             // attribute name order
	AttributeIndex map[string]int       // attribute name -> attribute index
	References     []Reference          // references
}

type Attribute struct {
	Name           string
	Type           string
	Nullable       bool
	Reference      int      // reference index, -1 if not a foreign key column
	Generated      bool     // TODO if computed column, read-only
	EnumValues     []string // for ENUM types, stores allowed values
	CustomTypeName string   // for custom types (PostgreSQL), stores the type name
}

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

// Column represents a display column with name and width
type Column struct {
	Name  string
	Width int
}
