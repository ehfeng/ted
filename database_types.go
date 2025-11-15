package main

import (
	"database/sql"
)

// should be configurable
const NullGlyph = "\\0"
const NullDisplay = "null"
const EmptyDisplay = "·"

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

	// name metadata
	name           string
	key            []string
	attributes     map[string]Attribute // attribute name -> attribute
	attributeOrder []string             // attribute name order
	attributeIndex map[string]int       // attribute name -> attribute index
	references     []Reference          // references
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
