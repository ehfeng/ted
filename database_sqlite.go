package main

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// getShortestLookupKeySQLite returns the best lookup key for a SQLite table.
func getShortestLookupKeySQLite(db *sql.DB, tableName string, sizeOf func(string, int) int) ([]string, error) {
	type candidate struct {
		name    string
		cols    []string
		numCols int
		totalSz int
		isPK    bool
	}

	// Column metadata, including PK info
	notNull := map[string]bool{}
	colType := map[string]string{}
	// Collect PK columns with correct order using pk ordinal in PRAGMA table_info
	type pkEntry struct {
		ord  int
		name string
	}
	var pkEntries []pkEntry
	ti, _ := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if ti != nil {
		defer ti.Close()
		for ti.Next() {
			var cid int
			var cname, ctype string
			var notnullStr string
			var dflt sql.NullString
			var pk int
			if err := ti.Scan(&cid, &cname, &ctype, &notnullStr, &dflt, &pk); err == nil {
				notNull[cname] = (notnullStr == "1")
				colType[cname] = ctype
				if pk > 0 {
					pkEntries = append(pkEntries, pkEntry{ord: pk, name: cname})
				}
			}
		}
	}
	if len(pkEntries) > 0 {
		sort.Slice(pkEntries, func(i, j int) bool { return pkEntries[i].ord < pkEntries[j].ord })
		pkCols := make([]string, 0, len(pkEntries))
		for _, e := range pkEntries {
			pkCols = append(pkCols, e.name)
		}
		// Primary key is always preferred; return early
		return pkCols, nil
	}

	candidates := []candidate{}

	// Unique indexes
	rows, err := db.Query(fmt.Sprintf("PRAGMA index_list(%s)", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			continue
		}
		if unique != 1 || origin == "pk" {
			continue
		}
		ii, err := db.Query(fmt.Sprintf("PRAGMA index_info(%s)", name))
		if err != nil {
			continue
		}
		type idxEntry struct {
			ord  int
			name string
		}
		var idxEntries []idxEntry
		for ii.Next() {
			var seqno, cid int
			var cname string
			if err := ii.Scan(&seqno, &cid, &cname); err == nil {
				idxEntries = append(idxEntries, idxEntry{ord: seqno, name: cname})
			}
		}
		ii.Close()
		if len(idxEntries) == 0 {
			continue
		}
		sort.Slice(idxEntries, func(i, j int) bool { return idxEntries[i].ord < idxEntries[j].ord })
		cols := make([]string, 0, len(idxEntries))
		for _, e := range idxEntries {
			cols = append(cols, e.name)
		}
		// Require NOT NULL for unique in SQLite
		valid := true
		total := 0
		for _, c := range cols {
			if !notNull[c] {
				valid = false
				break
			}
			total += sizeOf(colType[c], -1)
		}
		if valid {
			candidates = append(candidates, candidate{name: name, cols: cols, numCols: len(cols), totalSz: total})
		}
	}

	if len(candidates) == 0 {
		return []string{}, nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].numCols != candidates[j].numCols {
			return candidates[i].numCols < candidates[j].numCols
		}
		if candidates[i].totalSz != candidates[j].totalSz {
			return candidates[i].totalSz < candidates[j].totalSz
		}
		// Prefer primary key on ties
		if candidates[i].isPK != candidates[j].isPK {
			return candidates[i].isPK
		}
		return candidates[i].name < candidates[j].name
	})
	return candidates[0].cols, nil
}

// loadAttributesSQLite loads column attributes for a SQLite table.
func loadAttributesSQLite(db *sql.DB, tableName string) (map[string]Attribute, []string, map[string]int, []string, error) {
	query := fmt.Sprintf("PRAGMA table_info(%s)", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	defer rows.Close()

	attributes := make(map[string]Attribute)
	var attributeOrder []string
	attributeIndex := make(map[string]int)
	var primaryKeyColumns []string

	for rows.Next() {
		var attr Attribute
		attr.Reference = -1 // Initialize to -1 (not a foreign key)
		var nullable string
		var cid int
		var dfltValue sql.NullString
		var pk int

		err = rows.Scan(&cid, &attr.Name, &attr.Type, &nullable, &dfltValue, &pk)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		attr.Nullable = nullable != "1"
		if pk == 1 {
			primaryKeyColumns = append(primaryKeyColumns, attr.Name)
		}

		idx := len(attributeOrder)
		attributeOrder = append(attributeOrder, attr.Name)
		attributes[attr.Name] = attr
		attributeIndex[attr.Name] = idx
	}

	if err := rows.Err(); err != nil {
		return nil, nil, nil, nil, err
	}

	return attributes, attributeOrder, attributeIndex, primaryKeyColumns, nil
}

// loadForeignKeysSQLite loads foreign key constraints for a SQLite table.
func loadForeignKeysSQLite(db *sql.DB, dbType DatabaseType, tableName string, attrIndex map[string]int, attributes map[string]Attribute) ([]Reference, map[string]Attribute, error) {
	var references []Reference
	updatedAttrs := make(map[string]Attribute)
	for k, v := range attributes {
		updatedAttrs[k] = v
	}

	// PRAGMA foreign_key_list returns one row per referencing column
	// cols: id, seq, table, from, to, on_update, on_delete, match
	fkRows, err := db.Query(fmt.Sprintf("PRAGMA foreign_key_list(%s)", tableName))
	if err != nil {
		return references, updatedAttrs, nil
	}
	defer fkRows.Close()

	type fkCol struct {
		seq      int
		col      string
		toCol    string
		refTable string
	}
	byID := map[int][]fkCol{}
	for fkRows.Next() {
		var id, seq int
		var refTable, fromCol, onUpd, onDel, match string
		var toCol sql.NullString
		if scanErr := fkRows.Scan(&id, &seq, &refTable, &fromCol, &toCol, &onUpd, &onDel, &match); scanErr != nil {
			continue
		}
		toColStr := toCol.String
		if !toCol.Valid {
			toColStr = "" // Empty means reference the primary key (implicit)
		}
		col := fkCol{seq: seq, col: fromCol, toCol: toColStr, refTable: refTable}
		byID[id] = append(byID[id], col)
	}

	// Build references slice and update attributes
	for _, cols := range byID {
		sort.Slice(cols, func(i, j int) bool { return cols[i].seq < cols[j].seq })
		if len(cols) == 0 {
			continue
		}
		refTableName := cols[0].refTable
		// Load the foreign table metadata
		foreignRel, err := NewRelation(db, dbType, refTableName)
		if err != nil {
			// If we can't load the foreign table, skip this reference
			fmt.Fprintf(nil, "Warning: failed to load foreign table %s: %v\n", refTableName, err)
			continue
		}
		// Create a new Reference entry
		ref := Reference{
			ForeignTable:   foreignRel,
			ForeignColumns: make(map[int]string),
		}
		for i, c := range cols {
			if idx, ok := attrIndex[c.col]; ok {
				foreignCol := c.toCol
				// If toCol is empty, it references the foreign table's primary key
				if foreignCol == "" && i < len(foreignRel.key) {
					foreignCol = foreignRel.key[i]
				}
				ref.ForeignColumns[idx] = foreignCol
				// Update attribute with reference index
				if attr, exists := updatedAttrs[c.col]; exists {
					attr.Reference = len(references)
					updatedAttrs[c.col] = attr
				}
			}
		}
		references = append(references, ref)
	}

	return references, updatedAttrs, nil
}

// loadEnumAndCustomTypesSQLite is a no-op for SQLite (no native ENUM support).
func loadEnumAndCustomTypesSQLite(db *sql.DB, tableName string, attributes map[string]Attribute) (map[string]Attribute, error) {
	// SQLite doesn't have native ENUM support, but we can check for CHECK constraints
	// that simulate enums. This is a best-effort approach.
	// For now, we'll skip this as it's complex to parse CHECK constraints reliably
	return attributes, nil
}

// sizeOf estimates the byte width of a database column type.
func sizeOf(typ string, charLen int) int {
	t := strings.ToLower(strings.TrimSpace(typ))
	if charLen <= 0 {
		if i := strings.Index(t, "("); i != -1 {
			if j := strings.Index(t[i+1:], ")"); j != -1 {
				if n, err := strconv.Atoi(strings.TrimSpace(t[i+1 : i+1+j])); err == nil {
					charLen = n
				}
			}
		}
	}
	switch {
	case strings.Contains(t, "tinyint"):
		return 1
	case strings.Contains(t, "smallint"):
		return 2
	case t == "int" || strings.Contains(t, "integer"):
		return 4
	case strings.Contains(t, "bigint"):
		return 8
	case strings.Contains(t, "real") || strings.Contains(t, "double") || strings.Contains(t, "float"):
		return 8
	case strings.Contains(t, "bool"):
		return 1
	case strings.Contains(t, "uuid"):
		return 16
	case strings.Contains(t, "date") || strings.Contains(t, "time"):
		return 8
	case strings.Contains(t, "char") || strings.Contains(t, "text") || strings.Contains(t, "clob") || strings.Contains(t, "varchar"):
		if charLen > 0 {
			return charLen
		}
		return 1024 * 1024
	case strings.Contains(t, "decimal") || strings.Contains(t, "numeric"):
		return 16
	case strings.Contains(t, "bytea") || strings.Contains(t, "blob") || strings.Contains(t, "binary"):
		return 1024 * 1024
	default:
		return 8
	}
}
