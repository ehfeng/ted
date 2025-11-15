package main

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// getShortestLookupKeyMySQL returns the best lookup key for a MySQL table.
func getShortestLookupKeyMySQL(db *sql.DB, tableName string, sizeOf func(string, int) int) ([]string, error) {
	type candidate struct {
		name    string
		cols    []string
		numCols int
		totalSz int
		isPK    bool
	}

	// Column metadata
	colType := map[string]string{}
	colLen := map[string]int{}
	notNull := map[string]bool{}
	ctRows, _ := db.Query(`SELECT column_name, data_type, COALESCE(character_maximum_length, -1), is_nullable
                                FROM information_schema.columns WHERE table_name = ?`, tableName)
	if ctRows != nil {
		defer ctRows.Close()
		for ctRows.Next() {
			var cname, dtype, isNullStr string
			var clen int
			if err := ctRows.Scan(&cname, &dtype, &clen, &isNullStr); err == nil {
				colType[cname] = dtype
				colLen[cname] = clen
				notNull[cname] = strings.ToLower(isNullStr) == "no"
			}
		}
	}
	// Primary key
	pkRows, _ := db.Query(`SELECT column_name
                                FROM information_schema.key_column_usage
                                WHERE table_name = ? AND constraint_name = 'PRIMARY'
                                ORDER BY ordinal_position`, tableName)
	if pkRows != nil {
		pkCols := []string{}
		for pkRows.Next() {
			var c string
			if err := pkRows.Scan(&c); err == nil {
				pkCols = append(pkCols, c)
			}
		}
		pkRows.Close()
		if len(pkCols) > 0 {
			// Primary key is always preferred; return early
			return pkCols, nil
		}
	}

	candidates := []candidate{}

	// Unique non-primary indexes
	rows, err := db.Query(`SELECT index_name, column_name, seq_in_index
                                FROM information_schema.statistics
                                WHERE table_name = ? AND non_unique = 0 AND index_name != 'PRIMARY'
                                ORDER BY index_name, seq_in_index`, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	idxCols := map[string][]string{}
	for rows.Next() {
		var idxName, colName string
		var seq int
		if err := rows.Scan(&idxName, &colName, &seq); err != nil {
			continue
		}
		idxCols[idxName] = append(idxCols[idxName], colName)
	}
	for name, cols := range idxCols {
		if len(cols) == 0 {
			continue
		}
		valid := true
		total := 0
		for _, c := range cols {
			if !notNull[c] {
				valid = false
				break
			}
			total += sizeOf(colType[c], colLen[c])
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

// loadAttributesMySQL loads column attributes for a MySQL table.
func loadAttributesMySQL(db *sql.DB, tableName string) (map[string]Attribute, []string, map[string]int, error) {
	query := `SELECT column_name, data_type, is_nullable
			FROM information_schema.columns
			WHERE table_name = ?
			ORDER BY ordinal_position`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	attributes := make(map[string]Attribute)
	var attributeOrder []string
	attributeIndex := make(map[string]int)

	for rows.Next() {
		var attr Attribute
		attr.Reference = -1 // Initialize to -1 (not a foreign key)
		var nullable string

		err = rows.Scan(&attr.Name, &attr.Type, &nullable)
		if err != nil {
			return nil, nil, nil, err
		}

		attr.Nullable = strings.ToLower(nullable) == "yes"

		idx := len(attributeOrder)
		attributeOrder = append(attributeOrder, attr.Name)
		attributes[attr.Name] = attr
		attributeIndex[attr.Name] = idx
	}

	if err := rows.Err(); err != nil {
		return nil, nil, nil, err
	}

	return attributes, attributeOrder, attributeIndex, nil
}

// loadForeignKeysMySQL loads foreign key constraints for a MySQL table.
func loadForeignKeysMySQL(db *sql.DB, dbType DatabaseType, tableName string, attrIndex map[string]int, attributes map[string]Attribute) ([]Reference, map[string]Attribute, error) {
	var references []Reference
	updatedAttrs := make(map[string]Attribute)
	for k, v := range attributes {
		updatedAttrs[k] = v
	}

	// Use information_schema to identify FK columns with ordering
	fkQuery := `
            SELECT CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION,
                   REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL
            ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION`
	fkRows, err := db.Query(fkQuery, tableName)
	if err != nil {
		return references, updatedAttrs, nil
	}
	defer fkRows.Close()

	type fkCol struct {
		ord      int
		col      string
		refTable string
		refCol   string
	}
	byName := map[string][]fkCol{}
	for fkRows.Next() {
		var cname, col, refTable, refCol string
		var ord int
		if scanErr := fkRows.Scan(&cname, &col, &ord, &refTable, &refCol); scanErr != nil {
			continue
		}
		byName[cname] = append(byName[cname], fkCol{ord: ord, col: col, refTable: refTable, refCol: refCol})
	}

	for _, cols := range byName {
		sort.Slice(cols, func(i, j int) bool { return cols[i].ord < cols[j].ord })
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
				foreignCol := c.refCol
				// If refCol is empty, it references the foreign table's primary key
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

// loadEnumAndCustomTypesMySQL fetches enum values for MySQL columns.
func loadEnumAndCustomTypesMySQL(db *sql.DB, tableName string, attributes map[string]Attribute) (map[string]Attribute, error) {
	updatedAttrs := make(map[string]Attribute)
	for k, v := range attributes {
		updatedAttrs[k] = v
	}

	// MySQL ENUM types are stored in information_schema.columns.column_type
	query := `SELECT column_name, column_type
		          FROM information_schema.columns
		          WHERE table_name = ? AND table_schema = DATABASE()
		          AND data_type = 'enum'`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return updatedAttrs, err
	}
	defer rows.Close()

	for rows.Next() {
		var colName, colType string
		if err := rows.Scan(&colName, &colType); err != nil {
			continue
		}

		// Parse ENUM values from column_type like "enum('value1','value2','value3')"
		enumValues := parseEnumValues(colType)
		if attr, ok := updatedAttrs[colName]; ok {
			attr.EnumValues = enumValues
			updatedAttrs[colName] = attr
		}
	}

	return updatedAttrs, nil
}

// parseEnumValues extracts enum values from MySQL's column_type string
// Example input: "enum('active','inactive','pending')"
// Returns: ["active", "inactive", "pending"]
func parseEnumValues(colType string) []string {
	// Remove "enum(" prefix and ")" suffix
	if !strings.HasPrefix(colType, "enum(") || !strings.HasSuffix(colType, ")") {
		return nil
	}

	inner := colType[5 : len(colType)-1] // Remove "enum(" and ")"

	// Split by comma, handling quoted values
	var values []string
	var current strings.Builder
	inQuote := false
	escaped := false

	for i := 0; i < len(inner); i++ {
		ch := inner[i]

		if escaped {
			current.WriteByte(ch)
			escaped = false
			continue
		}

		if ch == '\\' {
			escaped = true
			continue
		}

		if ch == '\'' {
			if inQuote {
				// End of quoted value
				values = append(values, current.String())
				current.Reset()
				inQuote = false
			} else {
				// Start of quoted value
				inQuote = true
			}
			continue
		}

		if inQuote {
			current.WriteByte(ch)
		}
	}

	return values
}
