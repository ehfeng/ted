package main

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// database functions

// getShortestLookupKey returns the best lookup key for a table by considering
// the primary key and all suitable unique constraints, ranking by:
// - fewest columns
// - smallest estimated total byte width
// For PostgreSQL, NULLS NOT DISTINCT is supported so nullability is not a filter.
// For SQLite/MySQL, require all unique index columns to be NOT NULL.
func getShortestLookupKey(db *sql.DB, dbType DatabaseType, tableName string) ([]string, error) {
	type candidate struct {
		name    string
		cols    []string
		numCols int
		totalSz int
		isPK    bool
	}

	sizeOf := func(typ string, charLen int) int {
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

	candidates := []candidate{}

	switch dbType {
	case SQLite:
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

	case PostgreSQL:
		// Column metadata for sizing
		colType := map[string]string{}
		colLen := map[string]int{}
		ctRows, _ := db.Query(`SELECT column_name, data_type, COALESCE(character_maximum_length, -1)
                                FROM information_schema.columns WHERE table_name = $1`, tableName)
		if ctRows != nil {
			defer ctRows.Close()
			for ctRows.Next() {
				var cname, dtype string
				var clen int
				if err := ctRows.Scan(&cname, &dtype, &clen); err == nil {
					colType[cname] = dtype
					colLen[cname] = clen
				}
			}
		}
		// Primary key
		pkRows, _ := db.Query(`SELECT a.attname
                                FROM pg_index i
                                JOIN pg_class c ON c.oid = i.indrelid
                                JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE
                                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                                WHERE c.relname = $1 AND i.indisprimary
                                ORDER BY k.ord`, tableName)
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
		// Unique non-primary indexes
		uRows, err := db.Query(`SELECT i.indexname,
                                       array_agg(a.attname ORDER BY k.ord) as columns
                                 FROM pg_index idx
                                 JOIN pg_class c ON c.oid = idx.indrelid
                                 JOIN pg_class i ON i.oid = idx.indexrelid
                                 JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE
                                 JOIN pg_attribute a ON a.attrelid = idx.indrelid AND a.attnum = k.attnum
                                 WHERE c.relname = $1 AND idx.indisunique AND NOT idx.indisprimary
                                 GROUP BY i.indexname, array_length(idx.indkey, 1)
                                 ORDER BY array_length(idx.indkey, 1), i.indexname`, tableName)
		if err != nil {
			return nil, err
		}
		defer uRows.Close()
		for uRows.Next() {
			var idxName string
			var colArray string
			if err := uRows.Scan(&idxName, &colArray); err != nil {
				continue
			}
			colArray = strings.Trim(colArray, "{}")
			cols := []string{}
			if colArray != "" {
				cols = strings.Split(colArray, ",")
			}
			if len(cols) == 0 {
				continue
			}
			total := 0
			for _, c := range cols {
				total += sizeOf(colType[c], colLen[c])
			}
			candidates = append(candidates, candidate{name: idxName, cols: cols, numCols: len(cols), totalSz: total})
		}

	case MySQL:
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

	default:
		return []string{}, nil
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

// quoteIdent safely quotes an identifier (table/column) for the target DB.
// Attempts to minimize quoting by returning the identifier unquoted when it is
// obviously safe to do so:
// - comprised of lowercase letters, digits, and underscores
// - does not start with a digit
// - not a common SQL reserved keyword
// Otherwise it applies database-appropriate quoting with escaping.
func quoteIdent(dbType DatabaseType, ident string) string {
	// Fast-path: return plain if it's clearly safe to be unquoted
	if isSafeUnquotedIdent(ident) {
		return ident
	}

	switch dbType {
	case MySQL:
		// Escape backticks by doubling them
		escaped := strings.ReplaceAll(ident, "`", "``")
		return "`" + escaped + "`"
	case PostgreSQL, SQLite, DuckDB:
		// Escape double quotes by doubling them
		escaped := strings.ReplaceAll(ident, "\"", "\"\"")
		return "\"" + escaped + "\""
	default:
		escaped := strings.ReplaceAll(ident, "\"", "\"\"")
		return "\"" + escaped + "\""
	}
}

// isSafeUnquotedIdent returns true if ident can be used without quotes in a
// portable way across supported databases (lowercase [a-z_][a-z0-9_]* and not a
// common reserved keyword).
func isSafeUnquotedIdent(ident string) bool {
	if ident == "" {
		return false
	}
	// First char must be lowercase letter or underscore
	c0 := ident[0]
	if !((c0 >= 'a' && c0 <= 'z') || c0 == '_') {
		return false
	}
	// Remaining chars must be lowercase letters, digits, or underscore
	for i := 1; i < len(ident); i++ {
		c := ident[i]
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
			return false
		}
	}
	// Avoid common reserved keywords
	if _, ok := commonReservedIdents[ident]; ok {
		return false
	}
	return true
}

// Small, conservative set of common SQL reserved keywords to avoid unquoted.
var commonReservedIdents = map[string]struct{}{
	// DML/DDL
	"select": {}, "insert": {}, "update": {}, "delete": {}, "into": {}, "values": {},
	"create": {}, "alter": {}, "drop": {}, "table": {}, "index": {}, "view": {},
	// Clauses
	"from": {}, "where": {}, "group": {}, "order": {}, "by": {}, "having": {},
	"limit": {}, "offset": {}, "join": {}, "inner": {}, "left": {}, "right": {}, "full": {}, "outer": {},
	// Operators/Predicates
	"and": {}, "or": {}, "not": {}, "in": {}, "is": {}, "like": {}, "between": {}, "exists": {},
	// Literals
	"null": {}, "true": {}, "false": {},
	// Misc
	"as": {}, "on": {},
}

// quoteQualified splits on '.' and quotes each identifier part independently.
func quoteQualified(dbType DatabaseType, qualified string) string {
	parts := strings.Split(qualified, ".")
	for i, p := range parts {
		parts[i] = quoteIdent(dbType, p)
	}
	return strings.Join(parts, ".")
}
