package main

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// database: table, attribute, record
// sheet: sheet, column, row
// row id should be file line number, different than lookup key
type Relation struct {
	DB     *sql.DB
	DBType DatabaseType

	// name metadata
	name           string
	key            []string
	attributes     map[string]Attribute
	attributeOrder []string
	attributeIndex map[string]int
	// { foreigntable: { attr index: foreign column } }
	references map[string]map[int]string
}

type Attribute struct {
	Name      string
	Type      string
	Nullable  bool
	Reference string // foreign table name
	Generated bool   // TODO if computed column, read-only
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

// TODO
// direction is always secondarily sorted by key cols
// `select col, ... from tbl where col > ?, ... order by sortCol, keyCol, ...`
// for initial load, params are nil
func selectQuery(dbType DatabaseType, tableName string, columns []string, sortCol *SortColumn, keyCols []string, hasParams, inclusive, scrollDown bool) (string, error) {
	if len(keyCols) == 0 {
		panic("keyCols is empty")
	}
	length := 7 // "select from where order by"
	length += len(tableName)
	for _, col := range columns {
		length += len(col) + 2
	}
	for _, col := range keyCols {
		length += len(col)*2 + 9 + 6 // " = ? AND ", ", DESC"
	}
	var sortColString string
	if sortCol != nil {
		sortColString = sortCol.String(scrollDown)
		length += len(sortColString)
	}

	var builder strings.Builder
	builder.Grow(length)

	builder.WriteString("SELECT ")
	builder.WriteString(strings.Join(columns, ", "))
	builder.WriteString(" FROM ")
	builder.WriteString(quoteQualified(dbType, tableName))

	if hasParams {
		builder.WriteString(" WHERE ")
		nextPlaceholder := func(pos int) string {
			if databaseFeatures[dbType].positionalPlaceholder {
				return fmt.Sprintf("$%d", pos)
			}
			return "?"
		}
		for i, col := range keyCols {
			if i > 0 {
				builder.WriteString(" AND ")
			}
			builder.WriteString(quoteIdent(dbType, col))
			if scrollDown {
				if inclusive {
					builder.WriteString(" >= ")
				} else {
					builder.WriteString(" > ")
				}
			} else {
				if inclusive {
					builder.WriteString(" <= ")
				} else {
					builder.WriteString(" < ")
				}
			}
			builder.WriteString(nextPlaceholder(i + 1))
		}
	}
	builder.WriteString(" ORDER BY ")
	if sortCol != nil {
		builder.WriteString(sortColString)
		builder.WriteString(", ")
	}
	for i, col := range keyCols {
		sc := SortColumn{Name: quoteIdent(dbType, col), Asc: scrollDown}
		builder.WriteString(sc.String(scrollDown))
		if i < len(keyCols)-1 {
			builder.WriteString(", ")
		}
	}
	return builder.String(), nil
}

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

func NewRelation(db *sql.DB, dbType DatabaseType, tableName string) (*Relation, error) {
	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

	wrapErr := func(err error) (*Relation, error) {
		return nil, fmt.Errorf("failed to load table schema: %w", err)
	}

	relation := &Relation{
		DB:             db,
		DBType:         dbType,
		name:           tableName,
		attributes:     make(map[string]Attribute),
		attributeIndex: make(map[string]int),
		references:     make(map[string]map[int]string),
	}

	var (
		query string
		args  []interface{}
	)

	switch dbType {
	case SQLite:
		query = fmt.Sprintf("PRAGMA table_info(%s)", relation.name)
	case PostgreSQL:
		query = `SELECT column_name, data_type, is_nullable
				FROM information_schema.columns
				WHERE table_name = $1
				ORDER BY ordinal_position`
		args = append(args, relation.name)
	case MySQL:
		query = `SELECT column_name, data_type, is_nullable
				FROM information_schema.columns
				WHERE table_name = ?
				ORDER BY ordinal_position`
		args = append(args, relation.name)
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return wrapErr(err)
	}
	defer rows.Close()

	relation.attributes = make(map[string]Attribute)
	relation.attributeOrder = relation.attributeOrder[:0]
	relation.attributeIndex = make(map[string]int)

	var primaryKeyColumns []string
	for rows.Next() {
		var attr Attribute
		var nullable string

		switch dbType {
		case SQLite:
			var cid int
			var dfltValue sql.NullString
			var pk int
			err = rows.Scan(&cid, &attr.Name, &attr.Type, &nullable, &dfltValue, &pk)
			attr.Nullable = nullable != "1"
			if pk == 1 {
				primaryKeyColumns = append(primaryKeyColumns, attr.Name)
			}
		case PostgreSQL, MySQL:
			err = rows.Scan(&attr.Name, &attr.Type, &nullable)
			attr.Nullable = strings.ToLower(nullable) == "yes"
		}

		if err != nil {
			return wrapErr(err)
		}

		idx := len(relation.attributeOrder)
		relation.attributeOrder = append(relation.attributeOrder, attr.Name)
		relation.attributes[attr.Name] = attr
		relation.attributeIndex[attr.Name] = idx
	}

	if err := rows.Err(); err != nil {
		return wrapErr(err)
	}

	// Consolidated lookup key selection: choose shortest lookup key
	lookupCols, err := getShortestLookupKey(db, dbType, relation.name)
	if err != nil {
		return wrapErr(err)
	}
	primaryKeyColumns = lookupCols

	// TODO if lookup key not found, use databaseFeature.systemId if available

	// If not nullable unique constraint is found, error
	if len(primaryKeyColumns) == 0 {
		return wrapErr(fmt.Errorf("no primary key found"))
	}
	relation.key = make([]string, len(primaryKeyColumns))
	copy(relation.key, primaryKeyColumns)

	// Build a quick name->index map for attribute lookup
	attrIndex := relation.attributeIndex

	// Populate foreign key column indices (supports multicolumn FKs)
	switch dbType {
	case SQLite:
		// PRAGMA foreign_key_list returns one row per referencing column
		// cols: id, seq, table, from, to, on_update, on_delete, match
		fkRows, err := db.Query(fmt.Sprintf("PRAGMA foreign_key_list(%s)", relation.name))
		if err == nil {
			type fkCol struct {
				seq      int
				col      string
				toCol    string
				refTable string
			}
			byID := map[int][]fkCol{}
			for fkRows.Next() {
				var id, seq int
				var refTable, fromCol, toCol, onUpd, onDel, match string
				if scanErr := fkRows.Scan(&id, &seq, &refTable, &fromCol, &toCol, &onUpd, &onDel, &match); scanErr != nil {
					continue
				}
				byID[id] = append(byID[id], fkCol{seq: seq, col: fromCol, toCol: toCol, refTable: refTable})
			}
			fkRows.Close()
			// Build references map and update attributes
			for _, cols := range byID {
				sort.Slice(cols, func(i, j int) bool { return cols[i].seq < cols[j].seq })
				if len(cols) == 0 {
					continue
				}
				refTable := cols[0].refTable
				if relation.references[refTable] == nil {
					relation.references[refTable] = make(map[int]string)
				}
				for _, c := range cols {
					if idx, ok := attrIndex[c.col]; ok {
						relation.references[refTable][idx] = c.toCol
						// Update attribute with reference info
						if attr, exists := relation.attributes[c.col]; exists {
							attr.Reference = refTable
							relation.attributes[c.col] = attr
						}
					}
				}
			}
		}

	case PostgreSQL:
		// Use pg_catalog to get correct column order within FK
		schema := "public"
		rel := relation.name
		if dot := strings.IndexByte(rel, '.'); dot != -1 {
			schema = rel[:dot]
			rel = rel[dot+1:]
		}
		fkQuery := `
            SELECT con.oid::text AS id, att.attname AS col, u.ord AS ord,
                   frel.relname AS ref_table, fatt.attname AS ref_col
            FROM pg_constraint con
            JOIN unnest(con.conkey) WITH ORDINALITY AS u(attnum, ord) ON true
            JOIN pg_class rel ON rel.oid = con.conrelid
            JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
            JOIN pg_attribute att ON att.attrelid = rel.oid AND att.attnum = u.attnum
            JOIN pg_class frel ON frel.oid = con.confrelid
            JOIN unnest(con.confkey) WITH ORDINALITY AS fu(attnum, ord) ON fu.ord = u.ord
            JOIN pg_attribute fatt ON fatt.attrelid = frel.oid AND fatt.attnum = fu.attnum
            WHERE con.contype = 'f' AND rel.relname = $1 AND nsp.nspname = $2
            ORDER BY con.oid, u.ord`
		fkRows, err := db.Query(fkQuery, rel, schema)
		if err == nil {
			type fkCol struct {
				ord      int
				col      string
				refTable string
				refCol   string
			}
			byID := map[string][]fkCol{}
			for fkRows.Next() {
				var id, col, refTable, refCol string
				var ord int
				if scanErr := fkRows.Scan(&id, &col, &ord, &refTable, &refCol); scanErr != nil {
					continue
				}
				byID[id] = append(byID[id], fkCol{ord: ord, col: col, refTable: refTable, refCol: refCol})
			}
			fkRows.Close()
			for _, cols := range byID {
				sort.Slice(cols, func(i, j int) bool { return cols[i].ord < cols[j].ord })
				if len(cols) == 0 {
					continue
				}
				refTable := cols[0].refTable
				if relation.references[refTable] == nil {
					relation.references[refTable] = make(map[int]string)
				}
				for _, c := range cols {
					if idx, ok := attrIndex[c.col]; ok {
						relation.references[refTable][idx] = c.refCol
						// Update attribute with reference info
						if attr, exists := relation.attributes[c.col]; exists {
							attr.Reference = refTable
							relation.attributes[c.col] = attr
						}
					}
				}
			}
		}

	case MySQL:
		// Use information_schema to identify FK columns with ordering
		fkQuery := `
            SELECT CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION,
                   REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL
            ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION`
		fkRows, err := db.Query(fkQuery, relation.name)
		if err == nil {
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
			fkRows.Close()
			for _, cols := range byName {
				sort.Slice(cols, func(i, j int) bool { return cols[i].ord < cols[j].ord })
				if len(cols) == 0 {
					continue
				}
				refTable := cols[0].refTable
				if relation.references[refTable] == nil {
					relation.references[refTable] = make(map[int]string)
				}
				for _, c := range cols {
					if idx, ok := attrIndex[c.col]; ok {
						relation.references[refTable][idx] = c.refCol
						// Update attribute with reference info
						if attr, exists := relation.attributes[c.col]; exists {
							attr.Reference = refTable
							relation.attributes[c.col] = attr
						}
					}
				}
			}
		}
	}

	return relation, nil
}

// BuildUpdatePreview constructs a SQL UPDATE statement as a string with literal
// values inlined for preview purposes. It mirrors UpdateDBValue but does not
// execute any SQL. Intended only for UI preview.
func (rel *Relation) BuildUpdatePreview(records [][]interface{}, rowIdx int, colName string, newValue string) string {
	if rowIdx < 0 || rowIdx >= len(records) || len(rel.key) == 0 {
		return ""
	}

	// Convert raw text to DB-typed value (mirrors UpdateDBValue's toDBValue)
	toDBValue := func(colName, raw string) interface{} {
		attrType := ""
		if attr, ok := rel.attributes[colName]; ok {
			attrType = strings.ToLower(attr.Type)
		}
		if raw == "\\N" {
			return nil
		}
		t := attrType
		switch {
		case strings.Contains(t, "bool"):
			lower := strings.ToLower(strings.TrimSpace(raw))
			if lower == "1" || lower == "true" || lower == "t" {
				return true
			}
			if lower == "0" || lower == "false" || lower == "f" {
				return false
			}
			return raw
		case strings.Contains(t, "int"):
			if v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil {
				return v
			}
			return raw
		case strings.Contains(t, "real") || strings.Contains(t, "double") || strings.Contains(t, "float") || strings.Contains(t, "numeric") || strings.Contains(t, "decimal"):
			if v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64); err == nil {
				return v
			}
			return raw
		default:
			return raw
		}
	}

	// Render a literal value for preview (no placeholders)
	literal := func(val interface{}, attrType string) string {
		if val == nil {
			return "NULL"
		}
		at := strings.ToLower(attrType)
		switch v := val.(type) {
		case bool:
			if rel.DBType == PostgreSQL {
				if v {
					return "TRUE"
				}
				return "FALSE"
			}
			if v {
				return "1"
			}
			return "0"
		case int64:
			return strconv.FormatInt(v, 10)
		case float64:
			return strconv.FormatFloat(v, 'f', -1, 64)
		case []byte:
			s := string(v)
			s = strings.ReplaceAll(s, "'", "''")
			return "'" + s + "'"
		case string:
			// For non-numeric types, quote and escape
			if strings.Contains(at, "int") || strings.Contains(at, "real") || strings.Contains(at, "double") || strings.Contains(at, "float") || strings.Contains(at, "numeric") || strings.Contains(at, "decimal") {
				return v
			}
			s := strings.ReplaceAll(v, "'", "''")
			return "'" + s + "'"
		default:
			// Fallback to string formatting quoted
			s := strings.ReplaceAll(fmt.Sprintf("%v", v), "'", "''")
			return "'" + s + "'"
		}
	}

	// Where clause with literal values
	whereParts := make([]string, 0, len(rel.key))
	for _, lookupKeyCol := range rel.key {
		qKeyName := quoteIdent(rel.DBType, lookupKeyCol)
		idx, ok := rel.attributeIndex[lookupKeyCol]
		if !ok || rowIdx >= len(records) {
			return ""
		}
		row := records[rowIdx]
		if idx < 0 || idx >= len(row) {
			return ""
		}
		attr, ok := rel.attributes[lookupKeyCol]
		if !ok {
			return ""
		}
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", qKeyName, literal(row[idx], attr.Type)))
	}

	// SET clause literal
	targetAttrType := ""
	if attr, ok := rel.attributes[colName]; ok {
		targetAttrType = attr.Type
	}
	valueArg := toDBValue(colName, newValue)
	quotedTarget := quoteIdent(rel.DBType, colName)
	setClause := fmt.Sprintf("%s = %s", quotedTarget, literal(valueArg, targetAttrType))

	returningCols := make([]string, len(rel.attributeOrder))
	for i, name := range rel.attributeOrder {
		returningCols[i] = quoteIdent(rel.DBType, name)
	}
	if len(rel.key) == 1 {
		if rel.key[0] == "rowid" {
			returningCols = append(returningCols, "rowid")
		}
		if rel.key[0] == "ctid" {
			returningCols = append(returningCols, "ctid")
		}
	}
	returning := strings.Join(returningCols, ", ")

	quotedTable := quoteQualified(rel.DBType, rel.name)
	useReturning := databaseFeatures[rel.DBType].returning
	if useReturning {
		return fmt.Sprintf("UPDATE %s SET %s WHERE %s RETURNING %s", quotedTable, setClause, strings.Join(whereParts, " AND "), returning)
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s", quotedTable, setClause, strings.Join(whereParts, " AND "))
}

// UpdateDBValue updates a single cell in the underlying database using the
// relation's lookup key columns to identify the row. It returns the refreshed
// row values ordered by relation.attributeOrder. If no row is updated, returns
// an error.
func (rel *Relation) UpdateDBValue(records [][]interface{}, rowIdx int, colName string, newValue string) ([]interface{}, error) {
	if rowIdx < 0 || rowIdx >= len(records) {
		return nil, fmt.Errorf("index out of range")
	}
	if len(rel.key) == 0 {
		return nil, fmt.Errorf("no lookup key configured")
	}

	// Convert string to appropriate DB value
	toDBValue := func(colName, raw string) interface{} {
		attrType := ""
		if attr, ok := rel.attributes[colName]; ok {
			attrType = strings.ToLower(attr.Type)
		}
		if raw == "\\N" {
			return nil
		}
		t := attrType
		switch {
		case strings.Contains(t, "bool"):
			lower := strings.ToLower(strings.TrimSpace(raw))
			if lower == "1" || lower == "true" || lower == "t" {
				return true
			}
			if lower == "0" || lower == "false" || lower == "f" {
				return false
			}
			return raw
		case strings.Contains(t, "int"):
			if v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil {
				return v
			}
			return raw
		case strings.Contains(t, "real") || strings.Contains(t, "double") || strings.Contains(t, "float") || strings.Contains(t, "numeric") || strings.Contains(t, "decimal"):
			if v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64); err == nil {
				return v
			}
			return raw
		default:
			return raw
		}
	}

	// Placeholder builder
	placeholder := func(i int) string {
		switch rel.DBType {
		case PostgreSQL:
			return fmt.Sprintf("$%d", i)
		default:
			return "?"
		}
	}

	// Build SET and WHERE clauses and args
	valueArg := toDBValue(colName, newValue)
	keyArgs := make([]interface{}, 0, len(rel.key))
	whereParts := make([]string, 0, len(rel.key))
	for i := range rel.key {
		lookupKeyCol := rel.key[i]
		qKeyName := quoteIdent(rel.DBType, lookupKeyCol)
		var ph string
		if rel.DBType == PostgreSQL {
			ph = placeholder(2 + i)
		} else {
			ph = placeholder(0)
		}
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", qKeyName, ph))
		colIdx, ok := rel.attributeIndex[lookupKeyCol]
		if !ok || rowIdx >= len(records) {
			return nil, fmt.Errorf("lookup key column %s not found in records", lookupKeyCol)
		}
		row := records[rowIdx]
		if colIdx < 0 || colIdx >= len(row) {
			return nil, fmt.Errorf("lookup key column %s not loaded", lookupKeyCol)
		}
		keyArgs = append(keyArgs, row[colIdx])
	}

	// SET clause placeholder
	var setClause string
	quotedTarget := quoteIdent(rel.DBType, colName)
	if rel.DBType == PostgreSQL {
		setClause = fmt.Sprintf("%s = %s", quotedTarget, placeholder(1))
	} else {
		setClause = fmt.Sprintf("%s = %s", quotedTarget, placeholder(0))
	}

	returningCols := make([]string, len(rel.attributeOrder))
	for i, name := range rel.attributeOrder {
		returningCols[i] = quoteIdent(rel.DBType, name)
	}

	if len(rel.key) == 1 {
		if rel.key[0] == "rowid" {
			returningCols = append(returningCols, "rowid")
		}
		if rel.key[0] == "ctid" {
			returningCols = append(returningCols, "ctid")
		}
	}
	returning := strings.Join(returningCols, ", ")

	// Build full query
	var query string
	useReturning := databaseFeatures[rel.DBType].returning
	quotedTable := quoteQualified(rel.DBType, rel.name)
	if useReturning {
		query = fmt.Sprintf("UPDATE %s SET %s WHERE %s RETURNING %s", quotedTable, setClause, strings.Join(whereParts, " AND "), returning)
		// Combine args: value + keys
		args := make([]interface{}, 0, 1+len(keyArgs))
		args = append(args, valueArg)
		args = append(args, keyArgs...)

		// Scan into pointers to capture returned values
		rowVals := make([]interface{}, len(returningCols))
		scanArgs := make([]interface{}, len(returningCols))
		for i := range rowVals {
			scanArgs[i] = &rowVals[i]
		}

		if err := rel.DB.QueryRow(query, args...).Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("update failed: %w", err)
		}
		return rowVals, nil
	}

	// For database servers that don't support RETURNING, use a transaction
	// to perform the UPDATE followed by a SELECT of the updated row.
	query = fmt.Sprintf("UPDATE %s SET %s WHERE %s", quotedTable, setClause, strings.Join(whereParts, " AND "))

	// Begin transaction
	tx, err := rel.DB.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin tx failed: %w", err)
	}

	// Execute update inside the transaction
	args := make([]interface{}, 0, 1+len(keyArgs))
	args = append(args, valueArg)
	args = append(args, keyArgs...)
	res, err := tx.Exec(query, args...)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("update failed: %w", err)
	}
	if ra, _ := res.RowsAffected(); ra == 0 {
		_ = tx.Rollback()
		return nil, fmt.Errorf("no rows updated")
	}

	// Re-select the updated row within the same transaction
	selQuery := fmt.Sprintf("SELECT %s FROM %s WHERE %s", returning, quotedTable, strings.Join(whereParts, " AND "))
	row := tx.QueryRow(selQuery, keyArgs...)
	rowVals := make([]interface{}, len(returningCols))
	scanArgs := make([]interface{}, len(rowVals))
	for i := range rowVals {
		scanArgs[i] = &rowVals[i]
	}
	if err := row.Scan(scanArgs...); err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit failed: %w", err)
	}
	return rowVals, nil
}

// QueryRows executes a SELECT for the given columns and clauses, returning the
// resulting row cursor. Callers are responsible for closing the returned rows.
func (rel *Relation) QueryRows(columns []string, sortCol *SortColumn, params []interface{}, inclusive, scrollDown bool) (*sql.Rows, error) {
	query, err := selectQuery(rel.DBType, rel.name, columns, sortCol, rel.key, len(params) > 0, inclusive, scrollDown)
	if err != nil {
		return nil, err
	}
	return rel.DB.Query(query, params...)
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
