package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// database: table, attribute, record
// sheet: sheet, column, row
// TODO implement tview.TableContent interface
// instead of T pipe, pipe to file and then allow tview.TableContent to read from file once
// enough rows to fill the terminal height
// row id should be file line number, different than lookup key
type Sheet struct {
	// sheet
	DB       *sql.DB
	DBType   DatabaseType
	StopChan chan struct{} // channel to stop streaming

	// results
	table      string
	lookupKey  []string // empty if no primary key
	attributes []attribute
	uniques    [][]int // TODO unique columns (can be multicolumn)
	// references holds, for each foreign key constraint on this table,
	// the indices of this table's columns participating in that foreign key
	// (multi-column constraints are ordered according to their defined sequence).
	references  [][]int
	filePath    string // path to temporary file containing full dataset
	rowCount    int64
	selectCols  []string
	columnIndex map[string]int
	attrByName  map[string]attribute

	// optional filters
	whereClause string
	orderBy     string
	limit       int
}

// this is a config concept
// width = 0 means column is not selected
type Column struct {
	Name  string
	Width int
}

type attribute struct {
	Name      string
	Type      string // TODO enum, composite types
	Nullable  bool
	Generated bool // TODO if computed column, read-only
}

func NewSheet(db *sql.DB, dbType DatabaseType, tableName string,
	configCols []Column, terminalHeight int, whereClause string, orderBy string, limit int) (*Sheet, [][]interface{}, error) {

	if tableName == "" {
		return nil, nil, fmt.Errorf("table name is required")
	}

	sheet := &Sheet{
		DB:          db,
		DBType:      dbType,
		table:       tableName,
		whereClause: whereClause,
		orderBy:     orderBy,
		limit:       limit,
	}

	if err := loadTableSchema(db, dbType, sheet); err != nil {
		return nil, nil, fmt.Errorf("failed to load table schema: %w", err)
	}

	selected := []string{}
	if configCols == nil {
		for _, attr := range sheet.attributes {
			selected = append(selected, attr.Name)
		}
	} else {
		// Create map of selected column names for efficient lookup
		selectedMap := make(map[string]bool)
		for _, col := range configCols {
			selectedMap[col.Name] = true
		}

		// Add intersection of selectedColumns and lookupKey first
		for _, key := range sheet.lookupKey {
			if selectedMap[key] {
				selected = append(selected, key)
			}
		}

		// Add remaining selected columns that aren't in lookupKey
		for _, col := range configCols {
			isLookupKey := false
			for _, key := range sheet.lookupKey {
				if col.Name == key {
					isLookupKey = true
					break
				}
			}
			if !isLookupKey {
				selected = append(selected, col.Name)
			}
		}
	}
	sheet.selectCols = append([]string(nil), selected...)
	sheet.columnIndex = make(map[string]int, len(sheet.selectCols))
	for idx, name := range sheet.selectCols {
		sheet.columnIndex[name] = idx
	}

	initialData, err := loadTableData(db, sheet, selected, terminalHeight)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load table data: %w", err)
	}
	return sheet, initialData, nil
}

func loadTableSchema(db *sql.DB, dbType DatabaseType, table *Sheet) error {
	var query string

	switch dbType {
	case SQLite:
		query = fmt.Sprintf("PRAGMA table_info(%s)", table.table)
	case PostgreSQL:
		query = `SELECT column_name, data_type, is_nullable 
				FROM information_schema.columns 
				WHERE table_name = $1 
				ORDER BY ordinal_position`
	case MySQL:
		query = `SELECT column_name, data_type, is_nullable 
				FROM information_schema.columns 
				WHERE table_name = ? 
				ORDER BY ordinal_position`
	}

	rows, err := db.Query(query, table.table)
	if err != nil {
		return err
	}
	defer rows.Close()

	var primaryKeyColumns []string
	for rows.Next() {
		var attr attribute
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
			return err
		}

		table.attributes = append(table.attributes, attr)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// Consolidated lookup key selection: choose shortest lookup key
	lookupCols, err := getShortestLookupKey(db, dbType, table.table)
	if err != nil {
		return err
	}
	primaryKeyColumns = lookupCols

	// TODO if lookup key not found, use databaseFeature.systemId if available

	// If not nullable unique constraint is found, error
	if len(primaryKeyColumns) == 0 {
		return fmt.Errorf("no primary key found")
	}
	table.lookupKey = make([]string, len(primaryKeyColumns))
	copy(table.lookupKey, primaryKeyColumns)

	// Build a quick name->index map for attribute lookup
	attrIndex := make(map[string]int, len(table.attributes))
	for i, a := range table.attributes {
		attrIndex[a.Name] = i
	}

	table.attrByName = make(map[string]attribute, len(table.attributes))
	for _, attr := range table.attributes {
		table.attrByName[attr.Name] = attr
	}

	// Populate foreign key column indices (supports multicolumn FKs)
	switch dbType {
	case SQLite:
		// PRAGMA foreign_key_list returns one row per referencing column
		// cols: id, seq, table, from, to, on_update, on_delete, match
		fkRows, err := db.Query(fmt.Sprintf("PRAGMA foreign_key_list(%s)", table.table))
		if err == nil {
			type fkCol struct {
				seq int
				col string
			}
			byID := map[int][]fkCol{}
			for fkRows.Next() {
				var id, seq int
				var refTable, fromCol, toCol, onUpd, onDel, match string
				if scanErr := fkRows.Scan(&id, &seq, &refTable, &fromCol, &toCol, &onUpd, &onDel, &match); scanErr != nil {
					continue
				}
				byID[id] = append(byID[id], fkCol{seq: seq, col: fromCol})
			}
			fkRows.Close()
			// Assemble ordered index slices
			for _, cols := range byID {
				sort.Slice(cols, func(i, j int) bool { return cols[i].seq < cols[j].seq })
				idxs := make([]int, 0, len(cols))
				valid := true
				for _, c := range cols {
					if idx, ok := attrIndex[c.col]; ok {
						idxs = append(idxs, idx)
					} else {
						valid = false
						break
					}
				}
				if valid && len(idxs) > 0 {
					table.references = append(table.references, idxs)
				}
			}
		}

	case PostgreSQL:
		// Use pg_catalog to get correct column order within FK
		schema := "public"
		rel := table.table
		if dot := strings.IndexByte(rel, '.'); dot != -1 {
			schema = rel[:dot]
			rel = rel[dot+1:]
		}
		fkQuery := `
            SELECT con.oid::text AS id, att.attname AS col, u.ord AS ord
            FROM pg_constraint con
            JOIN unnest(con.conkey) WITH ORDINALITY AS u(attnum, ord) ON true
            JOIN pg_class rel ON rel.oid = con.conrelid
            JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
            JOIN pg_attribute att ON att.attrelid = rel.oid AND att.attnum = u.attnum
            WHERE con.contype = 'f' AND rel.relname = $1 AND nsp.nspname = $2
            ORDER BY con.oid, u.ord`
		fkRows, err := db.Query(fkQuery, rel, schema)
		if err == nil {
			type fkCol struct {
				ord int
				col string
			}
			byID := map[string][]fkCol{}
			for fkRows.Next() {
				var id, col string
				var ord int
				if scanErr := fkRows.Scan(&id, &col, &ord); scanErr != nil {
					continue
				}
				byID[id] = append(byID[id], fkCol{ord: ord, col: col})
			}
			fkRows.Close()
			for _, cols := range byID {
				sort.Slice(cols, func(i, j int) bool { return cols[i].ord < cols[j].ord })
				idxs := make([]int, 0, len(cols))
				valid := true
				for _, c := range cols {
					if idx, ok := attrIndex[c.col]; ok {
						idxs = append(idxs, idx)
					} else {
						valid = false
						break
					}
				}
				if valid && len(idxs) > 0 {
					table.references = append(table.references, idxs)
				}
			}
		}

	case MySQL:
		// Use information_schema to identify FK columns with ordering
		fkQuery := `
            SELECT CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL
            ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION`
		fkRows, err := db.Query(fkQuery, table.table)
		if err == nil {
			type fkCol struct {
				ord int
				col string
			}
			byName := map[string][]fkCol{}
			for fkRows.Next() {
				var cname, col string
				var ord int
				if scanErr := fkRows.Scan(&cname, &col, &ord); scanErr != nil {
					continue
				}
				byName[cname] = append(byName[cname], fkCol{ord: ord, col: col})
			}
			fkRows.Close()
			for _, cols := range byName {
				sort.Slice(cols, func(i, j int) bool { return cols[i].ord < cols[j].ord })
				idxs := make([]int, 0, len(cols))
				valid := true
				for _, c := range cols {
					if idx, ok := attrIndex[c.col]; ok {
						idxs = append(idxs, idx)
					} else {
						valid = false
						break
					}
				}
				if valid && len(idxs) > 0 {
					table.references = append(table.references, idxs)
				}
			}
		}
	}
	return nil
}

func loadTableData(db *sql.DB, sheet *Sheet, selectColumns []string,
	terminalHeight int) ([][]interface{}, error) {

	// Create temporary file for full dataset
	tempFile, err := os.CreateTemp("", "ted_data_*.csv")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary file: %w", err)
	}
	sheet.filePath = tempFile.Name()

	stopChan := make(chan struct{})
	sheet.StopChan = stopChan

	csvWriter := csv.NewWriter(tempFile)
	headers := append([]string(nil), selectColumns...)
	if err := csvWriter.Write(headers); err != nil {
		tempFile.Close()
		return nil, fmt.Errorf("failed to write CSV headers: %w", err)
	}
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		tempFile.Close()
		return nil, fmt.Errorf("failed to flush CSV headers: %w", err)
	}

	initialBatchSize := max(10, terminalHeight-5)

	readyCh := make(chan struct{})
	doneCh := make(chan struct{})
	errCh := make(chan error, 1)
	var readyOnce sync.Once

	go func(target int) {
		defer close(doneCh)
		defer func() {
			csvWriter.Flush()
			_ = csvWriter.Error()
			_ = tempFile.Sync()
			tempFile.Close()
		}()

		query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectColumns, ", "), sheet.table)
		if strings.TrimSpace(sheet.whereClause) != "" {
			query = query + " WHERE " + sheet.whereClause
		}
		if strings.TrimSpace(sheet.orderBy) != "" {
			query = query + " ORDER BY " + sheet.orderBy
		}
		if sheet.limit > 0 {
			query = fmt.Sprintf("%s LIMIT %d", query, sheet.limit)
		}

		rows, err := db.Query(query)
		if err != nil {
			errCh <- fmt.Errorf("query error: %w", err)
			readyOnce.Do(func() { close(readyCh) })
			return
		}
		defer rows.Close()

		for rows.Next() {
			select {
			case <-stopChan:
				readyOnce.Do(func() { close(readyCh) })
				return
			default:
			}

			values := make([]interface{}, len(selectColumns))
			scanArgs := make([]interface{}, len(selectColumns))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			if err := rows.Scan(scanArgs...); err != nil {
				errCh <- fmt.Errorf("scan error: %w", err)
				continue
			}

			record := make([]string, len(values))
			for i, val := range values {
				record[i] = formatCSVValue(val)
			}

			if err := csvWriter.Write(record); err != nil {
				errCh <- fmt.Errorf("write error: %w", err)
				continue
			}
			csvWriter.Flush()
			if err := csvWriter.Error(); err != nil {
				errCh <- fmt.Errorf("flush error: %w", err)
				csvWriter.Flush()
			}
			if err := tempFile.Sync(); err != nil {
				errCh <- fmt.Errorf("sync error: %w", err)
			}

			count := atomic.AddInt64(&sheet.rowCount, 1)
			if int(count) >= target {
				readyOnce.Do(func() { close(readyCh) })
			}
		}

		if err := rows.Err(); err != nil {
			errCh <- fmt.Errorf("rows iteration error: %w", err)
		}

		readyOnce.Do(func() { close(readyCh) })
	}(initialBatchSize)

	var initialData [][]interface{}

waitLoop:
	for {
		select {
		case <-readyCh:
			break waitLoop
		case <-doneCh:
			break waitLoop
		case err := <-errCh:
			if err != nil {
				return nil, err
			}
		}
	}

	rowsAvailable := int(atomic.LoadInt64(&sheet.rowCount))
	rowsToRead := min(rowsAvailable, initialBatchSize)
	if rowsToRead > 0 {
		data, err := sheet.readRows(0, rowsToRead)
		if err != nil {
			return nil, err
		}
		initialData = data
	} else {
		initialData = [][]interface{}{}
	}

	select {
	case err := <-errCh:
		if err != nil {
			return initialData, err
		}
	default:
	}

	return initialData, nil
}

func formatCSVValue(value interface{}) string {
	if value == nil {
		return "\\N"
	}

	switch v := value.(type) {
	case []byte:
		return string(v)
	case string:
		return v
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", value)
	}
}

func parseCSVValue(raw string, attr attribute) interface{} {
	if raw == "\\N" {
		return nil
	}

	trimmed := strings.TrimSpace(raw)
	t := strings.ToLower(attr.Type)

	switch {
	case strings.Contains(t, "bool"):
		lower := strings.ToLower(trimmed)
		if lower == "1" || lower == "true" || lower == "t" {
			return true
		}
		if lower == "0" || lower == "false" || lower == "f" {
			return false
		}
		return raw
	case strings.Contains(t, "int"):
		if v, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return v
		}
		return raw
	case strings.Contains(t, "real") || strings.Contains(t, "double") ||
		strings.Contains(t, "float") || strings.Contains(t, "numeric") ||
		strings.Contains(t, "decimal"):
		if v, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return v
		}
		return raw
	default:
		return raw
	}
}

func (sheet *Sheet) readRows(offset, count int) ([][]interface{}, error) {
	if sheet.filePath == "" || count <= 0 {
		return [][]interface{}{}, nil
	}

	file, err := os.Open(sheet.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	if _, err := reader.Read(); err != nil {
		if err == io.EOF {
			return [][]interface{}{}, nil
		}
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	for skipped := 0; skipped < offset; skipped++ {
		if _, err := reader.Read(); err != nil {
			if err == io.EOF {
				return [][]interface{}{}, nil
			}
			return nil, fmt.Errorf("failed to skip row: %w", err)
		}
	}

	rows := make([][]interface{}, 0, count)
	for len(rows) < count {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read row: %w", err)
		}

		row := make([]interface{}, len(record))
		for i, raw := range record {
			var colName string
			if i < len(sheet.selectCols) {
				colName = sheet.selectCols[i]
			}
			if attr, ok := sheet.attrByName[colName]; ok {
				row[i] = parseCSVValue(raw, attr)
			} else if raw == "\\N" {
				row[i] = nil
			} else {
				row[i] = raw
			}
		}
		rows = append(rows, row)
	}

	return rows, nil
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

// validateUniqueConstraintNullHandling checks that unique constraints either:
// 1. Have NULLS NOT DISTINCT (treat nulls as equal), or
// 2. All columns in the unique constraint are NOT NULL
// unique constraint null-handling validators removed; nullability is handled inline

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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

// BuildUpdatePreview constructs a SQL UPDATE statement as a string with literal
// values inlined for preview purposes. It mirrors UpdateDBValue but does not
// execute any SQL. Intended only for UI preview.
func (sheet *Sheet) BuildUpdatePreview(rowData []interface{}, colName string, newValue string) string {
	if len(rowData) == 0 || len(sheet.lookupKey) == 0 {
		return ""
	}

	toDBValue := func(colName, raw string) interface{} {
		attr, ok := sheet.attrByName[colName]
		if !ok {
			return raw
		}
		if raw == "\\N" {
			return nil
		}
		t := strings.ToLower(attr.Type)
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

	literal := func(val interface{}, attrType string) string {
		if val == nil {
			return "NULL"
		}
		switch v := val.(type) {
		case bool:
			if sheet.DBType == PostgreSQL {
				if v {
					return "true"
				}
				return "false"
			}
			if v {
				return "1"
			}
			return "0"
		case int64:
			return strconv.FormatInt(v, 10)
		case float64:
			return strconv.FormatFloat(v, 'f', -1, 64)
		case string:
			return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
		case []byte:
			s := strings.ReplaceAll(string(v), "'", "''")
			return fmt.Sprintf("'%s'", s)
		default:
			return fmt.Sprintf("'%v'", v)
		}
	}

	whereParts := make([]string, 0, len(sheet.lookupKey))
	for _, lookupKeyCol := range sheet.lookupKey {
		idx, ok := sheet.columnIndex[lookupKeyCol]
		if !ok || idx >= len(rowData) {
			return ""
		}
		attr := sheet.attrByName[lookupKeyCol]
		qKeyName := quoteIdent(sheet.DBType, lookupKeyCol)
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", qKeyName, literal(rowData[idx], attr.Type)))
	}

	attr := sheet.attrByName[colName]
	valueArg := toDBValue(colName, newValue)
	quotedTarget := quoteIdent(sheet.DBType, colName)
	setClause := fmt.Sprintf("%s = %s", quotedTarget, literal(valueArg, attr.Type))

	returningCols := make([]string, len(sheet.attributes))
	for i, attribute := range sheet.attributes {
		returningCols[i] = quoteIdent(sheet.DBType, attribute.Name)
	}
	if len(sheet.lookupKey) == 1 {
		if sheet.lookupKey[0] == "rowid" {
			returningCols = append(returningCols, "rowid")
		}
		if sheet.lookupKey[0] == "ctid" {
			returningCols = append(returningCols, "ctid")
		}
	}

	quotedTable := quoteQualified(sheet.DBType, sheet.table)
	whereClause := strings.Join(whereParts, " AND ")
	statement := fmt.Sprintf("UPDATE %s SET %s WHERE %s", quotedTable, setClause, whereClause)
	if databaseFeatures[sheet.DBType].returning {
		returning := strings.Join(returningCols, ", ")
		return fmt.Sprintf("%s RETURNING %s", statement, returning)
	}
	return statement
}

// UpdateDBValue updates a single cell in the underlying database using the
// sheet's lookupKey columns to identify the row. It returns the refreshed row
// values ordered by sheet.selectCols. If no row is updated, returns an error.
func (sheet *Sheet) UpdateDBValue(rowData []interface{}, colName string, newValue string) ([]interface{}, error) {
	if len(rowData) == 0 {
		return nil, fmt.Errorf("row data is empty")
	}
	if len(sheet.lookupKey) == 0 {
		return nil, fmt.Errorf("no lookup key configured")
	}

	toDBValue := func(colName, raw string) interface{} {
		attr, ok := sheet.attrByName[colName]
		if !ok {
			return raw
		}
		if raw == "\\N" {
			return nil
		}
		t := strings.ToLower(attr.Type)
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

	placeholder := func(i int) string {
		switch sheet.DBType {
		case PostgreSQL:
			return fmt.Sprintf("$%d", i)
		default:
			return "?"
		}
	}

	valueArg := toDBValue(colName, newValue)
	keyArgs := make([]interface{}, 0, len(sheet.lookupKey))
	whereParts := make([]string, 0, len(sheet.lookupKey))
	for i, lookupKeyCol := range sheet.lookupKey {
		qKeyName := quoteIdent(sheet.DBType, lookupKeyCol)
		var ph string
		if sheet.DBType == PostgreSQL {
			ph = placeholder(2 + i)
		} else {
			ph = placeholder(0)
		}
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", qKeyName, ph))
		idx, ok := sheet.columnIndex[lookupKeyCol]
		if !ok || idx >= len(rowData) {
			return nil, fmt.Errorf("lookup key %s not available in row data", lookupKeyCol)
		}
		keyArgs = append(keyArgs, rowData[idx])
	}

	var setClause string
	quotedTarget := quoteIdent(sheet.DBType, colName)
	if sheet.DBType == PostgreSQL {
		setClause = fmt.Sprintf("%s = %s", quotedTarget, placeholder(1))
	} else {
		setClause = fmt.Sprintf("%s = %s", quotedTarget, placeholder(0))
	}

	returningCols := make([]string, len(sheet.attributes))
	for i, attr := range sheet.attributes {
		returningCols[i] = quoteIdent(sheet.DBType, attr.Name)
	}

	if len(sheet.lookupKey) == 1 {
		if sheet.lookupKey[0] == "rowid" {
			returningCols = append(returningCols, "rowid")
		}
		if sheet.lookupKey[0] == "ctid" {
			returningCols = append(returningCols, "ctid")
		}
	}
	returning := strings.Join(returningCols, ", ")

	useReturning := databaseFeatures[sheet.DBType].returning
	quotedTable := quoteQualified(sheet.DBType, sheet.table)
	if useReturning {
		query := fmt.Sprintf("UPDATE %s SET %s WHERE %s RETURNING %s", quotedTable, setClause, strings.Join(whereParts, " AND "), returning)
		args := make([]interface{}, 0, 1+len(keyArgs))
		args = append(args, valueArg)
		args = append(args, keyArgs...)

		rowVals := make([]interface{}, len(returningCols))
		scanArgs := make([]interface{}, len(returningCols))
		for i := range rowVals {
			scanArgs[i] = &rowVals[i]
		}

		if err := sheet.DB.QueryRow(query, args...).Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("update failed: %w", err)
		}
		return rowVals, nil
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", quotedTable, setClause, strings.Join(whereParts, " AND "))

	tx, err := sheet.DB.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin tx failed: %w", err)
	}

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

	returningQuery := fmt.Sprintf("SELECT %s FROM %s WHERE %s", returning, quotedTable, strings.Join(whereParts, " AND "))
	row := tx.QueryRow(returningQuery, keyArgs...)
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
