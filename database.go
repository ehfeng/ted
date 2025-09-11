package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

// database: table, attribute, record
// sheet: sheet, column, row
type Sheet struct {
    // sheet
    DBType   DatabaseType
    TuiChan  <-chan []interface{} // channel for TUI updates
    FileChan <-chan []interface{} // channel for file writing
    StopChan chan struct{}        // channel to stop streaming

	// results
	table      string
	lookupKey  []string // empty if no primary key
	attributes []attribute
	uniques    [][]int // TODO unique columns (can be multicolumn)
	references [][]int // TODO foreign key columns (can be multicolumn)
    records    [][]interface{}
    filePath   string // path to temporary file containing full dataset

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
    configCols []Column, terminalHeight int, whereClause string, orderBy string, limit int) (*Sheet, error) {

	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

    sheet := &Sheet{
        DBType:      dbType,
        table:       tableName,
        whereClause: whereClause,
        orderBy:     orderBy,
        limit:       limit,
    }

	if err := loadTableSchema(db, dbType, sheet); err != nil {
		return nil, fmt.Errorf("failed to load table schema: %w", err)
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
    if err := loadTableData(db, sheet, selected, terminalHeight); err != nil {
        return nil, fmt.Errorf("failed to load table data: %w", err)
    }
	log.Println("sheet", sheet)
	return sheet, nil
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

	// TODO if postgres, try using ctid. if sqlite or duckdb, try using rowid.

	// If not nullable unique constraint is found, error
	if len(primaryKeyColumns) == 0 {
		return fmt.Errorf("no primary key found")
	}
	return nil
}

func loadTableData(db *sql.DB, sheet *Sheet, selectColumns []string,
	terminalHeight int) error {

	// Create temporary file for full dataset
	tempFile, err := os.CreateTemp("", "ted_data_*.csv")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	sheet.filePath = tempFile.Name()

	// Use passed terminal height for buffer sizing
	tuiBufferSize := max(50, terminalHeight*2) // Buffer for 2 screens worth

	// Create channels for streaming
	tuiChan := make(chan []interface{}, tuiBufferSize) // based on terminal size
	fileChan := make(chan []interface{}, 1000)
	stopChan := make(chan struct{})

	// Set up channels in table
	sheet.TuiChan = tuiChan
	sheet.FileChan = fileChan
	sheet.StopChan = stopChan

	// Write CSV header to temp file
	csvWriter := csv.NewWriter(tempFile)
	headers := make([]string, len(selectColumns))
	copy(headers, selectColumns)
	if err := csvWriter.Write(headers); err != nil {
		tempFile.Close()
		return fmt.Errorf("failed to write CSV headers: %w", err)
	}

	// Start goroutine for file writing
	go func() {
		defer func() {
			csvWriter.Flush()
			tempFile.Close()
		}()

		for row := range fileChan {
			record := make([]string, len(row))
			for i, val := range row {
				record[i] = formatCSVValue(val)
			}
			if err := csvWriter.Write(record); err != nil {
				// Log error but continue
				fmt.Fprintf(os.Stderr, "Error writing to temp file: %v\n", err)
			}
		}
	}()

	// Start goroutine for data streaming
	go func(maxTuiRows int) {
		defer func() {
			close(tuiChan)
			close(fileChan)
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
			fmt.Fprintf(os.Stderr, "Query error: %v\n", err)
			return
		}
		defer rows.Close()

		rowCount := 0
		for rows.Next() {
			select {
			case <-stopChan:
				return
			default:
			}

			values := make([]interface{}, len(selectColumns))
			scanArgs := make([]interface{}, len(selectColumns))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			if err := rows.Scan(scanArgs...); err != nil {
				fmt.Fprintf(os.Stderr, "Scan error: %v\n", err)
				continue
			}

			// Send to both channels
			rowCopy1 := make([]interface{}, len(values))
			rowCopy2 := make([]interface{}, len(values))
			copy(rowCopy1, values)
			copy(rowCopy2, values)

			// Send to file channel (non-blocking)
			select {
			case fileChan <- rowCopy1:
			default:
				// File channel full, skip this row for file (shouldn't happen with large buffer)
			}

			// Send to TUI channel for initial screen population
			if rowCount < maxTuiRows {
				select {
				case tuiChan <- rowCopy2:
					rowCount++
				default:
					// TUI channel full, continue streaming to file only
				}
			}
		}

		if err := rows.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Rows iteration error: %v\n", err)
		}
	}(tuiBufferSize)

	// Reserve space for headers, borders, and some margin
	initialBatchSize := max(10, terminalHeight-5)

	// Load initial batch with proper timeout handling
	initialBatch := make([][]interface{}, 0, initialBatchSize)
	done := make(chan bool, 1)

	// Collect available rows up to the batch size, but don't block if fewer are available
	go func() {
		defer func() { done <- true }()

		// Use a reasonable timeout for data collection
		timeout := make(chan bool, 1)
		go func() {
			// Wait 200ms for data, then timeout
			for i := 0; i < 200; i++ {
				// 1ms delays
				for j := 0; j < 100000; j++ {
				}
			}
			timeout <- true
		}()

		// Collect rows until we have enough, the channel closes, or we timeout
		for len(initialBatch) < initialBatchSize {
			select {
			case row, ok := <-sheet.TuiChan:
				if !ok {
					// Channel closed, no more data
					return
				}
				initialBatch = append(initialBatch, row)
			case <-timeout:
				// Timeout reached, use whatever we have
				return
			}
		}
	}()

	<-done
	sheet.records = initialBatch

	return nil
}

func formatCSVValue(value interface{}) string {
	if value == nil {
		return ""
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
