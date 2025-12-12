package dblib

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// PostgresHandler implements DatabaseHandler for PostgreSQL databases.
type PostgresHandler struct{}

// getShortestLookupKeyPostgreSQL returns the best lookup key for a PostgreSQL table.
func getShortestLookupKeyPostgreSQL(db *sql.DB, tableName string, sizeOf func(string, int) int) ([]string, error) {
	type candidate struct {
		name    string
		cols    []string
		numCols int
		totalSz int
		isPK    bool
	}

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

	candidates := []candidate{}

	// Unique non-primary indexes
	uRows, err := db.Query(`SELECT i.relname,
                                       array_agg(a.attname ORDER BY k.ord) as columns
                                 FROM pg_index idx
                                 JOIN pg_class c ON c.oid = idx.indrelid
                                 JOIN pg_class i ON i.oid = idx.indexrelid
                                 JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE
                                 JOIN pg_attribute a ON a.attrelid = idx.indrelid AND a.attnum = k.attnum
                                 WHERE c.relname = $1 AND idx.indisunique AND NOT idx.indisprimary
                                 GROUP BY i.relname, array_length(idx.indkey, 1)
                                 ORDER BY array_length(idx.indkey, 1), i.relname`, tableName)
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

// loadColumnsPostgreSQL loads columns for a PostgreSQL table.
func loadColumnsPostgreSQL(db *sql.DB, tableName string) ([]Column, map[string]int, error) {
	// Extract schema and relation name
	schema := "public"
	rel := tableName
	if dot := strings.IndexByte(rel, '.'); dot != -1 {
		schema = rel[:dot]
		rel = rel[dot+1:]
	}

	query := `SELECT column_name, data_type, is_nullable
			FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = $2
			ORDER BY ordinal_position`
	rows, err := db.Query(query, schema, rel)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var columns []Column
	columnIndex := make(map[string]int)

	for rows.Next() {
		var col Column
		col.Reference = -1 // Initialize to -1 (not a foreign key)
		var nullable string

		err = rows.Scan(&col.Name, &col.Type, &nullable)
		if err != nil {
			return nil, nil, err
		}

		col.Nullable = strings.ToLower(nullable) == "yes"
		col.Table = tableName     // For tables, Table is the table name itself
		col.BaseColumn = col.Name // For tables, BaseColumn is the same as Name

		idx := len(columns)
		columns = append(columns, col)
		columnIndex[col.Name] = idx
	}

	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	return columns, columnIndex, nil
}

// loadForeignKeysPostgreSQL loads foreign key constraints for a PostgreSQL table.
func loadForeignKeysPostgreSQL(db *sql.DB, dbType DatabaseType, tableName string, columnIndex map[string]int, columns []Column) ([]Reference, []Column, error) {
	var references []Reference
	updatedColumns := make([]Column, len(columns))
	copy(updatedColumns, columns)

	// Use pg_catalog to get correct column order within FK
	schema := "public"
	rel := tableName
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
	if err != nil {
		return references, updatedColumns, nil
	}
	defer fkRows.Close()

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

	for _, cols := range byID {
		sort.Slice(cols, func(i, j int) bool { return cols[i].ord < cols[j].ord })
		if len(cols) == 0 {
			continue
		}
		refTableName := cols[0].refTable
		// Load the foreign table metadata to get key column names if needed
		foreignRel, err := NewRelation(db, dbType, refTableName)
		if err != nil {
			// If we can't load the foreign table, skip this reference
			fmt.Fprintf(nil, "Warning: failed to load foreign table %s: %v\n", refTableName, err)
			continue
		}
		// Create a new Reference entry
		ref := Reference{
			Table:   refTableName,
			Columns: make(map[string]string),
		}
		for i, c := range cols {
			if idx, ok := columnIndex[c.col]; ok {
				foreignCol := c.refCol
				// If refCol is empty, it references the foreign table's primary key
				if foreignCol == "" {
					// Get key column name from foreign relation
					if i < len(foreignRel.Key) {
						keyIdx := foreignRel.Key[i]
						if keyIdx < len(foreignRel.Columns) {
							foreignCol = foreignRel.Columns[keyIdx].Name
						}
					}
				}
				ref.Columns[c.col] = foreignCol
				// Update column with reference index
				if idx < len(updatedColumns) {
					updatedColumns[idx].Reference = len(references)
				}
			}
		}
		references = append(references, ref)
	}

	return references, updatedColumns, nil
}

// getBestKeyPostgreSQL identifies the best key for a PostgreSQL table using system catalogs.
// Ranking: primary key > unique (NOT NULL/NULLS NOT DISTINCT) > fewer columns > shorter > earlier.
func getBestKeyPostgreSQL(db *sql.DB, tableName string) ([]string, error) {
	type keyCandidate struct {
		cols       []string
		isPK       bool
		numCols    int
		totalSize  int
		minOrdinal int // minimum ordinal position among columns in this key
	}

	// Extract schema and relation name
	schema := "public"
	rel := tableName
	if dot := strings.IndexByte(rel, '.'); dot != -1 {
		schema = rel[:dot]
		rel = rel[dot+1:]
	}

	// Get column metadata: name, type, length, ordinal position
	colInfo := make(map[string]struct {
		dataType string
		length   int
		ordinal  int
	})
	colQuery := `SELECT column_name, data_type,
	                    COALESCE(character_maximum_length, -1) AS max_len,
	                    ordinal_position
	             FROM information_schema.columns
	             WHERE table_schema = $1 AND table_name = $2`
	colRows, err := db.Query(colQuery, schema, rel)
	if err != nil {
		return nil, fmt.Errorf("failed to query column metadata: %w", err)
	}
	defer colRows.Close()
	for colRows.Next() {
		var name, dtype string
		var maxLen, ordinal int
		if err := colRows.Scan(&name, &dtype, &maxLen, &ordinal); err != nil {
			continue
		}
		colInfo[name] = struct {
			dataType string
			length   int
			ordinal  int
		}{dtype, maxLen, ordinal}
	}
	colRows.Close()

	var candidates []keyCandidate

	// Check for primary key
	pkQuery := `SELECT a.attname
	            FROM pg_index i
	            JOIN pg_class c ON c.oid = i.indrelid
	            JOIN pg_namespace n ON n.oid = c.relnamespace
	            JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE
	            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
	            WHERE n.nspname = $1 AND c.relname = $2 AND i.indisprimary
	            ORDER BY k.ord`
	pkRows, err := db.Query(pkQuery, schema, rel)
	if err == nil {
		defer pkRows.Close()
		var pkCols []string
		for pkRows.Next() {
			var colName string
			if err := pkRows.Scan(&colName); err == nil {
				pkCols = append(pkCols, colName)
			}
		}
		pkRows.Close()
		if len(pkCols) > 0 {
			// Compute size and min ordinal for PK
			totalSize := 0
			minOrd := int(^uint(0) >> 1) // max int
			for _, col := range pkCols {
				if info, ok := colInfo[col]; ok {
					totalSize += sizeOf(info.dataType, info.length)
					if info.ordinal < minOrd {
						minOrd = info.ordinal
					}
				}
			}
			candidates = append(candidates, keyCandidate{
				cols:       pkCols,
				isPK:       true,
				numCols:    len(pkCols),
				totalSize:  totalSize,
				minOrdinal: minOrd,
			})
		}
	}

	// Check for unique indexes with NOT NULL or NULLS NOT DISTINCT
	uQuery := `SELECT i.indexrelid::regclass::text AS index_name,
	                  array_agg(a.attname ORDER BY k.ord) AS columns,
	                  idx.indnullsnotdistinct
	           FROM pg_index idx
	           JOIN pg_class c ON c.oid = idx.indrelid
	           JOIN pg_namespace n ON n.oid = c.relnamespace
	           JOIN pg_class i ON i.oid = idx.indexrelid
	           JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE
	           JOIN pg_attribute a ON a.attrelid = idx.indrelid AND a.attnum = k.attnum
	           WHERE n.nspname = $1 AND c.relname = $2
	             AND idx.indisunique AND NOT idx.indisprimary
	           GROUP BY idx.indexrelid, idx.indnullsnotdistinct`
	uRows, err := db.Query(uQuery, schema, rel)
	if err == nil {
		defer uRows.Close()
		for uRows.Next() {
			var indexName, colArray string
			var nullsNotDistinct sql.NullBool
			if err := uRows.Scan(&indexName, &colArray, &nullsNotDistinct); err != nil {
				continue
			}
			// Parse array: {col1,col2,...}
			colArray = strings.Trim(colArray, "{}")
			if colArray == "" {
				continue
			}
			cols := strings.Split(colArray, ",")

			// Check if all columns are NOT NULL, or if NULLS NOT DISTINCT is enabled
			allNotNull := true
			for _, col := range cols {
				// Check nullability from information_schema
				var nullable string
				nullQuery := `SELECT is_nullable FROM information_schema.columns
				              WHERE table_schema = $1 AND table_name = $2 AND column_name = $3`
				if err := db.QueryRow(nullQuery, schema, rel, col).Scan(&nullable); err == nil {
					if strings.ToLower(nullable) == "yes" {
						allNotNull = false
						break
					}
				}
			}

			// Accept if all NOT NULL, or NULLS NOT DISTINCT is true
			if !allNotNull && !(nullsNotDistinct.Valid && nullsNotDistinct.Bool) {
				continue
			}

			// Compute size and min ordinal
			totalSize := 0
			minOrd := int(^uint(0) >> 1) // max int
			for _, col := range cols {
				if info, ok := colInfo[col]; ok {
					totalSize += sizeOf(info.dataType, info.length)
					if info.ordinal < minOrd {
						minOrd = info.ordinal
					}
				}
			}
			candidates = append(candidates, keyCandidate{
				cols:       cols,
				isPK:       false,
				numCols:    len(cols),
				totalSize:  totalSize,
				minOrdinal: minOrd,
			})
		}
		uRows.Close()
	}

	if len(candidates) == 0 {
		return []string{}, nil
	}

	// Sort by: isPK desc, numCols asc, totalSize asc, minOrdinal asc
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].isPK != candidates[j].isPK {
			return candidates[i].isPK // true < false
		}
		if candidates[i].numCols != candidates[j].numCols {
			return candidates[i].numCols < candidates[j].numCols
		}
		if candidates[i].totalSize != candidates[j].totalSize {
			return candidates[i].totalSize < candidates[j].totalSize
		}
		return candidates[i].minOrdinal < candidates[j].minOrdinal
	})

	return candidates[0].cols, nil
}

// loadEnumAndCustomTypesPostgreSQL fetches enum values and custom type information for PostgreSQL columns.
func loadEnumAndCustomTypesPostgreSQL(db *sql.DB, tableName string, columns []Column) ([]Column, error) {
	updatedColumns := make([]Column, len(columns))
	copy(updatedColumns, columns)

	// Extract schema and relation name
	schema := "public"
	rel := tableName
	if dot := strings.IndexByte(rel, '.'); dot != -1 {
		schema = rel[:dot]
		rel = rel[dot+1:]
	}

	// PostgreSQL: Fetch custom types and enum types
	// First, get UDT (user-defined types) information
	query := `SELECT c.column_name, c.udt_name, c.data_type
		          FROM information_schema.columns c
		          WHERE c.table_schema = $1 AND c.table_name = $2
		          AND (c.data_type = 'USER-DEFINED' OR c.udt_name NOT IN ('int4', 'int8', 'varchar', 'text', 'bool', 'timestamp', 'timestamptz', 'date', 'numeric', 'float8', 'bytea'))`
	rows, err := db.Query(query, schema, rel)
	if err != nil {
		return updatedColumns, err
	}
	defer rows.Close()

	customTypes := make(map[string]string) // column_name -> udt_name
	for rows.Next() {
		var colName, udtName, dataType string
		if err := rows.Scan(&colName, &udtName, &dataType); err != nil {
			continue
		}
		if dataType == "USER-DEFINED" {
			customTypes[colName] = udtName
		}
	}
	rows.Close()

	// For each custom type, check if it's an enum and fetch values
	for colName, udtName := range customTypes {
		enumQuery := `SELECT e.enumlabel
			              FROM pg_type t
			              JOIN pg_enum e ON t.oid = e.enumtypid
			              WHERE t.typname = $1
			              ORDER BY e.enumsortorder`
		enumRows, err := db.Query(enumQuery, udtName)
		if err != nil {
			continue
		}

		var enumValues []string
		for enumRows.Next() {
			var enumValue string
			if err := enumRows.Scan(&enumValue); err == nil {
				enumValues = append(enumValues, enumValue)
			}
		}
		enumRows.Close()

		// Find column by name and update it
		for i := range updatedColumns {
			if updatedColumns[i].Name == colName {
				if len(enumValues) > 0 {
					updatedColumns[i].EnumValues = enumValues
					updatedColumns[i].CustomTypeName = udtName
				} else {
					// Not an enum, but still a custom type
					updatedColumns[i].CustomTypeName = udtName
				}
				break
			}
		}
	}

	return updatedColumns, nil
}

// getViewDefinitionPostgreSQL retrieves the SQL definition of a view in PostgreSQL
func getViewDefinitionPostgreSQL(db *sql.DB, viewName string) (string, error) {
	// Extract schema and relation name
	rel := viewName
	if dot := strings.IndexByte(rel, '.'); dot != -1 {
		rel = rel[dot+1:]
	}

	var sqlDef string
	err := db.QueryRow("SELECT pg_get_viewdef($1::regclass, true)", rel).Scan(&sqlDef)
	if err != nil {
		return "", fmt.Errorf("failed to get view definition: %w", err)
	}
	return sqlDef, nil
}

// DatabaseHandler interface implementation for PostgresHandler

// CheckIsView returns true if the named relation is a view, false if it's a table.
func (h *PostgresHandler) CheckIsView(db *sql.DB, relationName string) (bool, error) {
	// Extract schema and relation name
	schema := "public"
	rel := relationName
	if dot := strings.IndexByte(rel, '.'); dot != -1 {
		schema = rel[:dot]
		rel = rel[dot+1:]
	}
	var isView bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM pg_views
			WHERE schemaname = $1 AND viewname = $2
		)`, schema, rel).Scan(&isView)
	return isView, err
}

// LoadColumns loads column metadata for a PostgreSQL table.
func (h *PostgresHandler) LoadColumns(db *sql.DB, tableName string) ([]Column, map[string]int, error) {
	return loadColumnsPostgreSQL(db, tableName)
}

// LoadForeignKeys loads foreign key constraints for a PostgreSQL table.
func (h *PostgresHandler) LoadForeignKeys(db *sql.DB, dbType DatabaseType, tableName string,
	columnIndex map[string]int, columns []Column) ([]Reference, []Column, error) {
	return loadForeignKeysPostgreSQL(db, dbType, tableName, columnIndex, columns)
}

// LoadEnumAndCustomTypes fetches enum values for PostgreSQL columns.
func (h *PostgresHandler) LoadEnumAndCustomTypes(db *sql.DB, tableName string, columns []Column) ([]Column, error) {
	return loadEnumAndCustomTypesPostgreSQL(db, tableName, columns)
}

// GetViewDefinition retrieves the SQL definition of a PostgreSQL view.
func (h *PostgresHandler) GetViewDefinition(db *sql.DB, viewName string) (string, error) {
	return getViewDefinitionPostgreSQL(db, viewName)
}

// GetBestKey identifies the best key column(s) for a PostgreSQL table.
func (h *PostgresHandler) GetBestKey(db *sql.DB, tableName string) ([]string, error) {
	return getBestKeyPostgreSQL(db, tableName)
}

// GetShortestLookupKey returns the best lookup key for a PostgreSQL table.
func (h *PostgresHandler) GetShortestLookupKey(db *sql.DB, tableName string) ([]string, error) {
	return getShortestLookupKeyPostgreSQL(db, tableName, sizeOf)
}

// QuoteIdent quotes an identifier (table/column name) for PostgreSQL using double quotes.
func (h *PostgresHandler) QuoteIdent(ident string) string {
	// Check if safe to leave unquoted
	if isSafeUnquotedIdent(ident) {
		return ident
	}
	// Escape double quotes by doubling them
	escaped := strings.ReplaceAll(ident, "\"", "\"\"")
	return "\"" + escaped + "\""
}

// Placeholder returns the parameter placeholder for PostgreSQL (positional: $1, $2, etc.).
func (h *PostgresHandler) Placeholder(position int) string {
	return fmt.Sprintf("$%d", position)
}
