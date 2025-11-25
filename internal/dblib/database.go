package dblib

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// getShortestLookupKey returns the best lookup key for a table by considering
// the primary key and all suitable unique constraints, ranking by:
// - fewest columns
// - smallest estimated total byte width
// For PostgreSQL, NULLS NOT DISTINCT is supported so nullability is not a filter.
// For SQLite/MySQL, require all unique index columns to be NOT NULL.
func getShortestLookupKey(db *sql.DB, dbType DatabaseType, tableName string) ([]string, error) {
	switch dbType {
	case SQLite:
		return getShortestLookupKeySQLite(db, tableName, sizeOf)
	case PostgreSQL:
		return getShortestLookupKeyPostgreSQL(db, tableName, sizeOf)
	case MySQL:
		return getShortestLookupKeyMySQL(db, tableName, sizeOf)
	default:
		return []string{}, nil
	}
}

// GetBestKey identifies the best key column(s) for a relation, using system tables
// when available. The ranking preferences are (in order of priority):
// 1. Primary keys over unique constraints (with NOT NULL or NULLS NOT DISTINCT)
// 2. Fewer columns over more columns
// 3. Shorter columns over longer columns (by estimated byte width)
// 4. Earlier columns over later columns in the table definition
//
// Returns the column names comprising the best key, or an empty slice if no suitable key exists.
func GetBestKey(db *sql.DB, dbType DatabaseType, tableName string) ([]string, error) {
	switch dbType {
	case SQLite:
		return getBestKeySQLite(db, tableName)
	case PostgreSQL:
		return getBestKeyPostgreSQL(db, tableName)
	case MySQL:
		return getBestKeyMySQL(db, tableName)
	case DuckDB:
		return getBestKeyDuckDB(db, tableName)
	default:
		return []string{}, fmt.Errorf("unsupported database type: %v", dbType)
	}
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
		Name:           tableName,
		Attributes:     make(map[string]Attribute),
		AttributeIndex: make(map[string]int),
		References:     []Reference{},
	}

	var (
		attributes     map[string]Attribute
		attributeOrder []string
		attributeIndex map[string]int
		primaryKeyCols []string
		err            error
	)

	// Load attributes based on database type
	switch dbType {
	case SQLite:
		attributes, attributeOrder, attributeIndex, primaryKeyCols, err = loadAttributesSQLite(db, tableName)
		if err != nil {
			return wrapErr(err)
		}
	case PostgreSQL:
		attributes, attributeOrder, attributeIndex, err = loadAttributesPostgreSQL(db, tableName)
		if err != nil {
			return wrapErr(err)
		}
	case MySQL:
		attributes, attributeOrder, attributeIndex, err = loadAttributesMySQL(db, tableName)
		if err != nil {
			return wrapErr(err)
		}
	default:
		return wrapErr(fmt.Errorf("unsupported database type: %v", dbType))
	}

	relation.Attributes = attributes
	relation.AttributeOrder = attributeOrder
	relation.AttributeIndex = attributeIndex

	// Consolidated lookup key selection: choose shortest lookup key
	lookupCols, err := getShortestLookupKey(db, dbType, relation.Name)
	if err != nil {
		return wrapErr(err)
	}
	if dbType == SQLite && len(lookupCols) == 0 {
		// For SQLite, use the primary key columns if no unique constraints found
		lookupCols = primaryKeyCols
	}

	// TODO if lookup key not found, use databaseFeature.systemId if available

	// If not nullable unique constraint is found, error
	if len(lookupCols) == 0 {
		return wrapErr(fmt.Errorf("no primary key found"))
	}
	relation.Key = make([]string, len(lookupCols))
	copy(relation.Key, lookupCols)

	// Fetch enum values and custom type information
	if err := relation.loadEnumAndCustomTypes(); err != nil {
		// Non-fatal error, just log and continue
		fmt.Fprintf(os.Stderr, "Warning: failed to load enum/custom types: %v\n", err)
	}

	// Load foreign keys
	var references []Reference
	var updatedAttrs map[string]Attribute
	switch dbType {
	case SQLite:
		references, updatedAttrs, err = loadForeignKeysSQLite(db, dbType, tableName, relation.AttributeIndex, relation.Attributes)
	case PostgreSQL:
		references, updatedAttrs, err = loadForeignKeysPostgreSQL(db, dbType, tableName, relation.AttributeIndex, relation.Attributes)
	case MySQL:
		references, updatedAttrs, err = loadForeignKeysMySQL(db, dbType, tableName, relation.AttributeIndex, relation.Attributes)
	}
	if err == nil {
		relation.References = references
		relation.Attributes = updatedAttrs
	}

	return relation, nil
}

// loadEnumAndCustomTypes fetches enum values and custom type information for columns
func (rel *Relation) loadEnumAndCustomTypes() error {
	var updatedAttrs map[string]Attribute
	var err error

	switch rel.DBType {
	case MySQL:
		updatedAttrs, err = loadEnumAndCustomTypesMySQL(rel.DB, rel.Name, rel.Attributes)
	case PostgreSQL:
		updatedAttrs, err = loadEnumAndCustomTypesPostgreSQL(rel.DB, rel.Name, rel.Attributes)
	case SQLite:
		updatedAttrs, err = loadEnumAndCustomTypesSQLite(rel.DB, rel.Name, rel.Attributes)
	default:
		return nil
	}

	if err != nil {
		return err
	}

	rel.Attributes = updatedAttrs
	return nil
}

// formatLiteral renders a value as a SQL literal string for preview purposes.
// For NULL values, returns "NULL". For other values, formats them appropriately
// based on type, quoting and escaping strings as needed.
func (rel *Relation) formatLiteral(val any, attrType string) string {
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
		if v == NullGlyph {
			return "NULL"
		}
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

// BuildInsertPreview constructs a SQL INSERT statement as a string with literal
// values inlined for preview purposes. Intended only for UI preview.
func (rel *Relation) BuildInsertPreview(newRecordRow []any, columns []Column) string {

	// Check if all values are nil/empty
	hasNonNullValue := false
	for _, val := range newRecordRow {
		if val != nil {
			hasNonNullValue = true
			break
		}
	}

	// Return empty string if all values are nil/empty
	if !hasNonNullValue {
		return ""
	}

	// Build column list and values list
	var cols []string
	var vals []string
	for i, column := range columns {
		attr := rel.Attributes[column.Name]
		// nil means no update
		if newRecordRow[i] != EmptyCellValue {
			cols = append(cols, quoteIdent(rel.DBType, column.Name))
			vals = append(vals, rel.formatLiteral(newRecordRow[i], attr.Type))
		}
	}

	if len(cols) == 0 {
		return ""
	}

	quotedTable := quoteQualified(rel.DBType, rel.Name)
	useReturning := databaseFeatures[rel.DBType].returning

	returningCols := make([]string, len(rel.AttributeOrder))
	for i, name := range rel.AttributeOrder {
		returningCols[i] = quoteIdent(rel.DBType, name)
	}
	returning := strings.Join(returningCols, ", ")

	if useReturning {
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING %s",
			quotedTable, strings.Join(cols, ", "), strings.Join(vals, ", "), returning)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quotedTable, strings.Join(cols, ", "), strings.Join(vals, ", "))
}

// BuildUpdatePreview constructs a SQL UPDATE statement as a string with literal
// values inlined for preview purposes. It mirrors UpdateDBValue but does not
// execute any SQL. Intended only for UI preview.
func (rel *Relation) BuildUpdatePreview(records [][]any, rowIdx int, colName string, newValue string) string {
	if rowIdx < 0 || rowIdx >= len(records) || len(rel.Key) == 0 {
		return ""
	}

	// Convert raw text to DB-typed value (mirrors UpdateDBValue's toDBValue)
	toDBValue := func(colName, raw string) any {
		attrType := ""
		if attr, ok := rel.Attributes[colName]; ok {
			attrType = strings.ToLower(attr.Type)
		}
		if raw == NullGlyph {
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

	// Where clause with literal values
	whereParts := make([]string, 0, len(rel.Key))
	for _, lookupKeyCol := range rel.Key {
		qKeyName := quoteIdent(rel.DBType, lookupKeyCol)
		idx, ok := rel.AttributeIndex[lookupKeyCol]
		if !ok || rowIdx >= len(records) {
			return ""
		}
		row := records[rowIdx]
		if idx < 0 || idx >= len(row) {
			return ""
		}
		attr, ok := rel.Attributes[lookupKeyCol]
		if !ok {
			return ""
		}
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", qKeyName, rel.formatLiteral(row[idx], attr.Type)))
	}

	// SET clause literal
	targetAttrType := ""
	if attr, ok := rel.Attributes[colName]; ok {
		targetAttrType = attr.Type
	}
	valueArg := toDBValue(colName, newValue)
	quotedTarget := quoteIdent(rel.DBType, colName)
	setClause := fmt.Sprintf("%s = %s", quotedTarget, rel.formatLiteral(valueArg, targetAttrType))

	returningCols := make([]string, len(rel.AttributeOrder))
	for i, name := range rel.AttributeOrder {
		returningCols[i] = quoteIdent(rel.DBType, name)
	}
	if len(rel.Key) == 1 {
		if rel.Key[0] == "rowid" {
			returningCols = append(returningCols, "rowid")
		}
		if rel.Key[0] == "ctid" {
			returningCols = append(returningCols, "ctid")
		}
	}
	returning := strings.Join(returningCols, ", ")

	quotedTable := quoteQualified(rel.DBType, rel.Name)
	useReturning := databaseFeatures[rel.DBType].returning
	if useReturning {
		return fmt.Sprintf("UPDATE %s SET %s WHERE %s RETURNING %s", quotedTable, setClause, strings.Join(whereParts, " AND "), returning)
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s", quotedTable, setClause, strings.Join(whereParts, " AND "))
}

// BuildDeletePreview constructs a SQL DELETE statement as a string with literal
// values inlined for preview purposes. Intended only for UI preview.
func (rel *Relation) BuildDeletePreview(records [][]any, rowIdx int) string {
	if rowIdx < 0 || rowIdx >= len(records) || len(rel.Key) == 0 {
		return ""
	}

	// Where clause with literal values
	whereParts := make([]string, 0, len(rel.Key))
	for _, lookupKeyCol := range rel.Key {
		qKeyName := quoteIdent(rel.DBType, lookupKeyCol)
		idx, ok := rel.AttributeIndex[lookupKeyCol]
		if !ok || rowIdx >= len(records) {
			return ""
		}
		row := records[rowIdx]
		if idx < 0 || idx >= len(row) {
			return ""
		}
		attr, ok := rel.Attributes[lookupKeyCol]
		if !ok {
			return ""
		}
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", qKeyName, rel.formatLiteral(row[idx], attr.Type)))
	}

	quotedTable := quoteQualified(rel.DBType, rel.Name)
	return fmt.Sprintf("DELETE FROM %s WHERE %s", quotedTable, strings.Join(whereParts, " AND "))
}

// DeleteDBRecord deletes a record from the database based on its key values.
func (rel *Relation) DeleteDBRecord(records [][]any, rowIdx int) error {
	if rowIdx < 0 || rowIdx >= len(records) || len(rel.Key) == 0 {
		return fmt.Errorf("invalid row index or no key columns")
	}

	// Build WHERE clause using key columns
	whereParts := make([]string, 0, len(rel.Key))
	whereParams := make([]any, 0, len(rel.Key))
	for _, lookupKeyCol := range rel.Key {
		qKeyName := quoteIdent(rel.DBType, lookupKeyCol)
		idx, ok := rel.AttributeIndex[lookupKeyCol]
		if !ok {
			return fmt.Errorf("key column %s not found in attribute index", lookupKeyCol)
		}
		row := records[rowIdx]
		if idx < 0 || idx >= len(row) {
			return fmt.Errorf("key column %s index out of range", lookupKeyCol)
		}
		whereParts = append(whereParts, fmt.Sprintf("%s = ?", qKeyName))
		whereParams = append(whereParams, row[idx])
	}

	quotedTable := quoteQualified(rel.DBType, rel.Name)
	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s", quotedTable, strings.Join(whereParts, " AND "))

	// Execute the DELETE
	result, err := rel.DB.Exec(deleteSQL, whereParams...)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return fmt.Errorf("no rows were deleted")
	}

	return nil
}

// InsertDBRecord inserts a new record into the database. It returns the inserted
// row values ordered by relation.AttributeOrder. The newRecordRow should contain
// values for all columns (or nil/NullGlyph for NULL values).
func (rel *Relation) InsertDBRecord(newRecordRow []any) ([]any, error) {
	if len(newRecordRow) != len(rel.AttributeOrder) {
		return nil, fmt.Errorf("newRecordRow length mismatch: expected %d, got %d", len(rel.AttributeOrder), len(newRecordRow))
	}

	// Convert string values to appropriate DB values
	toDBValue := func(colName, raw string) any {
		attrType := ""
		if attr, ok := rel.Attributes[colName]; ok {
			attrType = strings.ToLower(attr.Type)
		}
		if raw == NullGlyph || raw == "" {
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

	// Build column list and values
	var cols []string
	var placeholders []string
	var args []any
	paramPos := 1

	for i, attrName := range rel.AttributeOrder {
		attr, ok := rel.Attributes[attrName]
		if !ok {
			continue
		}

		val := newRecordRow[i]

		// Convert to DB value
		var dbVal any
		if strVal, ok := val.(string); ok {
			dbVal = toDBValue(attrName, strVal)
		} else {
			dbVal = val
		}

		// Skip NULL/empty values ONLY if column is nullable AND has a default
		// For now, we include all NOT NULL columns even if nil (let DB handle constraint violations)
		// We skip nullable columns with nil values (let DB use defaults)
		if (dbVal == nil || dbVal == "" || dbVal == NullGlyph) && attr.Nullable {
			continue
		}

		if dbVal == EmptyCellValue {
			continue
		}
		cols = append(cols, quoteIdent(rel.DBType, attrName))
		placeholders = append(placeholders, placeholder(paramPos))
		args = append(args, dbVal)
		paramPos++
	}

	// Build RETURNING clause
	returningCols := make([]string, len(rel.AttributeOrder))
	for i, name := range rel.AttributeOrder {
		returningCols[i] = quoteIdent(rel.DBType, name)
	}
	returning := strings.Join(returningCols, ", ")

	// Build query
	quotedTable := quoteQualified(rel.DBType, rel.Name)
	useReturning := databaseFeatures[rel.DBType].returning

	// If no columns to insert, use DEFAULT VALUES syntax
	if len(cols) == 0 {
		if useReturning {
			query := fmt.Sprintf("INSERT INTO %s DEFAULT VALUES RETURNING %s",
				quotedTable, returning)

			// Scan returned values
			rowVals := make([]any, len(returningCols))
			scanArgs := make([]any, len(returningCols))
			for i := range rowVals {
				scanArgs[i] = &rowVals[i]
			}

			if err := rel.DB.QueryRow(query).Scan(scanArgs...); err != nil {
				return nil, fmt.Errorf("insert failed: %w", err)
			}
			return rowVals, nil
		}

		// For databases without RETURNING, use a transaction
		query := fmt.Sprintf("INSERT INTO %s DEFAULT VALUES", quotedTable)

		tx, err := rel.DB.Begin()
		if err != nil {
			return nil, fmt.Errorf("begin tx failed: %w", err)
		}

		result, err := tx.Exec(query)
		if err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("insert failed: %w", err)
		}

		// Get last insert ID if available
		lastID, err := result.LastInsertId()
		if err == nil && lastID > 0 && len(rel.Key) == 1 {
			// Select the inserted row by last insert ID
			selQuery := fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s",
				returning, quotedTable, quoteIdent(rel.DBType, rel.Key[0]), placeholder(1))
			row := tx.QueryRow(selQuery, lastID)

			rowVals := make([]any, len(returningCols))
			scanArgs := make([]any, len(rowVals))
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

		// Fallback: commit and return nil (caller should refresh manually)
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit failed: %w", err)
		}
		return nil, nil
	}

	if useReturning {
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING %s",
			quotedTable, strings.Join(cols, ", "), strings.Join(placeholders, ", "), returning)

		// Scan returned values
		rowVals := make([]any, len(returningCols))
		scanArgs := make([]any, len(returningCols))
		for i := range rowVals {
			scanArgs[i] = &rowVals[i]
		}

		if err := rel.DB.QueryRow(query, args...).Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("insert failed: %w", err)
		}
		return rowVals, nil
	}

	// For databases without RETURNING, use a transaction
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quotedTable, strings.Join(cols, ", "), strings.Join(placeholders, ", "))

	tx, err := rel.DB.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin tx failed: %w", err)
	}

	result, err := tx.Exec(query, args...)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("insert failed: %w", err)
	}

	// Get last insert ID if available
	lastID, err := result.LastInsertId()
	if err == nil && lastID > 0 && len(rel.Key) == 1 {
		// Select the inserted row by last insert ID
		selQuery := fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s",
			returning, quotedTable, quoteIdent(rel.DBType, rel.Key[0]), placeholder(1))
		row := tx.QueryRow(selQuery, lastID)

		rowVals := make([]any, len(returningCols))
		scanArgs := make([]any, len(rowVals))
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

	// Fallback: commit and return nil (caller should refresh manually)
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit failed: %w", err)
	}
	return nil, nil
}

// UpdateDBValue updates a single cell in the underlying database using the
// relation's lookup key columns to identify the row. It returns the refreshed
// row values ordered by relation.AttributeOrder. If no row is updated, returns
// an error.
func (rel *Relation) UpdateDBValue(records [][]any, rowIdx int, colName string, newValue string) ([]any, error) {
	if rowIdx < 0 || rowIdx >= len(records) {
		return nil, fmt.Errorf("index out of range")
	}
	if len(rel.Key) == 0 {
		return nil, fmt.Errorf("no lookup key configured")
	}


	// Convert string to appropriate DB value
	toDBValue := func(colName, raw string) any {
		attrType := ""
		if attr, ok := rel.Attributes[colName]; ok {
			attrType = strings.ToLower(attr.Type)
		}
		if raw == NullGlyph {
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
	keyArgs := make([]any, 0, len(rel.Key))
	whereParts := make([]string, 0, len(rel.Key))
	for i := range rel.Key {
		lookupKeyCol := rel.Key[i]
		qKeyName := quoteIdent(rel.DBType, lookupKeyCol)
		var ph string
		if rel.DBType == PostgreSQL {
			ph = placeholder(2 + i)
		} else {
			ph = placeholder(0)
		}
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", qKeyName, ph))
		colIdx, ok := rel.AttributeIndex[lookupKeyCol]
		if !ok || rowIdx >= len(records) {
			err := fmt.Errorf("lookup key column %s not found in records", lookupKeyCol)
			return nil, err
		}
		row := records[rowIdx]
		if colIdx < 0 || colIdx >= len(row) {
			err := fmt.Errorf("lookup key column %s not loaded", lookupKeyCol)
			return nil, err
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

	returningCols := make([]string, len(rel.AttributeOrder))
	for i, name := range rel.AttributeOrder {
		returningCols[i] = quoteIdent(rel.DBType, name)
	}

	if len(rel.Key) == 1 {
		if rel.Key[0] == "rowid" {
			returningCols = append(returningCols, "rowid")
		}
		if rel.Key[0] == "ctid" {
			returningCols = append(returningCols, "ctid")
		}
	}
	returning := strings.Join(returningCols, ", ")

	// Build full query
	var query string
	useReturning := databaseFeatures[rel.DBType].returning
	quotedTable := quoteQualified(rel.DBType, rel.Name)
	if useReturning {
		query = fmt.Sprintf("UPDATE %s SET %s WHERE %s RETURNING %s", quotedTable, setClause, strings.Join(whereParts, " AND "), returning)
		// Combine args: value + keys
		args := make([]any, 0, 1+len(keyArgs))
		args = append(args, valueArg)
		args = append(args, keyArgs...)

		// Scan into pointers to capture returned values
		rowVals := make([]any, len(returningCols))
		scanArgs := make([]any, len(returningCols))
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
	args := make([]any, 0, 1+len(keyArgs))
	args = append(args, valueArg)
	args = append(args, keyArgs...)
	res, err := tx.Exec(query, args...)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("update failed: %w", err)
	}
	if ra, _ := res.RowsAffected(); ra == 0 {
		_ = tx.Rollback()
		err := fmt.Errorf("no rows updated")
		return nil, err
	}

	// Re-select the updated row within the same transaction
	selQuery := fmt.Sprintf("SELECT %s FROM %s WHERE %s", returning, quotedTable, strings.Join(whereParts, " AND "))
	row := tx.QueryRow(selQuery, keyArgs...)
	rowVals := make([]any, len(returningCols))
	scanArgs := make([]any, len(rowVals))
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
func (rel *Relation) QueryRows(columns []string, sortCol *SortColumn, params []any, inclusive, scrollDown bool) (*sql.Rows, error) {
	query, err := selectQuery(rel.DBType, rel.Name, columns, sortCol, rel.Key, len(params) > 0, inclusive, scrollDown)
	if err != nil {
		return nil, err
	}
	rows, err := rel.DB.Query(query, params...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (rel *Relation) placeholder(pos int) string {
	if databaseFeatures[rel.DBType].positionalPlaceholder {
		return fmt.Sprintf("$%d", pos)
	}
	return "?"
}

// FindNextRow searches for the next row matching findColVal in the column at index findCol.
// It searches below the current selection first, then wraps around to search from the top.
// Returns: (keys of found row, true if found below/false if wrapped, error)
func (rel *Relation) FindNextRow(findCol int, findColVal any, sortCol *SortColumn, sortColVal any, currentKeys []any) ([]any, bool, error) {
	if findCol < 0 || findCol >= len(rel.AttributeOrder) {
		return nil, false, fmt.Errorf("findCol index out of range")
	}
	if len(currentKeys) != len(rel.Key) {
		return nil, false, fmt.Errorf("currentKeys length mismatch: expected %d, got %d", len(rel.Key), len(currentKeys))
	}

	searchColName := rel.AttributeOrder[findCol]
	quotedSearchCol := quoteIdent(rel.DBType, searchColName)
	quotedTable := quoteQualified(rel.DBType, rel.Name)

	// Build key column list for SELECT
	keyCols := make([]string, len(rel.Key))
	for i, k := range rel.Key {
		keyCols[i] = quoteIdent(rel.DBType, k)
	}
	selectClause := strings.Join(keyCols, ", ")

	// Helper to build WHERE clause for multi-column key progression
	buildKeyWhere := func(op string, startPos int) (string, []any) {
		// For multi-column keys, we need: key1 >= ? AND key2 >= ? AND ... AND keyN > ?
		// The last key uses > (or <), the rest use >= (or <=)
		var parts []string
		var args []any
		lastIdx := len(rel.Key) - 1
		for i, keyCol := range rel.Key {
			quoted := quoteIdent(rel.DBType, keyCol)
			var cmp string
			if i < lastIdx {
				cmp = op + "="
			} else {
				cmp = op
			}
			parts = append(parts, fmt.Sprintf("%s %s %s", quoted, cmp, rel.placeholder(startPos)))
			args = append(args, currentKeys[i])
			startPos++
		}
		return strings.Join(parts, " AND "), args
	}

	// Search below current position
	var whereParts []string
	var args []any
	paramPos := 1

	// Sort column condition
	if sortCol != nil {
		quotedSortCol := quoteIdent(rel.DBType, sortCol.Name)
		whereParts = append(whereParts, fmt.Sprintf("%s >= %s", quotedSortCol, rel.placeholder(paramPos)))
		args = append(args, sortColVal)
		paramPos++
	}

	// Key progression conditions
	keyWhere, keyArgs := buildKeyWhere(">", paramPos)
	whereParts = append(whereParts, keyWhere)
	args = append(args, keyArgs...)
	paramPos += len(keyArgs)

	// Search column match
	whereParts = append(whereParts, fmt.Sprintf("%s = %s", quotedSearchCol, rel.placeholder(paramPos)))
	args = append(args, findColVal)

	// ORDER BY
	var orderParts []string
	if sortCol != nil {
		if sortCol.Asc {
			orderParts = append(orderParts, quoteIdent(rel.DBType, sortCol.Name)+" ASC")
		} else {
			orderParts = append(orderParts, quoteIdent(rel.DBType, sortCol.Name)+" DESC")
		}
	}
	for _, k := range rel.Key {
		orderParts = append(orderParts, quoteIdent(rel.DBType, k)+" ASC")
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT 1",
		selectClause, quotedTable, strings.Join(whereParts, " AND "), strings.Join(orderParts, ", "))

	row := rel.DB.QueryRow(query, args...)
	foundKeys := make([]any, len(rel.Key))
	scanArgs := make([]any, len(rel.Key))
	for i := range foundKeys {
		scanArgs[i] = &foundKeys[i]
	}

	err := row.Scan(scanArgs...)
	if err == nil {
		return foundKeys, true, nil // Found below
	}
	if err != sql.ErrNoRows {
		return nil, false, fmt.Errorf("search below failed: %w", err)
	}

	// Wrap around: search from top up to current position
	whereParts = whereParts[:0]
	args = args[:0]
	paramPos = 1

	// Sort column condition (reversed)
	if sortCol != nil {
		quotedSortCol := quoteIdent(rel.DBType, sortCol.Name)
		whereParts = append(whereParts, fmt.Sprintf("%s <= %s", quotedSortCol, rel.placeholder(paramPos)))
		args = append(args, sortColVal)
		paramPos++
	}

	// Key progression conditions (reversed)
	keyWhere, keyArgs = buildKeyWhere("<", paramPos)
	whereParts = append(whereParts, keyWhere)
	args = append(args, keyArgs...)
	paramPos += len(keyArgs)

	// Search column match
	whereParts = append(whereParts, fmt.Sprintf("%s = %s", quotedSearchCol, rel.placeholder(paramPos)))
	args = append(args, findColVal)

	// ORDER BY (reversed)
	orderParts = orderParts[:0]
	if sortCol != nil {
		if sortCol.Asc {
			orderParts = append(orderParts, quoteIdent(rel.DBType, sortCol.Name)+" DESC")
		} else {
			orderParts = append(orderParts, quoteIdent(rel.DBType, sortCol.Name)+" ASC")
		}
	}
	for _, k := range rel.Key {
		orderParts = append(orderParts, quoteIdent(rel.DBType, k)+" DESC")
	}

	query = fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT 1",
		selectClause, quotedTable, strings.Join(whereParts, " AND "), strings.Join(orderParts, ", "))

	row = rel.DB.QueryRow(query, args...)
	err = row.Scan(scanArgs...)
	if err == sql.ErrNoRows {
		return nil, false, nil // Not found at all
	}
	if err != nil {
		return nil, false, fmt.Errorf("wrap search failed: %w", err)
	}

	return foundKeys, false, nil // Found after wrapping
}
