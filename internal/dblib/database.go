package dblib

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/parser/test_driver"
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

// buildColumnsFromAnalysis creates Column structs from SQL analysis and column metadata.
// For passthrough columns (non-derived), it queries the base table to get accurate type information.
func buildColumnsFromAnalysis(db *sql.DB, dbType DatabaseType, analysis *ViewAnalysis,
	columnNames []string, columnTypes []*sql.ColumnType) ([]Column, map[string]int, error) {

	if len(columnNames) != len(analysis.Columns) {
		return nil, nil, fmt.Errorf("column count mismatch: analysis has %d columns, query returned %d",
			len(analysis.Columns), len(columnNames))
	}

	columns := make([]Column, 0, len(columnNames))
	columnIndex := make(map[string]int)

	for i, colName := range columnNames {
		lineage := analysis.Columns[i]

		col := Column{
			Name:       colName,
			Table:      lineage.SourceTable,
			BaseColumn: lineage.SourceColumn,
			Generated:  lineage.IsDerived,
			Reference:  -1,
			Nullable:   true, // Default to nullable
		}

		// Get type from column metadata if available
		if i < len(columnTypes) {
			col.Type = columnTypes[i].DatabaseTypeName()
			if nullable, ok := columnTypes[i].Nullable(); ok {
				col.Nullable = nullable
			}
		} else {
			col.Type = "text"
		}

		// For passthrough columns, try to get more accurate type from base table
		if !lineage.IsDerived && lineage.SourceTable != "" && lineage.SourceColumn != "" {
			baseRel, err := NewRelation(db, dbType, lineage.SourceTable)
			if err == nil {
				if baseColIdx, ok := baseRel.ColumnIndex[lineage.SourceColumn]; ok && baseColIdx < len(baseRel.Columns) {
					baseCol := baseRel.Columns[baseColIdx]
					col.Type = baseCol.Type
					col.Nullable = baseCol.Nullable
					col.EnumValues = baseCol.EnumValues
					col.CustomTypeName = baseCol.CustomTypeName
				}
			}
		}

		columns = append(columns, col)
		columnIndex[col.Name] = i
	}

	return columns, columnIndex, nil
}

// buildTablesMap creates the Tables map by loading base tables and mapping their keys
// to result column indices.
func buildTablesMap(db *sql.DB, dbType DatabaseType, baseTables []string,
	resultColumns []Column) (map[string]*Table, error) {

	tables := make(map[string]*Table)

	for _, tableName := range baseTables {
		baseRel, err := NewRelation(db, dbType, tableName)
		if err != nil {
			continue // Skip if we can't load the base table
		}

		table := &Table{
			Name: tableName,
			Key:  []int{},
		}

		// Map base table key columns to result column indices
		for _, keyIdx := range baseRel.Key {
			if keyIdx < len(baseRel.Columns) {
				baseKeyCol := baseRel.Columns[keyIdx]
				// Find this column in the result columns
				for resultColIdx, resultCol := range resultColumns {
					if resultCol.Table == tableName && resultCol.BaseColumn == baseKeyCol.Name {
						table.Key = append(table.Key, resultColIdx)
						break
					}
				}
			}
		}

		tables[tableName] = table
	}

	return tables, nil
}

// determineKeysFromAnalysis determines the key columns for a relation based on SQL analysis.
// Returns deduplicated key column indices.
func determineKeysFromAnalysis(analysis *ViewAnalysis, columns []Column,
	columnIndex map[string]int, tables map[string]*Table) ([]int, error) {

	var keys []int

	if analysis.HasGroupBy {
		// If GROUP BY present, check if it includes all base table keys
		groupByCols := make(map[string]bool)
		for _, expr := range analysis.GroupByExprs {
			// Try to match GROUP BY expressions to columns
			for _, col := range columns {
				if expr == col.Name || expr == col.BaseColumn {
					groupByCols[col.Name] = true
					break
				}
			}
		}

		// Check if all base table keys are in GROUP BY
		allKeysInGroupBy := true
		for _, table := range tables {
			if len(table.Key) == 0 {
				continue
			}
			for _, keyIdx := range table.Key {
				if keyIdx < len(columns) {
					keyCol := columns[keyIdx]
					if !groupByCols[keyCol.Name] {
						allKeysInGroupBy = false
						break
					}
				}
			}
			if !allKeysInGroupBy {
				break
			}
		}

		if allKeysInGroupBy {
			// GROUP BY includes all keys, so use base table keys
			for _, table := range tables {
				keys = append(keys, table.Key...)
			}
		} else {
			// GROUP BY doesn't include all keys, use GROUP BY columns as keys
			for _, expr := range analysis.GroupByExprs {
				if colIdx, ok := columnIndex[expr]; ok {
					keys = append(keys, colIdx)
				}
			}
		}
	} else if analysis.HasDistinct {
		// SELECT DISTINCT - all columns form the key
		for i := range columns {
			keys = append(keys, i)
		}
	} else if len(tables) == 1 {
		// Simple query from single table - use base table keys
		for _, table := range tables {
			keys = append(keys, table.Key...)
		}
	} else {
		// Complex query or joins - use all columns as composite key
		for i := range columns {
			keys = append(keys, i)
		}
	}

	// Remove duplicates
	keyMap := make(map[int]bool)
	var uniqueKeys []int
	for _, keyIdx := range keys {
		if !keyMap[keyIdx] {
			keyMap[keyIdx] = true
			uniqueKeys = append(uniqueKeys, keyIdx)
		}
	}

	return uniqueKeys, nil
}

var ErrNoKeyableColumns = errors.New("no keyable columns found")

func NewRelation(db *sql.DB, dbType DatabaseType, tableName string) (*Relation, error) {
	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

	wrapErr := func(err error) (*Relation, error) {
		return nil, fmt.Errorf("failed to load table schema: %w", err)
	}

	// First check if it's a view or table
	isView, err := checkIsView(db, dbType, tableName)
	if err != nil {
		return wrapErr(fmt.Errorf("failed to check if relation is view: %w", err))
	}
	relation := &Relation{
		DB:          db,
		DBType:      dbType,
		Name:        tableName,
		IsView:      isView,
		Tables:      make(map[string]*Table),
		Columns:     []Column{},
		ColumnIndex: make(map[string]int),
		References:  []Reference{},
	}

	var (
		columns        []Column
		columnIndex    map[string]int
		primaryKeyCols []string
	)

	if isView {
		// Load view columns and metadata
		columns, columnIndex, err = loadViewColumns(db, dbType, tableName)
		if err != nil {
			return wrapErr(err)
		}
		relation.Columns = columns
		relation.ColumnIndex = columnIndex

		// Identify view keys
		viewKeys, err := getViewKeys(db, dbType, tableName, relation)
		if err != nil {
			return wrapErr(fmt.Errorf("failed to identify view keys: %w", err))
		}
		if len(viewKeys) == 0 {
			return wrapErr(fmt.Errorf("view has no keyable columns"))
		}
		relation.Key = viewKeys
	} else {
		// Load table columns based on database type
		switch dbType {
		case SQLite:
			columns, columnIndex, primaryKeyCols, err = loadColumnsSQLite(db, tableName)
			if err != nil {
				return wrapErr(err)
			}
		case PostgreSQL:
			columns, columnIndex, err = loadColumnsPostgreSQL(db, tableName)
			if err != nil {
				return wrapErr(err)
			}
		case MySQL:
			columns, columnIndex, err = loadColumnsMySQL(db, tableName)
			if err != nil {
				return wrapErr(err)
			}
		default:
			return wrapErr(fmt.Errorf("unsupported database type: %v", dbType))
		}

		relation.Columns = columns
		relation.ColumnIndex = columnIndex

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
			return nil, ErrNoKeyableColumns
		}

		// Convert key column names to indices
		relation.Key = make([]int, 0, len(lookupCols))
		for _, keyCol := range lookupCols {
			if idx, ok := relation.ColumnIndex[keyCol]; ok {
				relation.Key = append(relation.Key, idx)
			}
		}

		if len(relation.Key) == 0 {
			return wrapErr(fmt.Errorf("key columns not found in column list"))
		}
	}

	// Fetch enum values and custom type information
	if err := relation.loadEnumAndCustomTypes(); err != nil {
		// Non-fatal error, just log and continue
		fmt.Fprintf(os.Stderr, "Warning: failed to load enum/custom types: %v\n", err)
	}

	// Load foreign keys
	var references []Reference
	var updatedColumns []Column
	switch dbType {
	case SQLite:
		references, updatedColumns, err = loadForeignKeysSQLite(db, dbType, tableName, relation.ColumnIndex, relation.Columns)
	case PostgreSQL:
		references, updatedColumns, err = loadForeignKeysPostgreSQL(db, dbType, tableName, relation.ColumnIndex, relation.Columns)
	case MySQL:
		references, updatedColumns, err = loadForeignKeysMySQL(db, dbType, tableName, relation.ColumnIndex, relation.Columns)
	}
	if err == nil {
		relation.References = references
		relation.Columns = updatedColumns
	}

	return relation, nil
}

// NewRelationFromSQL creates a Relation from a custom SQL SELECT statement
func NewRelationFromSQL(db *sql.DB, dbType DatabaseType, sqlStr string) (*Relation, error) {
	// 1. Validate SQL
	trimmedSQL := strings.TrimRight(sqlStr, "; \t\n\r")
	if strings.Contains(trimmedSQL, ";") {
		return nil, fmt.Errorf("multiple statements not allowed")
	}

	upperSQL := strings.ToUpper(strings.TrimSpace(sqlStr))

	// Check it starts with SELECT or WITH
	if !strings.HasPrefix(upperSQL, "SELECT") && !strings.HasPrefix(upperSQL, "WITH") {
		return nil, fmt.Errorf("only SELECT or WITH statements allowed")
	}

	// Check for dangerous keywords
	dangerousKeywords := []string{
		"INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
		"TRUNCATE", "GRANT", "REVOKE",
	}
	for _, kw := range dangerousKeywords {
		// Use word boundaries to avoid false positives (e.g., "INSERTED" column name)
		if strings.Contains(upperSQL, " "+kw+" ") || strings.Contains(upperSQL, " "+kw+"\n") {
			return nil, fmt.Errorf("keyword %s not allowed in custom SQL", kw)
		}
	}

	// 2. Parse SQL using existing ParseViewDefinition
	analysis, err := ParseViewDefinition(sqlStr, dbType, db)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	// 3. Extract column schema by executing LIMIT 0 query
	// Wrap in subquery to ensure LIMIT applies to entire query
	schemaQuery := fmt.Sprintf("SELECT * FROM (%s) AS custom_query LIMIT 0", sqlStr)
	rows, err := db.Query(schemaQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names: %w", err)
	}

	if len(columnNames) == 0 {
		return nil, fmt.Errorf("query must return at least one column")
	}

	if len(columnNames) != len(analysis.Columns) {
		return nil, fmt.Errorf("column count mismatch: analysis has %d columns, query returned %d",
			len(analysis.Columns), len(columnNames))
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	// 4. Build Columns array using helper
	columns, columnIndex, err := buildColumnsFromAnalysis(db, dbType, analysis, columnNames, columnTypes)
	if err != nil {
		return nil, err
	}

	relation := &Relation{
		DB:           db,
		DBType:       dbType,
		Name:         "",
		IsView:       false,
		IsCustomSQL:  true,
		SQLStatement: sqlStr,
		Tables:       make(map[string]*Table),
		Columns:      columns,
		ColumnIndex:  columnIndex,
		References:   []Reference{},
	}

	// 5. Build Tables map using helper
	tables, err := buildTablesMap(db, dbType, analysis.BaseTables, relation.Columns)
	if err != nil {
		return nil, err
	}
	relation.Tables = tables

	// 6. Determine custom SQL keys using helper
	keys, err := determineKeysFromAnalysis(analysis, relation.Columns, relation.ColumnIndex, relation.Tables)
	if err != nil {
		return nil, err
	}
	relation.Key = keys

	// Validate we have at least one key column
	if len(relation.Key) == 0 {
		// Fall back to all columns as key
		relation.Key = make([]int, len(relation.Columns))
		for i := range relation.Columns {
			relation.Key[i] = i
		}
	}

	return relation, nil
}

// checkIsView checks if a relation is a view or table
func checkIsView(db *sql.DB, dbType DatabaseType, relationName string) (bool, error) {
	switch dbType {
	case SQLite:
		var sqlType string
		err := db.QueryRow("SELECT type FROM sqlite_master WHERE name = ? AND type IN ('table', 'view')", relationName).Scan(&sqlType)
		if err != nil {
			return false, err
		}
		return sqlType == "view", nil
	case PostgreSQL:
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
	case MySQL:
		var isView bool
		err := db.QueryRow(`
			SELECT EXISTS (
				SELECT 1 FROM information_schema.views 
				WHERE table_schema = DATABASE() AND table_name = ?
			)`, relationName).Scan(&isView)
		return isView, err
	default:
		return false, fmt.Errorf("unsupported database type: %v", dbType)
	}
}

// loadViewColumns loads columns for a view by parsing its definition
func loadViewColumns(db *sql.DB, dbType DatabaseType, viewName string) ([]Column, map[string]int, error) {
	// Get view definition
	var viewDef string
	var err error
	switch dbType {
	case SQLite:
		viewDef, err = getViewDefinitionSQLite(db, viewName)
	case PostgreSQL:
		viewDef, err = getViewDefinitionPostgreSQL(db, viewName)
	case MySQL:
		viewDef, err = getViewDefinitionMySQL(db, viewName)
	default:
		return nil, nil, fmt.Errorf("unsupported database type for views: %v", dbType)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get view definition: %w", err)
	}
	// Parse view definition - handle both CREATE VIEW and plain SELECT
	// Try parsing as SELECT first, if that fails, try to extract SELECT from CREATE VIEW
	analysis, err := ParseViewDefinition(viewDef, dbType, db)
	if err != nil {
		// If it's a CREATE VIEW statement, try to extract the SELECT part
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(viewDef)), "CREATE") {
			// Parse as CREATE VIEW and extract the SELECT
			p := parser.New()
			stmtNodes, _, parseErr := p.Parse(viewDef, "", "")
			if parseErr == nil && len(stmtNodes) > 0 {
				if createView, ok := stmtNodes[0].(*ast.CreateViewStmt); ok {
					if selectStmt, ok := createView.Select.(*ast.SelectStmt); ok {
						// Create a temporary analysis and parse the SELECT
						tempAnalysis := &ViewAnalysis{
							CTEs: make(map[string]*ViewAnalysis),
						}
						analysis, err = parseSelectStmt(selectStmt, dbType, tempAnalysis, db, viewDef)
					}
				}
			}
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse view definition: %w", err)
		}
	}

	// Get actual column names from the view by querying it
	// Use SELECT * LIMIT 0 to get column metadata without fetching data
	queryView := fmt.Sprintf("SELECT * FROM %s LIMIT 0", quoteQualified(dbType, viewName))
	rows, err := db.Query(queryView)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query view for column names: %w", err)
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get column names from query: %w", err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get column types: %w", err)
	}

	// Build columns using helper
	columns, columnIndex, err := buildColumnsFromAnalysis(db, dbType, analysis, columnNames, columnTypes)
	if err != nil {
		return nil, nil, err
	}

	return columns, columnIndex, nil
}

// getViewKeys identifies key columns for a view
func getViewKeys(db *sql.DB, dbType DatabaseType, viewName string, relation *Relation) ([]int, error) {
	// Get view definition
	var viewDef string
	var err error
	switch dbType {
	case SQLite:
		viewDef, err = getViewDefinitionSQLite(db, viewName)
	case PostgreSQL:
		viewDef, err = getViewDefinitionPostgreSQL(db, viewName)
	case MySQL:
		viewDef, err = getViewDefinitionMySQL(db, viewName)
	default:
		return nil, fmt.Errorf("unsupported database type for views: %v", dbType)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get view definition: %w", err)
	}

	// Parse view definition - handle both CREATE VIEW and plain SELECT
	analysis, err := ParseViewDefinition(viewDef, dbType, db)
	if err != nil {
		// If it's a CREATE VIEW statement, try to extract the SELECT
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(viewDef)), "CREATE") {
			p := parser.New()
			stmtNodes, _, parseErr := p.Parse(viewDef, "", "")
			if parseErr == nil && len(stmtNodes) > 0 {
				if createView, ok := stmtNodes[0].(*ast.CreateViewStmt); ok {
					if selectStmt, ok := createView.Select.(*ast.SelectStmt); ok {
						// Create a temporary analysis and parse the SELECT
						tempAnalysis := &ViewAnalysis{
							CTEs: make(map[string]*ViewAnalysis),
						}
						analysis, err = parseSelectStmt(selectStmt, dbType, tempAnalysis, db, viewDef)
					}
				}
			}
		}
		if err != nil {
			return nil, fmt.Errorf("failed to parse view definition: %w", err)
		}
	}

	// Build Tables map using helper
	tables, err := buildTablesMap(db, dbType, analysis.BaseTables, relation.Columns)
	if err != nil {
		return nil, err
	}
	// Update relation.Tables with the loaded tables
	for tableName, table := range tables {
		relation.Tables[tableName] = table
	}

	// Determine view keys using helper
	viewKeys, err := determineKeysFromAnalysis(analysis, relation.Columns, relation.ColumnIndex, relation.Tables)
	if err != nil {
		return nil, err
	}

	return viewKeys, nil
}

// IsColumnEditable returns true if a column can be edited
func (r *Relation) IsColumnEditable(colIdx int) bool {
	if colIdx < 0 || colIdx >= len(r.Columns) {
		return false
	}

	col := r.Columns[colIdx]

	// Custom SQL editability
	if r.IsCustomSQL {
		// Generated/derived columns are not editable
		if col.Generated {
			return false
		}

		// Must have clear lineage to base table
		if col.Table == "" || col.BaseColumn == "" {
			return false
		}

		// Base table must exist and have keys
		baseTable, ok := r.Tables[col.Table]
		if !ok || len(baseTable.Key) == 0 {
			return false
		}

		// All base table keys must be present in result set
		for _, keyIdx := range baseTable.Key {
			if keyIdx >= len(r.Columns) {
				return false
			}
			keyCol := r.Columns[keyIdx]
			if keyCol.Table != col.Table {
				return false
			}
		}

		return true
	}

	// All table columns are editable
	if !r.IsView {
		return true
	}

	// Derived columns are never editable
	if col.Table == "" {
		return false
	}

	// Check if base table key is included in view columns
	baseTable, ok := r.Tables[col.Table]
	if !ok {
		return false
	}

	// All key columns of the base table must be present in the view
	for _, keyIdx := range baseTable.Key {
		if keyIdx >= len(r.Columns) {
			return false
		}
		keyCol := r.Columns[keyIdx]
		if keyCol.Table != col.Table {
			return false
		}
	}

	return true
}

// updateViewColumn updates a column in a view by updating the base table
func (rel *Relation) updateViewColumn(records [][]any, rowIdx int, colName string, newValue string) ([]any, error) {
	// Find the column
	colIdx, ok := rel.ColumnIndex[colName]
	if !ok || colIdx >= len(rel.Columns) {
		return nil, fmt.Errorf("column %s not found", colName)
	}

	col := rel.Columns[colIdx]

	// Check if editable
	if !rel.IsColumnEditable(colIdx) {
		return nil, fmt.Errorf("column %s is not editable", colName)
	}

	// Get base table
	baseTable, ok := rel.Tables[col.Table]
	if !ok {
		return nil, fmt.Errorf("base table %s not found", col.Table)
	}

	// Load base table relation
	baseRel, err := NewRelation(rel.DB, rel.DBType, col.Table)
	if err != nil {
		return nil, fmt.Errorf("failed to load base table: %w", err)
	}

	// Build key values for base table from view row
	baseKeyValues := make([]any, len(baseTable.Key))
	for i, viewKeyIdx := range baseTable.Key {
		if viewKeyIdx < len(records[rowIdx]) {
			baseKeyValues[i] = records[rowIdx][viewKeyIdx]
		}
	}

	// Find base table column name
	baseColName := col.BaseColumn
	if baseColName == "" {
		baseColName = colName
	}

	// Update base table using base table's UpdateDBValue
	// We need to find the row in the base table using the key values
	// For now, use a direct UPDATE query
	whereParts := make([]string, 0, len(baseRel.Key))
	whereParams := make([]any, 0, len(baseRel.Key))
	for i, keyIdx := range baseRel.Key {
		if keyIdx < len(baseRel.Columns) {
			keyCol := baseRel.Columns[keyIdx]
			whereParts = append(whereParts, fmt.Sprintf("%s = ?", quoteIdent(rel.DBType, keyCol.Name)))
			whereParams = append(whereParams, baseKeyValues[i])
		}
	}

	// Convert newValue to appropriate type
	toDBValue := func(colName, raw string) any {
		attrType := ""
		if colIdx, ok := baseRel.ColumnIndex[colName]; ok && colIdx < len(baseRel.Columns) {
			attrType = strings.ToLower(baseRel.Columns[colIdx].Type)
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
		case strings.Contains(t, "real") || strings.Contains(t, "double") || strings.Contains(t, "float") ||
			strings.Contains(t, "numeric") || strings.Contains(t, "decimal"):
			if v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64); err == nil {
				return v
			}
			return raw
		default:
			return raw
		}
	}

	valueArg := toDBValue(baseColName, newValue)

	// Build UPDATE query
	placeholder := func(i int) string {
		switch rel.DBType {
		case PostgreSQL:
			return fmt.Sprintf("$%d", i)
		default:
			return "?"
		}
	}

	var setClause string
	quotedTarget := quoteIdent(rel.DBType, baseColName)
	if rel.DBType == PostgreSQL {
		setClause = fmt.Sprintf("%s = %s", quotedTarget, placeholder(1))
	} else {
		setClause = fmt.Sprintf("%s = %s", quotedTarget, placeholder(0))
	}

	returningCols := make([]string, len(baseRel.Columns))
	for i, col := range baseRel.Columns {
		returningCols[i] = quoteIdent(rel.DBType, col.Name)
	}
	returning := strings.Join(returningCols, ", ")

	quotedTable := quoteQualified(rel.DBType, baseRel.Name)
	useReturning := databaseFeatures[rel.DBType].returning

	var query string
	var args []any
	if rel.DBType == PostgreSQL {
		args = append(args, valueArg)
		args = append(args, whereParams...)
	} else {
		args = append(args, valueArg)
		args = append(args, whereParams...)
	}

	if useReturning {
		query = fmt.Sprintf("UPDATE %s SET %s WHERE %s RETURNING %s",
			quotedTable, setClause, strings.Join(whereParts, " AND "), returning)
	} else {
		query = fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			quotedTable, setClause, strings.Join(whereParts, " AND "))
	}

	// Execute update
	if useReturning {
		rowVals := make([]any, len(returningCols))
		scanArgs := make([]any, len(returningCols))
		for i := range rowVals {
			scanArgs[i] = &rowVals[i]
		}

		if err := rel.DB.QueryRow(query, args...).Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("update failed: %w", err)
		}

		// Map base table columns back to view columns
		viewRow := make([]any, len(rel.Columns))
		for i, viewCol := range rel.Columns {
			if viewCol.Table == col.Table && viewCol.BaseColumn != "" {
				// Find in base table result
				if baseColIdx, ok := baseRel.ColumnIndex[viewCol.BaseColumn]; ok && baseColIdx < len(rowVals) {
					viewRow[i] = rowVals[baseColIdx]
				}
			} else if i < len(records[rowIdx]) {
				// Keep existing value for non-updated columns
				viewRow[i] = records[rowIdx][i]
			}
		}

		return viewRow, nil
	}

	// For databases without RETURNING, use transaction
	tx, err := rel.DB.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin tx failed: %w", err)
	}

	res, err := tx.Exec(query, args...)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("update failed: %w", err)
	}
	if ra, _ := res.RowsAffected(); ra == 0 {
		_ = tx.Rollback()
		return nil, fmt.Errorf("no rows updated")
	}

	// Re-select the updated row
	selQuery := fmt.Sprintf("SELECT %s FROM %s WHERE %s", returning, quotedTable, strings.Join(whereParts, " AND "))
	row := tx.QueryRow(selQuery, whereParams...)
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

	// Map base table columns back to view columns
	viewRow := make([]any, len(rel.Columns))
	for i, viewCol := range rel.Columns {
		if viewCol.Table == col.Table && viewCol.BaseColumn != "" {
			// Find in base table result
			if baseColIdx, ok := baseRel.ColumnIndex[viewCol.BaseColumn]; ok && baseColIdx < len(rowVals) {
				viewRow[i] = rowVals[baseColIdx]
			}
		} else if i < len(records[rowIdx]) {
			// Keep existing value for non-updated columns
			viewRow[i] = records[rowIdx][i]
		}
	}

	return viewRow, nil
}

// loadEnumAndCustomTypes fetches enum values and custom type information for columns
func (rel *Relation) loadEnumAndCustomTypes() error {
	var updatedColumns []Column
	var err error

	switch rel.DBType {
	case MySQL:
		updatedColumns, err = loadEnumAndCustomTypesMySQL(rel.DB, rel.Name, rel.Columns)
	case PostgreSQL:
		updatedColumns, err = loadEnumAndCustomTypesPostgreSQL(rel.DB, rel.Name, rel.Columns)
	case SQLite:
		updatedColumns, err = loadEnumAndCustomTypesSQLite(rel.DB, rel.Name, rel.Columns)
	default:
		return nil
	}

	if err != nil {
		return err
	}

	rel.Columns = updatedColumns
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
		if strings.Contains(at, "int") || strings.Contains(at, "real") || strings.Contains(at, "double") ||
			strings.Contains(at, "float") || strings.Contains(at, "numeric") || strings.Contains(at, "decimal") {
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
func (rel *Relation) BuildInsertPreview(newRecordRow []any, columns []DisplayColumn) string {

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
		colIdx, ok := rel.ColumnIndex[column.Name]
		if !ok || colIdx >= len(rel.Columns) {
			continue
		}
		col := rel.Columns[colIdx]
		// nil means no update
		if newRecordRow[i] != EmptyCellValue {
			cols = append(cols, quoteIdent(rel.DBType, column.Name))
			vals = append(vals, rel.formatLiteral(newRecordRow[i], col.Type))
		}
	}

	if len(cols) == 0 {
		return ""
	}

	quotedTable := quoteQualified(rel.DBType, rel.Name)
	useReturning := databaseFeatures[rel.DBType].returning

	returningCols := make([]string, len(rel.Columns))
	for i, col := range rel.Columns {
		returningCols[i] = quoteIdent(rel.DBType, col.Name)
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
		if colIdx, ok := rel.ColumnIndex[colName]; ok && colIdx < len(rel.Columns) {
			attrType = strings.ToLower(rel.Columns[colIdx].Type)
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
		case strings.Contains(t, "real") || strings.Contains(t, "double") || strings.Contains(t, "float") ||
			strings.Contains(t, "numeric") || strings.Contains(t, "decimal"):
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
	for _, keyIdx := range rel.Key {
		if keyIdx >= len(rel.Columns) {
			return ""
		}
		keyCol := rel.Columns[keyIdx]
		qKeyName := quoteIdent(rel.DBType, keyCol.Name)
		if rowIdx >= len(records) {
			return ""
		}
		row := records[rowIdx]
		if keyIdx < 0 || keyIdx >= len(row) {
			return ""
		}
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", qKeyName, rel.formatLiteral(row[keyIdx], keyCol.Type)))
	}

	// SET clause literal
	targetAttrType := ""
	if colIdx, ok := rel.ColumnIndex[colName]; ok && colIdx < len(rel.Columns) {
		targetAttrType = rel.Columns[colIdx].Type
	}
	valueArg := toDBValue(colName, newValue)
	quotedTarget := quoteIdent(rel.DBType, colName)
	setClause := fmt.Sprintf("%s = %s", quotedTarget, rel.formatLiteral(valueArg, targetAttrType))

	returningCols := make([]string, len(rel.Columns))
	for i, col := range rel.Columns {
		returningCols[i] = quoteIdent(rel.DBType, col.Name)
	}
	if len(rel.Key) == 1 {
		keyIdx := rel.Key[0]
		if keyIdx < len(rel.Columns) {
			keyCol := rel.Columns[keyIdx]
			if keyCol.Name == "rowid" {
				returningCols = append(returningCols, "rowid")
			}
			if keyCol.Name == "ctid" {
				returningCols = append(returningCols, "ctid")
			}
		}
	}
	returning := strings.Join(returningCols, ", ")

	quotedTable := quoteQualified(rel.DBType, rel.Name)
	useReturning := databaseFeatures[rel.DBType].returning
	if useReturning {
		return fmt.Sprintf("UPDATE %s SET %s WHERE %s RETURNING %s",
			quotedTable, setClause, strings.Join(whereParts, " AND "), returning)
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
	for _, keyIdx := range rel.Key {
		if keyIdx >= len(rel.Columns) {
			return ""
		}
		keyCol := rel.Columns[keyIdx]
		qKeyName := quoteIdent(rel.DBType, keyCol.Name)
		if rowIdx >= len(records) {
			return ""
		}
		row := records[rowIdx]
		if keyIdx < 0 || keyIdx >= len(row) {
			return ""
		}
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", qKeyName, rel.formatLiteral(row[keyIdx], keyCol.Type)))
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
	for _, keyIdx := range rel.Key {
		if keyIdx >= len(rel.Columns) {
			return fmt.Errorf("key column index %d out of range", keyIdx)
		}
		keyCol := rel.Columns[keyIdx]
		qKeyName := quoteIdent(rel.DBType, keyCol.Name)
		row := records[rowIdx]
		if keyIdx < 0 || keyIdx >= len(row) {
			return fmt.Errorf("key column %s index out of range", keyCol.Name)
		}
		whereParts = append(whereParts, fmt.Sprintf("%s = ?", qKeyName))
		whereParams = append(whereParams, row[keyIdx])
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

// selectInsertedRowByKeys selects a row from the database using key values from newRecordRow.
// This is used when RETURNING is not supported or LastInsertId is unavailable.
// Key values are type-converted before being used in the WHERE clause.
func (rel *Relation) selectInsertedRowByKeys(tx *sql.Tx, newRecordRow []any, returningCols []string,
	placeholder func(int) string, quotedTable string) ([]any, error) {
	// Type conversion helper (matches toDBValue in InsertDBRecord)
	toDBValue := func(colIdx int, raw any) any {
		// If already not a string, return as-is
		strVal, ok := raw.(string)
		if !ok {
			return raw
		}

		attrType := ""
		if colIdx < len(rel.Columns) {
			attrType = strings.ToLower(rel.Columns[colIdx].Type)
		}
		if strVal == NullGlyph || strVal == "" {
			return nil
		}
		t := attrType
		switch {
		case strings.Contains(t, "bool"):
			lower := strings.ToLower(strings.TrimSpace(strVal))
			if lower == "1" || lower == "true" || lower == "t" {
				return true
			}
			if lower == "0" || lower == "false" || lower == "f" {
				return false
			}
			return strVal
		case strings.Contains(t, "int"):
			if v, err := strconv.ParseInt(strings.TrimSpace(strVal), 10, 64); err == nil {
				return v
			}
			return strVal
		case strings.Contains(t, "real") || strings.Contains(t, "double") || strings.Contains(t, "float") ||
			strings.Contains(t, "numeric") || strings.Contains(t, "decimal"):
			if v, err := strconv.ParseFloat(strings.TrimSpace(strVal), 64); err == nil {
				return v
			}
			return strVal
		default:
			return strVal
		}
	}

	// Build WHERE clause using key values
	whereParts := make([]string, 0, len(rel.Key))
	whereParams := make([]any, 0, len(rel.Key))
	paramIdx := 1

	for _, keyIdx := range rel.Key {
		if keyIdx >= len(rel.Columns) {
			return nil, fmt.Errorf("key column index %d out of range", keyIdx)
		}
		keyColName := rel.Columns[keyIdx].Name
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", quoteIdent(rel.DBType, keyColName), placeholder(paramIdx)))

		// Apply type conversion to key value
		keyVal := newRecordRow[keyIdx]
		whereParams = append(whereParams, toDBValue(keyIdx, keyVal))
		paramIdx++
	}

	// Execute SELECT
	selQuery := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		strings.Join(returningCols, ", "), quotedTable, strings.Join(whereParts, " AND "))

	row := tx.QueryRow(selQuery, whereParams...)
	rowVals := make([]any, len(returningCols))
	scanArgs := make([]any, len(rowVals))
	for i := range rowVals {
		scanArgs[i] = &rowVals[i]
	}

	if err := row.Scan(scanArgs...); err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	return rowVals, nil
}

// InsertDBRecord inserts a new record into the database. It returns the inserted
// row values ordered by relation.Columns. The newRecordRow should contain
// values for all columns (or nil/NullGlyph for NULL values).
func (rel *Relation) InsertDBRecord(newRecordRow []any) ([]any, error) {
	if len(newRecordRow) != len(rel.Columns) {
		return nil, fmt.Errorf("newRecordRow length mismatch: expected %d, got %d", len(rel.Columns), len(newRecordRow))
	}

	// For multi-column keys, validate that all key values are present
	// (multi-column keys cannot use LastInsertId since only single auto-increment columns can be auto-generated)
	if len(rel.Key) > 1 {
		for _, keyIdx := range rel.Key {
			if keyIdx >= len(newRecordRow) {
				return nil, fmt.Errorf("multi-column key requires all key values to be provided")
			}
			keyVal := newRecordRow[keyIdx]
			if keyVal == nil || keyVal == "" || keyVal == NullGlyph || keyVal == EmptyCellValue {
				return nil, fmt.Errorf("multi-column key requires all key values to be provided (key column %s is missing)", rel.Columns[keyIdx].Name)
			}
		}
	}

	// Convert string values to appropriate DB values
	toDBValue := func(colIdx int, raw string) any {
		attrType := ""
		if colIdx < len(rel.Columns) {
			attrType = strings.ToLower(rel.Columns[colIdx].Type)
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

	for i, col := range rel.Columns {
		val := newRecordRow[i]

		// Convert to DB value
		var dbVal any
		if strVal, ok := val.(string); ok {
			dbVal = toDBValue(i, strVal)
		} else {
			dbVal = val
		}

		// Skip NULL/empty values ONLY if column is nullable AND has a default
		// For now, we include all NOT NULL columns even if nil (let DB handle constraint violations)
		// We skip nullable columns with nil values (let DB use defaults)
		if (dbVal == nil || dbVal == "" || dbVal == NullGlyph) && col.Nullable {
			continue
		}

		if dbVal == EmptyCellValue {
			continue
		}
		cols = append(cols, quoteIdent(rel.DBType, col.Name))
		placeholders = append(placeholders, placeholder(paramPos))
		args = append(args, dbVal)
		paramPos++
	}

	// Build RETURNING clause
	returningCols := make([]string, len(rel.Columns))
	for i, col := range rel.Columns {
		returningCols[i] = quoteIdent(rel.DBType, col.Name)
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

		// Retrieve the inserted row
		var rowVals []any
		var selectErr error

		if len(rel.Key) == 1 {
			// For single-column keys, try LastInsertId first
			lastID, err := result.LastInsertId()
			if err == nil && lastID > 0 {
				// Select the inserted row by last insert ID
				keyIdx := rel.Key[0]
				if keyIdx >= len(rel.Columns) {
					_ = tx.Rollback()
					return nil, fmt.Errorf("key column index out of range")
				}
				keyColName := rel.Columns[keyIdx].Name
				selQuery := fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s",
					returning, quotedTable, quoteIdent(rel.DBType, keyColName), placeholder(1))
				row := tx.QueryRow(selQuery, lastID)

				rowVals = make([]any, len(returningCols))
				scanArgs := make([]any, len(rowVals))
				for i := range rowVals {
					scanArgs[i] = &rowVals[i]
				}

				selectErr = row.Scan(scanArgs...)
			} else {
				// LastInsertId unavailable, use helper
				rowVals, selectErr = rel.selectInsertedRowByKeys(tx, newRecordRow, returningCols, placeholder, quotedTable)
			}
		} else {
			// For multi-column keys, skip LastInsertId and use helper directly
			rowVals, selectErr = rel.selectInsertedRowByKeys(tx, newRecordRow, returningCols, placeholder, quotedTable)
		}

		if selectErr != nil {
			_ = tx.Rollback()
			return nil, selectErr
		}

		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit failed: %w", err)
		}
		return rowVals, nil
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

	// Retrieve the inserted row
	var rowVals []any
	var selectErr error

	if len(rel.Key) == 1 {
		// For single-column keys, try LastInsertId first
		lastID, err := result.LastInsertId()
		if err == nil && lastID > 0 {
			// Select the inserted row by last insert ID
			keyIdx := rel.Key[0]
			if keyIdx >= len(rel.Columns) {
				_ = tx.Rollback()
				return nil, fmt.Errorf("key column index out of range")
			}
			keyColName := rel.Columns[keyIdx].Name
			selQuery := fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s",
				returning, quotedTable, quoteIdent(rel.DBType, keyColName), placeholder(1))
			row := tx.QueryRow(selQuery, lastID)

			rowVals = make([]any, len(returningCols))
			scanArgs := make([]any, len(rowVals))
			for i := range rowVals {
				scanArgs[i] = &rowVals[i]
			}

			selectErr = row.Scan(scanArgs...)
		} else {
			// LastInsertId unavailable, use helper
			rowVals, selectErr = rel.selectInsertedRowByKeys(tx, newRecordRow, returningCols, placeholder, quotedTable)
		}
	} else {
		// For multi-column keys, skip LastInsertId and use helper directly
		rowVals, selectErr = rel.selectInsertedRowByKeys(tx, newRecordRow, returningCols, placeholder, quotedTable)
	}

	if selectErr != nil {
		_ = tx.Rollback()
		return nil, selectErr
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit failed: %w", err)
	}
	return rowVals, nil
}

// UpdateDBValue updates a single cell in the underlying database using the
// relation's lookup key columns to identify the row. It returns the refreshed
// row values ordered by relation.Columns. If no row is updated, returns
// an error.
// For views, this updates the base table column.
func (rel *Relation) UpdateDBValue(records [][]any, rowIdx int, colName string, newValue string) ([]any, error) {
	if rowIdx < 0 || rowIdx >= len(records) {
		return nil, fmt.Errorf("index out of range")
	}
	if len(rel.Key) == 0 {
		return nil, fmt.Errorf("no lookup key configured")
	}

	// For views and custom SQL, update the base table instead
	if rel.IsView || rel.IsCustomSQL {
		return rel.updateViewColumn(records, rowIdx, colName, newValue)
	}

	// Convert string to appropriate DB value
	toDBValue := func(colName, raw string) any {
		attrType := ""
		if colIdx, ok := rel.ColumnIndex[colName]; ok && colIdx < len(rel.Columns) {
			attrType = strings.ToLower(rel.Columns[colIdx].Type)
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
		case strings.Contains(t, "real") || strings.Contains(t, "double") || strings.Contains(t, "float") ||
			strings.Contains(t, "numeric") || strings.Contains(t, "decimal"):
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
	for i, keyIdx := range rel.Key {
		if keyIdx >= len(rel.Columns) {
			return nil, fmt.Errorf("key column index %d out of range", keyIdx)
		}
		keyCol := rel.Columns[keyIdx]
		qKeyName := quoteIdent(rel.DBType, keyCol.Name)
		var ph string
		if rel.DBType == PostgreSQL {
			ph = placeholder(2 + i)
		} else {
			ph = placeholder(0)
		}
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", qKeyName, ph))
		if rowIdx >= len(records) {
			err := fmt.Errorf("lookup key column %s not found in records", keyCol.Name)
			return nil, err
		}
		row := records[rowIdx]
		if keyIdx < 0 || keyIdx >= len(row) {
			err := fmt.Errorf("lookup key column %s not loaded", keyCol.Name)
			return nil, err
		}
		keyArgs = append(keyArgs, row[keyIdx])
	}

	// SET clause placeholder
	var setClause string
	quotedTarget := quoteIdent(rel.DBType, colName)
	if rel.DBType == PostgreSQL {
		setClause = fmt.Sprintf("%s = %s", quotedTarget, placeholder(1))
	} else {
		setClause = fmt.Sprintf("%s = %s", quotedTarget, placeholder(0))
	}

	returningCols := make([]string, len(rel.Columns))
	for i, col := range rel.Columns {
		returningCols[i] = quoteIdent(rel.DBType, col.Name)
	}

	if len(rel.Key) == 1 {
		keyIdx := rel.Key[0]
		if keyIdx < len(rel.Columns) {
			keyCol := rel.Columns[keyIdx]
			if keyCol.Name == "rowid" {
				returningCols = append(returningCols, "rowid")
			}
			if keyCol.Name == "ctid" {
				returningCols = append(returningCols, "ctid")
			}
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
	// Custom SQL uses different query building
	if rel.IsCustomSQL {
		return rel.queryRowsCustomSQL(columns, sortCol, params, inclusive, scrollDown)
	}

	// Convert key indices to column names
	keyColNames := make([]string, len(rel.Key))
	for i, keyIdx := range rel.Key {
		keyColNames[i] = rel.Columns[keyIdx].Name
	}

	query, err := selectQuery(rel.DBType, rel.Name, columns, sortCol, keyColNames, len(params) > 0, inclusive, scrollDown)
	if err != nil {
		return nil, err
	}

	rows, err := rel.DB.Query(query, params...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// queryRowsCustomSQL builds and executes a paginated query for custom SQL
func (rel *Relation) queryRowsCustomSQL(columns []string, sortCol *SortColumn, params []any, inclusive, scrollDown bool) (*sql.Rows, error) {
	// Convert key indices to column names
	keyColNames := make([]string, len(rel.Key))
	for i, keyIdx := range rel.Key {
		keyColNames[i] = rel.Columns[keyIdx].Name
	}

	// Build the query by wrapping the custom SQL in a subquery
	var builder strings.Builder

	// SELECT clause
	builder.WriteString("SELECT ")
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdent(rel.DBType, col)
	}
	builder.WriteString(strings.Join(quotedColumns, ", "))

	// FROM clause - wrap custom SQL as subquery
	builder.WriteString(" FROM (")
	builder.WriteString(rel.SQLStatement)
	builder.WriteString(") AS custom_query")

	// WHERE clause for pagination
	if len(params) > 0 {
		builder.WriteString(" WHERE ")

		// Build row value comparison for key columns
		colExpr, placeholders := buildRowValueExpression(rel.DBType, keyColNames, 1)
		placeholderExpr := buildPlaceholderExpression(placeholders)

		// Determine operator based on direction and inclusivity
		var operator string
		if scrollDown {
			if inclusive {
				operator = " >= "
			} else {
				operator = " > "
			}
		} else {
			if inclusive {
				operator = " <= "
			} else {
				operator = " < "
			}
		}

		builder.WriteString(colExpr)
		builder.WriteString(operator)
		builder.WriteString(placeholderExpr)
	}

	// ORDER BY clause
	builder.WriteString(" ORDER BY ")
	if sortCol != nil {
		quotedSortCol := SortColumn{Name: quoteIdent(rel.DBType, sortCol.Name), Asc: sortCol.Asc}
		sortColString := quotedSortCol.String(scrollDown)
		builder.WriteString(sortColString)
		builder.WriteString(", ")
	}
	for i, colName := range keyColNames {
		quotedCol := quoteIdent(rel.DBType, colName)
		sc := SortColumn{Name: quotedCol, Asc: scrollDown}
		builder.WriteString(sc.String(scrollDown))
		if i < len(keyColNames)-1 {
			builder.WriteString(", ")
		}
	}
	query := builder.String()

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
	if findCol < 0 || findCol >= len(rel.Columns) {
		return nil, false, fmt.Errorf("findCol index out of range")
	}
	if len(currentKeys) != len(rel.Key) {
		return nil, false, fmt.Errorf("currentKeys length mismatch: expected %d, got %d", len(rel.Key), len(currentKeys))
	}

	searchColName := rel.Columns[findCol].Name
	quotedSearchCol := quoteIdent(rel.DBType, searchColName)
	quotedTable := quoteQualified(rel.DBType, rel.Name)

	// Build key column list for SELECT
	keyCols := make([]string, len(rel.Key))
	for i, keyIdx := range rel.Key {
		if keyIdx < len(rel.Columns) {
			keyCols[i] = quoteIdent(rel.DBType, rel.Columns[keyIdx].Name)
		}
	}
	selectClause := strings.Join(keyCols, ", ")

	// Helper to build WHERE clause for multi-column key progression using row value syntax
	buildKeyWhere := func(op string, startPos int) (string, []any) {
		// Get key column names
		keyColNames := make([]string, len(rel.Key))
		for i, keyIdx := range rel.Key {
			if keyIdx >= len(rel.Columns) {
				continue
			}
			keyColNames[i] = rel.Columns[keyIdx].Name
		}

		// Build row value expression
		colExpr, placeholders := buildRowValueExpression(rel.DBType, keyColNames, startPos)
		placeholderExpr := buildPlaceholderExpression(placeholders)

		// Build WHERE clause: (key1, key2) > (?, ?)
		whereClause := fmt.Sprintf("%s %s %s", colExpr, op, placeholderExpr)

		// Build args from currentKeys
		args := make([]any, len(currentKeys))
		copy(args, currentKeys)

		return whereClause, args
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
	for _, keyIdx := range rel.Key {
		if keyIdx < len(rel.Columns) {
			orderParts = append(orderParts, quoteIdent(rel.DBType, rel.Columns[keyIdx].Name)+" ASC")
		}
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
	for _, keyIdx := range rel.Key {
		if keyIdx < len(rel.Columns) {
			orderParts = append(orderParts, quoteIdent(rel.DBType, rel.Columns[keyIdx].Name)+" DESC")
		}
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
