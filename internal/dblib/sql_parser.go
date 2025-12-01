package dblib

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/parser/test_driver"
)

// ViewAnalysis contains the result of parsing a view definition
type ViewAnalysis struct {
	Columns      []ColumnLineage
	BaseTables   []string
	HasGroupBy   bool
	HasDistinct  bool
	GroupByExprs []string
	CTEs         map[string]*ViewAnalysis // CTE name -> analysis
}

// ColumnLineage tracks where a column comes from
type ColumnLineage struct {
	SourceTable  string // real base table in the database (empty if derived), never an alias or view
	SourceColumn string // base column name (empty if derived, only for passthrough columns)
	IsDerived    bool   // true for aggregates, expressions
}

// parseContext holds state during view parsing
type parseContext struct {
	db           *sql.DB
	dbType       DatabaseType
	viewCache    map[string]*ViewAnalysis // cache parsed view analyses
	aliasToTable map[string]string        // alias -> actual table/view name in FROM clause
}

// ParseViewDefinition parses a view's SQL and returns column lineage info
// db is required to resolve column names for SELECT * statements and to resolve views
func ParseViewDefinition(sqlStr string, dbType DatabaseType, db *sql.DB) (*ViewAnalysis, error) {
	// Create parser
	p := parser.New()

	// Parse the SQL
	stmtNodes, _, err := p.Parse(sqlStr, "", "")
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	if len(stmtNodes) == 0 {
		return nil, fmt.Errorf("no SQL statement found")
	}

	// Get the first statement (should be a SELECT)
	stmt, ok := stmtNodes[0].(*ast.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("expected SELECT statement, got %T", stmtNodes[0])
	}

	// Create parsing context
	ctx := &parseContext{
		db:           db,
		dbType:       dbType,
		viewCache:    make(map[string]*ViewAnalysis),
		aliasToTable: make(map[string]string),
	}

	analysis := &ViewAnalysis{
		Columns:      []ColumnLineage{},
		BaseTables:   []string{},
		CTEs:         make(map[string]*ViewAnalysis),
		GroupByExprs: []string{},
	}

	// Process CTEs first if present
	if stmt.With != nil {
		for _, cte := range stmt.With.CTEs {
			cteName := cte.Name.String()
			// Extract SelectStmt from SubqueryExpr
			var cteSelect *ast.SelectStmt
			if subquery := cte.Query; subquery != nil {
				if sel, ok := subquery.Query.(*ast.SelectStmt); ok {
					cteSelect = sel
				}
			}
			if cteSelect == nil {
				continue // Skip if we can't parse the CTE
			}
			// Recursively parse CTE (CTEs don't need original SQL for SELECT *)
			cteAnalysis, err := parseSelectStmtWithContext(cteSelect, ctx, analysis, "")
			if err != nil {
				return nil, fmt.Errorf("failed to parse CTE %s: %w", cteName, err)
			}
			analysis.CTEs[cteName] = cteAnalysis
		}
	}

	// Process main SELECT
	mainAnalysis, err := parseSelectStmtWithContext(stmt, ctx, analysis, sqlStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse main SELECT: %w", err)
	}

	// Merge main analysis into result
	analysis.Columns = mainAnalysis.Columns
	analysis.BaseTables = mainAnalysis.BaseTables
	analysis.HasGroupBy = mainAnalysis.HasGroupBy
	analysis.HasDistinct = mainAnalysis.HasDistinct
	analysis.GroupByExprs = mainAnalysis.GroupByExprs

	return analysis, nil
}

// parseSelectStmt parses a SELECT statement and returns analysis (legacy wrapper)
// originalSQL is the original SQL string, needed for SELECT * with multiple tables
func parseSelectStmt(stmt *ast.SelectStmt, dbType DatabaseType, parentAnalysis *ViewAnalysis, db *sql.DB, originalSQL string) (*ViewAnalysis, error) {
	ctx := &parseContext{
		db:           db,
		dbType:       dbType,
		viewCache:    make(map[string]*ViewAnalysis),
		aliasToTable: make(map[string]string),
	}
	return parseSelectStmtWithContext(stmt, ctx, parentAnalysis, originalSQL)
}

// parseSelectStmtWithContext parses a SELECT statement with full context
func parseSelectStmtWithContext(stmt *ast.SelectStmt, ctx *parseContext, parentAnalysis *ViewAnalysis, originalSQL string) (*ViewAnalysis, error) {
	analysis := &ViewAnalysis{
		Columns:      []ColumnLineage{},
		BaseTables:   []string{},
		GroupByExprs: []string{},
	}

	// Check for DISTINCT
	analysis.HasDistinct = stmt.Distinct

	// Process FROM clause to identify base tables and build alias map
	// Create a fresh alias map for this SELECT statement
	localAliasMap := make(map[string]string)
	if stmt.From != nil && stmt.From.TableRefs != nil {
		tables, aliases, err := extractTablesWithAliases(stmt.From.TableRefs, parentAnalysis, ctx)
		if err != nil {
			return nil, err
		}
		analysis.BaseTables = tables
		// Merge aliases into local map
		for alias, tableName := range aliases {
			localAliasMap[alias] = tableName
		}
	}

	// Create a child context with the local alias map
	childCtx := &parseContext{
		db:           ctx.db,
		dbType:       ctx.dbType,
		viewCache:    ctx.viewCache, // Share the view cache
		aliasToTable: localAliasMap,
	}

	// Process SELECT fields
	if stmt.Fields != nil {
		for i, field := range stmt.Fields.Fields {
			lineages, err := analyzeSelectFieldWithContext(field, stmt, childCtx, parentAnalysis, originalSQL)
			if err != nil {
				return nil, fmt.Errorf("failed to analyze field %d: %w", i, err)
			}
			analysis.Columns = append(analysis.Columns, lineages...)
		}
	}

	// Process GROUP BY
	if stmt.GroupBy != nil {
		analysis.HasGroupBy = true
		for _, item := range stmt.GroupBy.Items {
			expr := item.Expr
			// Try to get column name from expression
			exprStr := formatExpr(expr)
			analysis.GroupByExprs = append(analysis.GroupByExprs, exprStr)
		}
	}

	return analysis, nil
}

// extractTables extracts table names from FROM clause (legacy wrapper)
func extractTables(tableRefs ast.ResultSetNode, parentAnalysis *ViewAnalysis) ([]string, error) {
	tables, _, err := extractTablesWithAliases(tableRefs, parentAnalysis, nil)
	return tables, err
}

// extractTablesWithAliases extracts table names and alias mappings from FROM clause
// Returns: (list of actual table/view names, alias->actualName map, error)
func extractTablesWithAliases(tableRefs ast.ResultSetNode, parentAnalysis *ViewAnalysis, ctx *parseContext) ([]string, map[string]string, error) {
	if tableRefs == nil {
		return []string{}, make(map[string]string), nil
	}

	var tables []string
	aliases := make(map[string]string)

	switch ref := tableRefs.(type) {
	case *ast.TableSource:
		// Simple table reference with possible alias
		var actualTableName string
		if tableName, ok := ref.Source.(*ast.TableName); ok {
			actualTableName = tableName.Name.String()
			tables = append(tables, actualTableName)
		} else if join, ok := ref.Source.(*ast.Join); ok {
			// Handle JOIN
			return extractTablesFromJoinWithAliases(join, parentAnalysis, ctx)
		} else if subquery, ok := ref.Source.(*ast.SelectStmt); ok {
			// Subquery - extract tables from it
			if subquery.From != nil && subquery.From.TableRefs != nil {
				return extractTablesWithAliases(subquery.From.TableRefs, parentAnalysis, ctx)
			}
		}
		// Check for alias (e.g., "products p" or "products AS p")
		if ref.AsName.String() != "" && actualTableName != "" {
			aliases[ref.AsName.String()] = actualTableName
		}
	case *ast.Join:
		return extractTablesFromJoinWithAliases(ref, parentAnalysis, ctx)
	case *ast.TableName:
		// Direct table name
		tables = append(tables, ref.Name.String())
	default:
		return nil, nil, fmt.Errorf("unsupported table reference type: %T", ref)
	}

	return tables, aliases, nil
}

// extractTablesFromJoin extracts tables from a JOIN (legacy wrapper)
func extractTablesFromJoin(join *ast.Join, parentAnalysis *ViewAnalysis) ([]string, error) {
	tables, _, err := extractTablesFromJoinWithAliases(join, parentAnalysis, nil)
	return tables, err
}

// extractTablesFromJoinWithAliases extracts tables and aliases from a JOIN
func extractTablesFromJoinWithAliases(join *ast.Join, parentAnalysis *ViewAnalysis, ctx *parseContext) ([]string, map[string]string, error) {
	var tables []string
	aliases := make(map[string]string)

	// Left side
	leftTables, leftAliases, err := extractTablesWithAliases(join.Left, parentAnalysis, ctx)
	if err != nil {
		return nil, nil, err
	}
	tables = append(tables, leftTables...)
	for k, v := range leftAliases {
		aliases[k] = v
	}

	// Right side
	rightTables, rightAliases, err := extractTablesWithAliases(join.Right, parentAnalysis, ctx)
	if err != nil {
		return nil, nil, err
	}
	tables = append(tables, rightTables...)
	for k, v := range rightAliases {
		aliases[k] = v
	}

	return tables, aliases, nil
}

// analyzeSelectField analyzes a single SELECT field to determine its lineage (legacy wrapper)
// Returns a slice of ColumnLineage because SELECT * can expand to multiple columns
func analyzeSelectField(field *ast.SelectField, stmt *ast.SelectStmt, dbType DatabaseType, parentAnalysis *ViewAnalysis, db *sql.DB, originalSQL string) ([]ColumnLineage, error) {
	ctx := &parseContext{
		db:           db,
		dbType:       dbType,
		viewCache:    make(map[string]*ViewAnalysis),
		aliasToTable: make(map[string]string),
	}
	return analyzeSelectFieldWithContext(field, stmt, ctx, parentAnalysis, originalSQL)
}

// analyzeSelectFieldWithContext analyzes a single SELECT field with full context
func analyzeSelectFieldWithContext(field *ast.SelectField, stmt *ast.SelectStmt, ctx *parseContext, parentAnalysis *ViewAnalysis, originalSQL string) ([]ColumnLineage, error) {
	// Check if this is a wildcard (SELECT *)
	if field.WildCard != nil {
		if ctx.db == nil {
			return nil, fmt.Errorf("database connection required to resolve SELECT * columns")
		}
		return analyzeWildcardFieldWithContext(field.WildCard, stmt, ctx, parentAnalysis, originalSQL)
	}

	// Regular field - return single lineage
	lineage := ColumnLineage{
		IsDerived: true, // Assume derived unless proven otherwise
	}

	// Analyze the expression
	expr := field.Expr

	// Check if it's a simple column reference
	if colNameExpr, ok := expr.(*ast.ColumnNameExpr); ok {
		colName := colNameExpr.Name
		tableRef := colName.Table.String() // This might be an alias
		columnName := colName.Name.String()

		// Resolve table reference (might be an alias)
		var actualTableName string
		if tableRef != "" {
			// Check if it's an alias
			if resolved, ok := ctx.aliasToTable[tableRef]; ok {
				actualTableName = resolved
			} else {
				actualTableName = tableRef
			}
		} else {
			// No table name - need to resolve from FROM clause
			if stmt.From != nil {
				tables, _, _ := extractTablesWithAliases(stmt.From.TableRefs, parentAnalysis, ctx)
				if len(tables) == 1 {
					actualTableName = tables[0]
				}
			}
		}

		if actualTableName != "" {
			// Now resolve through views if necessary
			resolvedTable, resolvedColumn, err := resolveColumnLineage(ctx, actualTableName, columnName)
			if err != nil {
				// If resolution fails, fall back to the immediate table
				lineage.SourceTable = actualTableName
				lineage.SourceColumn = columnName
				lineage.IsDerived = false
			} else {
				lineage.SourceTable = resolvedTable
				lineage.SourceColumn = resolvedColumn
				lineage.IsDerived = false
			}
		}
		// If no table resolved, it remains derived
	} else if _, ok := expr.(*ast.AggregateFuncExpr); ok {
		// Aggregate function - always derived
		lineage.IsDerived = true
	} else {
		// Other expression - mark as derived
		lineage.IsDerived = true
	}

	return []ColumnLineage{lineage}, nil
}

// resolveColumnLineage resolves a column through views to find the real base table
// Returns (realTableName, realColumnName, error)
func resolveColumnLineage(ctx *parseContext, tableName, columnName string) (string, string, error) {
	if ctx.db == nil {
		// Can't resolve without DB connection, return as-is
		return tableName, columnName, nil
	}

	// Check if the table is actually a view
	isView, err := checkIsView(ctx.db, ctx.dbType, tableName)
	if err != nil {
		// If we can't determine, assume it's a real table
		return tableName, columnName, nil
	}

	if !isView {
		// It's a real table, we're done
		return tableName, columnName, nil
	}

	// It's a view - need to parse and follow the lineage
	viewAnalysis, err := getOrParseView(ctx, tableName)
	if err != nil {
		// Can't parse view, return as-is
		return tableName, columnName, nil
	}

	// Find the column in the view's output and follow its lineage
	// First, get the view's column names by querying the view
	viewColumns, err := getViewColumnNames(ctx.db, ctx.dbType, tableName)
	if err != nil {
		return tableName, columnName, nil
	}

	// Find the index of our column in the view
	colIndex := -1
	for i, col := range viewColumns {
		if col == columnName {
			colIndex = i
			break
		}
	}

	if colIndex < 0 || colIndex >= len(viewAnalysis.Columns) {
		// Column not found in view analysis
		return tableName, columnName, nil
	}

	lineage := viewAnalysis.Columns[colIndex]
	if lineage.IsDerived {
		// Derived column - no further resolution possible
		return "", "", fmt.Errorf("column %s is derived in view %s", columnName, tableName)
	}

	if lineage.SourceTable == "" || lineage.SourceColumn == "" {
		// Unknown source
		return tableName, columnName, nil
	}

	// Recursively resolve through nested views
	return resolveColumnLineage(ctx, lineage.SourceTable, lineage.SourceColumn)
}

// getOrParseView gets a view analysis from cache or parses it
func getOrParseView(ctx *parseContext, viewName string) (*ViewAnalysis, error) {
	// Check cache first
	if cached, ok := ctx.viewCache[viewName]; ok {
		return cached, nil
	}

	// Get view definition
	viewDef, err := getViewDefinition(ctx.db, ctx.dbType, viewName)
	if err != nil {
		return nil, err
	}

	// Parse the view definition
	analysis, err := ParseViewDefinition(viewDef, ctx.dbType, ctx.db)
	if err != nil {
		return nil, err
	}

	// Cache it
	ctx.viewCache[viewName] = analysis
	return analysis, nil
}

// getViewDefinition gets the SQL definition of a view
func getViewDefinition(db *sql.DB, dbType DatabaseType, viewName string) (string, error) {
	switch dbType {
	case SQLite:
		return getViewDefinitionSQLite(db, viewName)
	case PostgreSQL:
		return getViewDefinitionPostgreSQL(db, viewName)
	case MySQL:
		return getViewDefinitionMySQL(db, viewName)
	default:
		return "", fmt.Errorf("unsupported database type: %v", dbType)
	}
}

// getViewColumnNames gets the column names of a view by querying it
func getViewColumnNames(db *sql.DB, dbType DatabaseType, viewName string) ([]string, error) {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 0", quoteQualified(dbType, viewName))
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return rows.Columns()
}

// analyzeWildcardField handles SELECT * by querying the database for column names (legacy wrapper)
func analyzeWildcardField(wildcard *ast.WildCardField, stmt *ast.SelectStmt, dbType DatabaseType, parentAnalysis *ViewAnalysis, db *sql.DB, originalSQL string) ([]ColumnLineage, error) {
	ctx := &parseContext{
		db:           db,
		dbType:       dbType,
		viewCache:    make(map[string]*ViewAnalysis),
		aliasToTable: make(map[string]string),
	}
	return analyzeWildcardFieldWithContext(wildcard, stmt, ctx, parentAnalysis, originalSQL)
}

// analyzeWildcardFieldWithContext handles SELECT * with full context
func analyzeWildcardFieldWithContext(wildcard *ast.WildCardField, stmt *ast.SelectStmt, ctx *parseContext, parentAnalysis *ViewAnalysis, originalSQL string) ([]ColumnLineage, error) {
	// Extract table names from FROM clause
	if stmt.From == nil || stmt.From.TableRefs == nil {
		return nil, fmt.Errorf("SELECT * requires a FROM clause")
	}

	tables, aliases, err := extractTablesWithAliases(stmt.From.TableRefs, parentAnalysis, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to extract tables: %w", err)
	}

	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables found in FROM clause")
	}

	var lineages []ColumnLineage

	// Handle table.* syntax
	tableRefStr := wildcard.Table.String()
	if tableRefStr != "" {
		// Specific table wildcard (e.g., t.*)
		// Resolve alias if necessary
		actualTableName := tableRefStr
		if resolved, ok := aliases[tableRefStr]; ok {
			actualTableName = resolved
		}

		columns, err := getTableColumns(ctx.db, ctx.dbType, actualTableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get columns for table %s: %w", actualTableName, err)
		}
		for _, colName := range columns {
			// Resolve through views
			resolvedTable, resolvedColumn, resolveErr := resolveColumnLineage(ctx, actualTableName, colName)
			if resolveErr != nil {
				// Column is derived in view
				lineages = append(lineages, ColumnLineage{
					SourceTable:  "",
					SourceColumn: "",
					IsDerived:    true,
				})
			} else {
				lineages = append(lineages, ColumnLineage{
					SourceTable:  resolvedTable,
					SourceColumn: resolvedColumn,
					IsDerived:    false,
				})
			}
		}
	} else {
		// Plain * - get columns from all tables
		if len(tables) == 1 {
			// Single table - get all columns
			tableName := tables[0]
			columns, err := getTableColumns(ctx.db, ctx.dbType, tableName)
			if err != nil {
				return nil, fmt.Errorf("failed to get columns for table %s: %w", tableName, err)
			}
			for _, colName := range columns {
				// Resolve through views
				resolvedTable, resolvedColumn, resolveErr := resolveColumnLineage(ctx, tableName, colName)
				if resolveErr != nil {
					// Column is derived in view
					lineages = append(lineages, ColumnLineage{
						SourceTable:  "",
						SourceColumn: "",
						IsDerived:    true,
					})
				} else {
					lineages = append(lineages, ColumnLineage{
						SourceTable:  resolvedTable,
						SourceColumn: resolvedColumn,
						IsDerived:    false,
					})
				}
			}
		} else {
			// Multiple tables (JOIN) - get columns from each table in order
			// Tables are ordered left-to-right from the FROM clause
			for _, tableName := range tables {
				columns, err := getTableColumns(ctx.db, ctx.dbType, tableName)
				if err != nil {
					return nil, fmt.Errorf("failed to get columns for table %s: %w", tableName, err)
				}
				for _, colName := range columns {
					// Resolve through views
					resolvedTable, resolvedColumn, resolveErr := resolveColumnLineage(ctx, tableName, colName)
					if resolveErr != nil {
						// Column is derived in view
						lineages = append(lineages, ColumnLineage{
							SourceTable:  "",
							SourceColumn: "",
							IsDerived:    true,
						})
					} else {
						lineages = append(lineages, ColumnLineage{
							SourceTable:  resolvedTable,
							SourceColumn: resolvedColumn,
							IsDerived:    false,
						})
					}
				}
			}
		}
	}

	return lineages, nil
}

// getTableColumns gets column names for a table using database-specific queries
func getTableColumns(db *sql.DB, dbType DatabaseType, tableName string) ([]string, error) {
	var query string
	switch dbType {
	case SQLite:
		query = fmt.Sprintf("PRAGMA table_info(%s)", tableName)
	case PostgreSQL:
		// Extract schema if present
		schema := "public"
		rel := tableName
		if dot := strings.IndexByte(rel, '.'); dot != -1 {
			schema = rel[:dot]
			rel = rel[dot+1:]
		}
		query = fmt.Sprintf(`SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' ORDER BY ordinal_position`, schema, rel)
	case MySQL:
		query = fmt.Sprintf(`SELECT column_name FROM information_schema.columns WHERE table_name = '%s' AND table_schema = DATABASE() ORDER BY ordinal_position`, tableName)
	default:
		return nil, fmt.Errorf("unsupported database type: %v", dbType)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var colName string
		// PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
		// For SQLite, we need to scan all columns but only use name
		if dbType == SQLite {
			var cid int
			var colType string
			var notnull int
			var dfltValue sql.NullString
			var pk int
			if err := rows.Scan(&cid, &colName, &colType, &notnull, &dfltValue, &pk); err != nil {
				return nil, err
			}
		} else {
			// PostgreSQL and MySQL return just column_name
			if err := rows.Scan(&colName); err != nil {
				return nil, err
			}
		}
		columns = append(columns, colName)
	}

	return columns, rows.Err()
}

// formatExpr formats an AST expression to a string
func formatExpr(expr ast.ExprNode) string {
	switch e := expr.(type) {
	case *ast.ColumnNameExpr:
		if e.Name.Table.String() != "" {
			return fmt.Sprintf("%s.%s", e.Name.Table.String(), e.Name.Name.String())
		}
		return e.Name.Name.String()
	case *ast.AggregateFuncExpr:
		return e.F // Function name
	default:
		return fmt.Sprintf("%T", expr)
	}
}
