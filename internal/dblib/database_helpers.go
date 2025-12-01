package dblib

import (
	"database/sql"
	"fmt"
	"strings"
)

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
		quotedSortCol := SortColumn{Name: quoteIdent(dbType, sortCol.Name), Asc: sortCol.Asc}
		sortColString = quotedSortCol.String(scrollDown)
		length += len(sortColString)
	}

	var builder strings.Builder
	builder.Grow(length)

	builder.WriteString("SELECT ")
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdent(dbType, col)
	}
	builder.WriteString(strings.Join(quotedColumns, ", "))
	builder.WriteString(" FROM ")
	builder.WriteString(quoteQualified(dbType, tableName))

	if hasParams {
		builder.WriteString(" WHERE ")

		// Build row value comparison for key columns
		colExpr, placeholders := buildRowValueExpression(dbType, keyCols, 1)
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
	query := builder.String()
	return query, nil
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

// quoteQualified splits on '.' and quotes each identifier part independently.
func quoteQualified(dbType DatabaseType, qualified string) string {
	parts := strings.Split(qualified, ".")
	for i, p := range parts {
		parts[i] = quoteIdent(dbType, p)
	}
	return strings.Join(parts, ".")
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

// columns is the list of columns to display in the preview based on configs
func GetForeignRow(db *sql.DB, table *Relation, foreignKey map[string]any, columns []string) (map[string]any, error) {
	// choose non-key columns
	columnNames := make([]string, 0, len(columns))
	for _, col := range columns {
		columnNames = append(columnNames, col)
	}

	whereParts := make([]string, 0, len(foreignKey))
	args := make([]any, 0, len(foreignKey))
	placeholderPos := 1
	for col := range foreignKey {
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", quoteIdent(table.DBType, col), table.placeholder(placeholderPos)))
		args = append(args, foreignKey[col])
		placeholderPos++
	}
	quotedColumns := make([]string, len(columnNames))
	for i, col := range columnNames {
		quotedColumns[i] = quoteIdent(table.DBType, col)
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s", strings.Join(quotedColumns, ", "), quoteQualified(table.DBType, table.Name), strings.Join(whereParts, " AND "))
	row := db.QueryRow(query, args...)
	values := make([]any, len(columnNames))
	// scan into pointers
	scanArgs := make([]any, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	if err := row.Scan(scanArgs...); err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}
	result := make(map[string]any, len(columnNames))
	for i, col := range columnNames {
		result[col] = *scanArgs[i].(*any)
	}
	return result, nil
}

// CompareRowPosition compares the position of updatedKeys relative to viewport boundaries
// Returns (isAbove, isBelow, error) where:
// - isAbove=true means the updated row is above the first visible row
// - isBelow=true means the updated row is below the last visible row
// If both are false, the row is within the current viewport
func CompareRowPosition(db *sql.DB, table *Relation, sortCol *SortColumn, updatedKeys, firstKeys, lastKeys []any) (bool, bool, error) {
	if len(updatedKeys) == 0 || len(firstKeys) == 0 || len(lastKeys) == 0 {
		return false, false, fmt.Errorf("key data cannot be empty")
	}
	if len(table.Key) == 0 {
		return false, false, fmt.Errorf("table has no key columns")
	}

	// Build comparison using row value syntax for composite keys
	// Query: SELECT (updated) < (first), (updated) > (last)
	numCols := len(updatedKeys)

	// Build placeholder tuples based on database type
	// For positional placeholders: reuse $1,$2 for both comparisons
	// Args: [updated..., first..., last...]
	nextPlaceholder := func(pos int) string {
		if databaseFeatures[table.DBType].positionalPlaceholder {
			return fmt.Sprintf("$%d", pos)
		}
		return "?"
	}

	updatedPlaceholders := make([]string, numCols)
	firstPlaceholders := make([]string, numCols)
	lastPlaceholders := make([]string, numCols)

	for i := 0; i < numCols; i++ {
		updatedPlaceholders[i] = nextPlaceholder(i + 1)
		firstPlaceholders[i] = nextPlaceholder(i + 1 + numCols)
		lastPlaceholders[i] = nextPlaceholder(i + 1 + 2*numCols)
	}

	// Build tuples - for single column, use scalar; for multiple, use row value syntax
	var updatedExpr, firstExpr, lastExpr string
	if numCols == 1 {
		updatedExpr = updatedPlaceholders[0]
		firstExpr = firstPlaceholders[0]
		lastExpr = lastPlaceholders[0]
	} else {
		updatedExpr = "(" + strings.Join(updatedPlaceholders, ", ") + ")"
		firstExpr = "(" + strings.Join(firstPlaceholders, ", ") + ")"
		lastExpr = "(" + strings.Join(lastPlaceholders, ", ") + ")"
	}

	// Build the comparison query
	query := fmt.Sprintf("SELECT %s < %s AS is_above, %s > %s AS is_below",
		updatedExpr, firstExpr, updatedExpr, lastExpr)
	// Combine all arguments: updated, first, last
	args := make([]any, 0, 3*numCols)
	args = append(args, updatedKeys...)
	args = append(args, firstKeys...)
	args = append(args, updatedKeys...)
	args = append(args, lastKeys...)

	// Execute query
	var isAbove, isBelow bool
	err := db.QueryRow(query, args...).Scan(&isAbove, &isBelow)
	if err != nil {
		return false, false, fmt.Errorf("failed to compare row positions: %w", err)
	}

	return isAbove, isBelow, nil
}

// buildRowValueExpression builds column and placeholder expressions for row value comparison.
// For single column keys: returns "key1", []string{"?"} (or "$1" for positional placeholders)
// For multi-column keys: returns "(key1, key2)", []string{"?", "?"} (or "$1", "$2" for positional)
//
// This enables correct lexicographic ordering comparison in WHERE clauses using row value syntax:
//   Single: WHERE key1 > ?
//   Multi:  WHERE (key1, key2) > (?, ?)
func buildRowValueExpression(dbType DatabaseType, keyCols []string, startPos int) (columnExpr string, placeholders []string) {
	// Generate placeholders with positions
	nextPlaceholder := func(pos int) string {
		if databaseFeatures[dbType].positionalPlaceholder {
			return fmt.Sprintf("$%d", pos)
		}
		return "?"
	}

	placeholders = make([]string, len(keyCols))
	for i := 0; i < len(keyCols); i++ {
		placeholders[i] = nextPlaceholder(startPos + i)
	}

	// Quote column names
	quotedCols := make([]string, len(keyCols))
	for i, col := range keyCols {
		quotedCols[i] = quoteIdent(dbType, col)
	}

	// Build expression
	if len(keyCols) == 1 {
		// Scalar: just the column name
		columnExpr = quotedCols[0]
	} else {
		// Tuple: (col1, col2, col3)
		columnExpr = "(" + strings.Join(quotedCols, ", ") + ")"
	}

	return columnExpr, placeholders
}

// buildPlaceholderExpression builds the placeholder side of a row value comparison.
// For single value: returns "?" or "$1"
// For multiple values: returns "(?, ?, ?)" or "($1, $2, $3)"
func buildPlaceholderExpression(placeholders []string) string {
	if len(placeholders) == 1 {
		return placeholders[0]
	}
	return "(" + strings.Join(placeholders, ", ") + ")"
}
