package dblib

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// DuckDBHandler implements DatabaseHandler for DuckDB databases.
type DuckDBHandler struct{}

// getBestKeyDuckDB identifies the best key for a DuckDB table.
// DuckDB supports information_schema similar to PostgreSQL.
// Ranking: primary key > unique NOT NULL > fewer columns > shorter > earlier.
func getBestKeyDuckDB(db *sql.DB, tableName string) ([]string, error) {
	type keyCandidate struct {
		cols       []string
		isPK       bool
		numCols    int
		totalSize  int
		minOrdinal int
	}

	// Get column metadata
	colInfo := make(map[string]struct {
		dataType string
		length   int
		ordinal  int
		notNull  bool
	})
	colQuery := `SELECT column_name, data_type,
	                    COALESCE(character_maximum_length, -1) AS max_len,
	                    ordinal_position, is_nullable
	             FROM information_schema.columns
	             WHERE table_name = ?`
	colRows, err := db.Query(colQuery, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query column metadata: %w", err)
	}
	defer colRows.Close()
	for colRows.Next() {
		var name, dtype, isNullable string
		var maxLen, ordinal int
		if err := colRows.Scan(&name, &dtype, &maxLen, &ordinal, &isNullable); err != nil {
			continue
		}
		notNull := strings.ToLower(isNullable) == "no"
		colInfo[name] = struct {
			dataType string
			length   int
			ordinal  int
			notNull  bool
		}{dtype, maxLen, ordinal, notNull}
	}
	colRows.Close()

	var candidates []keyCandidate

	// Check for primary key using information_schema
	pkQuery := `SELECT column_name
	            FROM information_schema.key_column_usage
	            WHERE table_name = ? AND constraint_name LIKE 'PRIMARY%'
	            ORDER BY ordinal_position`
	pkRows, err := db.Query(pkQuery, tableName)
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

	// Check for unique constraints
	uQuery := `SELECT constraint_name, column_name
	           FROM information_schema.key_column_usage
	           WHERE table_name = ? AND constraint_name NOT LIKE 'PRIMARY%'
	           ORDER BY constraint_name, ordinal_position`
	uRows, err := db.Query(uQuery, tableName)
	if err == nil {
		defer uRows.Close()
		constraintCols := make(map[string][]string)
		for uRows.Next() {
			var constraintName, colName string
			if err := uRows.Scan(&constraintName, &colName); err != nil {
				continue
			}
			constraintCols[constraintName] = append(constraintCols[constraintName], colName)
		}
		uRows.Close()

		for _, cols := range constraintCols {
			if len(cols) == 0 {
				continue
			}
			// Check all columns are NOT NULL
			allNotNull := true
			totalSize := 0
			minOrd := int(^uint(0) >> 1) // max int
			for _, col := range cols {
				if info, ok := colInfo[col]; ok {
					if !info.notNull {
						allNotNull = false
						break
					}
					totalSize += sizeOf(info.dataType, info.length)
					if info.ordinal < minOrd {
						minOrd = info.ordinal
					}
				} else {
					allNotNull = false
					break
				}
			}

			if !allNotNull {
				continue
			}

			candidates = append(candidates, keyCandidate{
				cols:       cols,
				isPK:       false,
				numCols:    len(cols),
				totalSize:  totalSize,
				minOrdinal: minOrd,
			})
		}
	}

	if len(candidates) == 0 {
		return []string{}, nil
	}

	// Sort by: isPK desc, numCols asc, totalSize asc, minOrdinal asc
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].isPK != candidates[j].isPK {
			return candidates[i].isPK
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

// DatabaseHandler interface implementation for DuckDBHandler

// CheckIsView returns true if the named relation is a view, false if it's a table.
func (h *DuckDBHandler) CheckIsView(db *sql.DB, relationName string) (bool, error) {
	var isView bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.views
			WHERE table_name = ?
		)`, relationName).Scan(&isView)
	return isView, err
}

// LoadColumns loads column metadata for a DuckDB table.
func (h *DuckDBHandler) LoadColumns(db *sql.DB, tableName string) ([]Column, map[string]int, error) {
	query := `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = ?
		ORDER BY ordinal_position`

	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var columns []Column
	columnIndex := make(map[string]int)

	for rows.Next() {
		var col Column
		col.Reference = -1
		var nullable string

		err = rows.Scan(&col.Name, &col.Type, &nullable)
		if err != nil {
			return nil, nil, err
		}

		col.Nullable = nullable == "YES"
		col.Table = tableName
		col.BaseColumn = col.Name

		idx := len(columns)
		columns = append(columns, col)
		columnIndex[col.Name] = idx
	}

	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	return columns, columnIndex, nil
}

// LoadForeignKeys loads foreign key constraints for a DuckDB table.
func (h *DuckDBHandler) LoadForeignKeys(db *sql.DB, dbType DatabaseType, tableName string,
	columnIndex map[string]int, columns []Column) ([]Reference, []Column, error) {
	// DuckDB foreign key support is limited - return empty for now
	updatedColumns := make([]Column, len(columns))
	copy(updatedColumns, columns)
	return []Reference{}, updatedColumns, nil
}

// LoadEnumAndCustomTypes is a no-op for DuckDB (minimal enum support).
func (h *DuckDBHandler) LoadEnumAndCustomTypes(db *sql.DB, tableName string, columns []Column) ([]Column, error) {
	// DuckDB has enum support but it's not widely used yet
	return columns, nil
}

// GetViewDefinition retrieves the SQL definition of a DuckDB view.
func (h *DuckDBHandler) GetViewDefinition(db *sql.DB, viewName string) (string, error) {
	var sqlDef string
	err := db.QueryRow(`
		SELECT view_definition
		FROM information_schema.views
		WHERE table_name = ?`, viewName).Scan(&sqlDef)
	if err != nil {
		return "", fmt.Errorf("failed to get view definition: %w", err)
	}
	return sqlDef, nil
}

// GetBestKey identifies the best key column(s) for a DuckDB table.
func (h *DuckDBHandler) GetBestKey(db *sql.DB, tableName string) ([]string, error) {
	return getBestKeyDuckDB(db, tableName)
}

// GetShortestLookupKey returns the best lookup key for a DuckDB table.
func (h *DuckDBHandler) GetShortestLookupKey(db *sql.DB, tableName string) ([]string, error) {
	// For now, delegate to GetBestKey
	// This could be enhanced with unique constraint analysis similar to PostgreSQL
	return h.GetBestKey(db, tableName)
}

// QuoteIdent quotes an identifier (table/column name) for DuckDB using double quotes.
func (h *DuckDBHandler) QuoteIdent(ident string) string {
	// Check if safe to leave unquoted
	if isSafeUnquotedIdent(ident) {
		return ident
	}
	// Escape double quotes by doubling them
	escaped := strings.ReplaceAll(ident, "\"", "\"\"")
	return "\"" + escaped + "\""
}

// Placeholder returns the parameter placeholder for DuckDB (always "?").
func (h *DuckDBHandler) Placeholder(position int) string {
	return "?"
}

