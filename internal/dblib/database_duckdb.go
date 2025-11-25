package dblib

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

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
