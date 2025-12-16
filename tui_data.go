package main

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"time"
)

// e.table.SetData based on e.records and e.pointer
func (e *Editor) renderData() {
	// number of rows to send to TableView
	rowCount := len(e.buffer)

	normalizedRows := make([]Row, rowCount)
	for i := 0; i < rowCount; i++ {
		ptr := (i + e.pointer) % len(e.buffer)
		normalizedRows[i] = e.buffer[ptr] // Reference to Row, not a copy
	}

	// If in insert mode, append insert row with RowStateInsert
	if len(e.insertRow) > 0 {
		insertRow := Row{
			state:    RowStateInsert,
			data:     e.insertRow,
			modified: nil,
		}
		normalizedRows = append(normalizedRows, insertRow)
	}

	e.table.SetDataReferences(normalizedRows)
}

// extractKeys returns a copy of the key values from a row
func (e *Editor) extractKeys(row []any) []any {
	if row == nil || len(e.relation.Key) == 0 {
		return nil
	}
	keys := make([]any, len(e.relation.Key))
	for i, keyIdx := range e.relation.Key {
		if keyIdx < len(row) {
			keys[i] = row[keyIdx]
		}
	}
	return keys
}

// keysEqual compares two key slices for equality
func keysEqual(k1, k2 []any) bool {
	if len(k1) != len(k2) {
		return false
	}
	for i := range k1 {
		if k1[i] != k2[i] {
			return false
		}
	}
	return true
}

// getViewportBoundaryKeys returns the key values of the first and last visible rows
func (e *Editor) getViewportBoundaryKeys() (firstKeys, lastKeys []any) {
	if len(e.buffer) == 0 {
		return nil, nil
	}

	// Get first visible row
	firstRow := e.buffer[e.pointer]
	if firstRow.data != nil {
		firstKeys = e.extractKeys(firstRow.data)
	}

	// Get last visible row (accounting for nil sentinel at end)
	lastIdx := e.lastRowIdx()
	lastRow := e.buffer[lastIdx]

	// If last row is nil sentinel, try previous row
	if lastRow.data == nil && lastIdx != e.pointer {
		lastIdx = (lastIdx - 1 + len(e.buffer)) % len(e.buffer)
		lastRow = e.buffer[lastIdx]
	}

	if lastRow.data != nil {
		lastKeys = e.extractKeys(lastRow.data)
	}

	return firstKeys, lastKeys
}

// diffRows compares two rows and returns the indices of columns that differ
func diffRows(oldRow, newRow []any) []int {
	if len(oldRow) != len(newRow) {
		return nil
	}
	var modified []int
	for i := range oldRow {
		if oldRow[i] != newRow[i] {
			modified = append(modified, i)
		}
	}
	return modified
}

// rowsEqual checks if two slices of Rows are equal by comparing their data
func rowsEqual(rows1, rows2 []Row) bool {
	if len(rows1) != len(rows2) {
		return false
	}
	for i := range rows1 {
		if len(rows1[i].data) != len(rows2[i].data) {
			return false
		}
		for j := range rows1[i].data {
			if rows1[i].data[j] != rows2[i].data[j] {
				return false
			}
		}
	}
	return true
}

// findMatchingIndexes finds all pairs of matching rows between previousRows and currentRows
// based on primary key equality. Returns a slice of [2]int where [0] is the index in previousRows
// and [1] is the index in currentRows.
func (e *Editor) findMatchingIndexes(previousRows, currentRows []Row) [][2]int {
	var matches [][2]int

	// Build a map of current row keys to their indexes for faster lookup
	currentKeyMap := make(map[string]int)
	for i, row := range currentRows {
		keys := e.extractKeys(row.data)
		if keys != nil {
			keyStr := fmt.Sprintf("%v", keys)
			currentKeyMap[keyStr] = i
		}
	}

	// Find matches by looking up each previous row's key in the current map
	for i, prevRow := range previousRows {
		prevKeys := e.extractKeys(prevRow.data)
		if prevKeys != nil {
			keyStr := fmt.Sprintf("%v", prevKeys)
			if currIdx, exists := currentKeyMap[keyStr]; exists {
				matches = append(matches, [2]int{i, currIdx})
			}
		}
	}

	return matches
}

// checkRowsExistInDB checks if rows with the given primary keys still exist in the database
// Returns a slice of booleans, one for each key, indicating existence
func (e *Editor) checkRowsExistInDB(keys [][]any) ([]bool, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	// Build the IN clause query
	keyColCount := len(e.relation.Key)
	if keyColCount == 0 {
		return nil, fmt.Errorf("no primary key defined for relation")
	}

	// Get key column names
	keyColNames := make([]string, len(e.relation.Key))
	for i, keyIdx := range e.relation.Key {
		if keyIdx < len(e.relation.Columns) {
			keyColNames[i] = e.relation.Columns[keyIdx].Name
		}
	}

	// For single-column keys, we can use a simple IN clause
	// For multi-column keys, we need to use OR conditions
	var query string
	var args []any

	if keyColCount == 1 {
		// Simple case: single column key
		keyColName := keyColNames[0]
		query = fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (",
			keyColName, e.relation.Name, keyColName)
		placeholders := make([]string, len(keys))
		for i, key := range keys {
			placeholders[i] = "?"
			args = append(args, key[0])
		}
		query += fmt.Sprintf("%s)", strings.Join(placeholders, ", "))
	} else {
		// Complex case: multi-column key
		query = fmt.Sprintf("SELECT %s FROM %s WHERE ",
			strings.Join(keyColNames, ", "), e.relation.Name)
		conditions := make([]string, len(keys))
		for i, key := range keys {
			keyConditions := make([]string, keyColCount)
			for j := 0; j < keyColCount; j++ {
				keyConditions[j] = fmt.Sprintf("%s = ?", keyColNames[j])
				args = append(args, key[j])
			}
			conditions[i] = fmt.Sprintf("(%s)", strings.Join(keyConditions, " AND "))
		}
		query += strings.Join(conditions, " OR ")
	}

	rows, err := e.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Build a set of existing keys
	existingKeys := make(map[string]bool)
	scanTargets := make([]any, keyColCount)
	scanPtrs := make([]any, keyColCount)
	for i := range scanTargets {
		scanPtrs[i] = &scanTargets[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			return nil, err
		}
		keyStr := fmt.Sprintf("%v", scanTargets)
		existingKeys[keyStr] = true
	}

	// Check each input key against the set
	result := make([]bool, len(keys))
	for i, key := range keys {
		keyStr := fmt.Sprintf("%v", key)
		result[i] = existingKeys[keyStr]
	}

	return result, nil
}

// diffAndPopulateBuffer performs the diffing algorithm between previousRows and currentRows
// and populates the buffer with appropriate state markers (Normal/New/Deleted/Modified)
func (e *Editor) diffAndPopulateBuffer(currentRows []Row) {
	// Use separate ptr variable instead of incrementing e.pointer
	ptr := e.pointer
	bufferSize := len(e.buffer)

	// Find all matching indexes
	matches := e.findMatchingIndexes(e.previousRows, currentRows)

	// Track which indexes have been processed
	prevIdx := 0
	currIdx := 0

	// Helper to add a row to buffer and advance ptr
	addToBuffer := func(row Row) bool {
		if ptr >= bufferSize {
			return false // Buffer full
		}
		e.buffer[ptr] = row
		ptr++
		return true
	}

	// Process sequences between matches
	for matchNum, match := range matches {
		prevMatchIdx := match[0]
		currMatchIdx := match[1]

		// Process deleted rows (only in previousRows)
		for i := prevIdx; i < prevMatchIdx; i++ {
			deletedRow := e.previousRows[i]
			deletedRow.state = RowStateDeleted
			deletedRow.modified = nil
			if !addToBuffer(deletedRow) {
				return // Buffer full
			}
		}

		// Process inserted rows (only in currentRows) before this match
		for i := currIdx; i < currMatchIdx; i++ {
			insertedRow := currentRows[i]
			insertedRow.state = RowStateNew
			insertedRow.modified = nil
			if !addToBuffer(insertedRow) {
				return // Buffer full
			}
		}

		// Process the matched row (check for modifications)
		matchedRow := currentRows[currMatchIdx]
		matchedRow.state = RowStateNormal
		matchedRow.modified = diffRows(e.previousRows[prevMatchIdx].data, currentRows[currMatchIdx].data)
		if !addToBuffer(matchedRow) {
			return // Buffer full
		}

		// Move indexes forward
		prevIdx = prevMatchIdx + 1
		currIdx = currMatchIdx + 1

		// Special handling for last sequence
		if matchNum == len(matches)-1 {
			// Check if there are remaining rows in previousRows
			if prevIdx < len(e.previousRows) {
				// Build list of keys to check
				var keysToCheck [][]any
				for i := prevIdx; i < len(e.previousRows); i++ {
					key := e.extractKeys(e.previousRows[i].data)
					if key != nil {
						keysToCheck = append(keysToCheck, key)
					}
				}

				// Check if these rows still exist in DB
				if len(keysToCheck) > 0 {
					exists, err := e.checkRowsExistInDB(keysToCheck)
					if err == nil {
						// Add deleted rows for those that still exist in DB
						keyIdx := 0
						for i := prevIdx; i < len(e.previousRows); i++ {
							if keyIdx < len(exists) && exists[keyIdx] {
								deletedRow := e.previousRows[i]
								deletedRow.state = RowStateDeleted
								deletedRow.modified = nil
								if !addToBuffer(deletedRow) {
									return // Buffer full
								}
							}
							keyIdx++
						}
					}
				}
			}

			// Fill remaining buffer with inserted rows from currentRows
			for i := currIdx; i < len(currentRows); i++ {
				insertedRow := currentRows[i]
				insertedRow.state = RowStateNew
				insertedRow.modified = nil
				if !addToBuffer(insertedRow) {
					return // Buffer full
				}
			}

			// Done processing
			return
		}
	}

	// If there were no matches, handle all rows
	if len(matches) == 0 {
		// All previousRows are deleted (if they exist in DB)
		if len(e.previousRows) > 0 {
			var keysToCheck [][]any
			for _, row := range e.previousRows {
				key := e.extractKeys(row.data)
				if key != nil {
					keysToCheck = append(keysToCheck, key)
				}
			}

			if len(keysToCheck) > 0 {
				exists, err := e.checkRowsExistInDB(keysToCheck)
				if err == nil {
					for i, row := range e.previousRows {
						if i < len(exists) && exists[i] {
							deletedRow := row
							deletedRow.state = RowStateDeleted
							deletedRow.modified = nil
							if !addToBuffer(deletedRow) {
								return // Buffer full
							}
						}
					}
				}
			}
		}

		// All currentRows are new
		for _, row := range currentRows {
			insertedRow := row
			insertedRow.state = RowStateNew
			insertedRow.modified = nil
			if !addToBuffer(insertedRow) {
				return // Buffer full
			}
		}
	}
}

// refresh reloads data from the current position and applies change tracking
func (e *Editor) refresh() error {
	if e.relation == nil || e.relation.DB == nil {
		return fmt.Errorf("no database connection available")
	}

	// Get current row ID for refresh anchor
	if len(e.buffer) == 0 || e.buffer[e.pointer].data == nil {
		return nil // Nothing to refresh
	}

	// Extract key values from the row using the key column indices
	id := make([]any, len(e.relation.Key))
	for i, keyIdx := range e.relation.Key {
		id[i] = e.buffer[e.pointer].data[keyIdx]
	}

	// Close existing query before refresh
	e.queryMu.Lock()
	if e.query != nil {
		e.query.Close()
		e.query = nil
	}
	e.queryMu.Unlock()

	// Stop refresh timer during refresh operation
	e.stopRefreshTimer()

	// Prepare query
	headers := e.table.GetHeaders()
	selectCols := make([]string, len(headers))
	for i, col := range headers {
		selectCols[i] = col.Name
	}

	colCount := len(headers)
	if colCount == 0 {
		return nil
	}

	// Query from current position (fromTop=true)
	rows, err := e.relation.QueryRows(selectCols, nil, id, true, true)
	if err != nil {
		return err
	}

	// Scan all rows into local currentRows
	e.pointer = 0
	scanTargets := make([]any, colCount)
	var currentRows []Row
	for rows.Next() && len(currentRows) < e.table.rowsHeight {
		row := make([]any, colCount)
		for j := 0; j < colCount; j++ {
			scanTargets[j] = &row[j]
		}
		if err := rows.Scan(scanTargets...); err != nil {
			rows.Close()
			return err
		}
		currentRows = append(currentRows, Row{
			state:    RowStateNormal,
			data:     row,
			modified: nil,
		})
	}
	rows.Close()

	// Apply diff tracking if previous data exists and differs
	// if e.previousRows != nil && !rowsEqual(e.previousRows, currentRows) {
	// 	e.diffAndPopulateBuffer(currentRows)
	// } else {
	// No diffing needed - just populate buffer normally
	for i := 0; i < len(currentRows) && i < len(e.buffer); i++ {
		e.buffer[i] = currentRows[i]
	}
	// Mark end with empty row if we didn't fill the buffer
	if len(currentRows) < len(e.buffer) {
		e.buffer[len(currentRows)] = BottomBorderRow
	}
	// }

	// Clone currentRows to previousRows for next refresh
	e.previousRows = make([]Row, len(currentRows))
	copy(e.previousRows, currentRows)

	e.renderData()
	e.table.Select(e.table.selectedRow, e.table.selectedCol)
	e.startRefreshTimer()

	return nil
}

// id can be nil, in which case load from the top or bottom
func (e *Editor) loadFromRowId(id []any, fromTop bool, focusColumn int) error {
	if e.relation == nil || e.relation.DB == nil {
		return fmt.Errorf("no database connection available")
	}

	// Stop refresh timer when starting a new query
	e.stopRefreshTimer()

	headers := e.table.GetHeaders()
	selectCols := make([]string, len(headers))
	for i, col := range headers {
		selectCols[i] = col.Name
	}

	colCount := len(headers)
	if colCount == 0 {
		return nil
	}

	var rows *sql.Rows
	var err error
	if fromTop {
		// Load from top: use QueryRows with inclusive true, scrollDown true
		rows, err = e.relation.QueryRows(selectCols, nil, id, true, true)
		if err != nil {
			return err
		}
		e.queryMu.Lock()
		e.query = rows
		e.scrollDown = true
		e.queryMu.Unlock()
		e.startRowsTimer()

		// Scan rows into e.records starting from pointer
		e.pointer = 0
		scanTargets := make([]any, colCount)

		// Scan all rows from database into local currentRows
		var currentRows []Row
		for rows.Next() && len(currentRows) < e.table.rowsHeight {
			row := make([]any, colCount)
			for j := 0; j < colCount; j++ {
				scanTargets[j] = &row[j]
			}
			if err := rows.Scan(scanTargets...); err != nil {
				return err
			}
			currentRows = append(currentRows, Row{
				state:    RowStateNormal,
				data:     row,
				modified: nil,
			})
		}

		// Populate buffer normally
		for i := 0; i < len(currentRows) && i < len(e.buffer); i++ {
			e.buffer[i] = currentRows[i]
		}
		// Mark end with empty row if we didn't fill the buffer
		if len(currentRows) < len(e.buffer) {
			e.buffer[len(currentRows)] = BottomBorderRow
		}

		// Set previousRows to currentRows
		e.previousRows = currentRows
	} else {
		// Load from bottom: use QueryRows with inclusive true, scrollDown false
		rows, err = e.relation.QueryRows(selectCols, nil, id, true, false)
		if err != nil {
			return err
		}
		e.queryMu.Lock()
		e.query = rows
		e.scrollDown = false
		e.queryMu.Unlock()
		e.startRowsTimer()

		// Scan rows into e.records in reverse, starting from end of buffer
		e.pointer = 0
		scanTargets := make([]any, colCount)

		// Scan all rows from database into local currentRows
		var currentRows []Row
		for rows.Next() {
			row := make([]any, colCount)
			for j := 0; j < colCount; j++ {
				scanTargets[j] = &row[j]
			}
			if err := rows.Scan(scanTargets...); err != nil {
				return err
			}
			currentRows = append(currentRows, Row{
				state:    RowStateNormal,
				data:     row,
				modified: nil,
			})
			if len(currentRows) >= len(e.buffer)-1 {
				break
			}
		}

		// Reverse currentRows for fromBottom
		slices.Reverse(currentRows)

		// Populate buffer normally
		for i := 0; i < len(currentRows) && i < len(e.buffer); i++ {
			e.buffer[i] = currentRows[i]
		}
		// Mark end with empty row if we didn't fill the buffer
		if len(currentRows) < len(e.buffer) {
			e.buffer[len(currentRows)] = BottomBorderRow
		}

		// Set previousRows to currentRows
		e.previousRows = currentRows

		e.pointer = 0
	}
	e.renderData()

	// Focus on the specified column
	if fromTop {
		e.table.Select(e.table.selectedRow, focusColumn)
	} else {
		e.table.Select(len(e.buffer)-1, focusColumn)
	}

	return nil
}

// nextRows fetches the next i rows from e.rows and resets the auto-close timer
// when there are no more rows, adds a nil sentinel to mark the end
// returns bool, err. bool if the edge of table is reached
func (e *Editor) nextRows(i int) (bool, error) {
	// Only reuse query if it's in the correct direction (scrollDown)
	// Check if we're already at the end (last record is nil)
	if e.isAtBottom() {
		return true, nil // No-op, already at end of data
	}
	// this is a problem

	needNewQuery := e.query == nil || !e.scrollDown
	if needNewQuery && e.query != nil {
		// Close existing query if it's in the wrong direction
		e.queryMu.Lock()
		e.query.Close()
		e.query = nil
		e.queryMu.Unlock()
	}

	if needNewQuery {
		// Stop refresh timer when starting a new query
		e.stopRefreshTimer()

		params := make([]any, len(e.relation.Key))
		lastRecordIdx := (e.pointer - 1 + len(e.buffer)) % len(e.buffer)
		if e.buffer[lastRecordIdx].data == nil {
			return false, nil // Can't query from nil record
		}
		for i, keyIdx := range e.relation.Key {
			if keyIdx < len(e.buffer[lastRecordIdx].data) {
				params[i] = e.buffer[lastRecordIdx].data[keyIdx]
			}
		}
		headers := e.table.GetHeaders()
		selectCols := make([]string, len(headers))
		for i, col := range headers {
			selectCols[i] = col.Name
		}
		newQuery, err := e.relation.QueryRows(selectCols, nil, params, false, true)
		if err != nil {
			return false, err
		}
		e.query = newQuery
		e.scrollDown = true
		e.startRowsTimer()
	}

	// Signal timer reset
	if e.rowsTimerReset != nil {
		select {
		case e.rowsTimerReset <- struct{}{}:
		default:
		}
	}

	colCount := len(e.table.GetHeaders())
	if colCount == 0 {
		return false, nil
	}

	// Get a local reference to the query to avoid holding the lock during scanning
	e.queryMu.Lock()
	query := e.query
	e.queryMu.Unlock()

	if query == nil {
		return false, fmt.Errorf("query is nil")
	}

	scanTargets := make([]any, colCount)

	rowsFetched := 0
	for ; rowsFetched < i && query.Next(); rowsFetched++ {
		row := make([]any, colCount)
		for j := 0; j < colCount; j++ {
			scanTargets[j] = &row[j]
		}
		if err := query.Scan(scanTargets...); err != nil {
			return false, err
		}
		bufferIdx := (e.pointer + rowsFetched) % len(e.buffer)
		e.buffer[bufferIdx] = Row{state: RowStateNormal, data: row}
	}
	// new pointer position
	e.incrPtr(rowsFetched)
	// If we fetched fewer rows than requested, we've reached the end
	if rowsFetched < i {
		e.incrPtr(1)
		e.buffer[e.lastRowIdx()] = BottomBorderRow // Mark end of data
	}

	// Update previousRows to match current buffer view (for accurate diff on next refresh)
	e.previousRows = make([]Row, 0, len(e.buffer))
	for i := 0; i < len(e.buffer); i++ {
		row := e.buffer[(e.pointer+i)%len(e.buffer)]
		if row.data == nil {
			break // Stop at nil sentinel
		}
		e.previousRows = append(e.previousRows, row)
	}

	e.renderData()
	return rowsFetched < i, query.Err()
}

func (e *Editor) lastRowIdx() int {
	return (e.pointer + len(e.buffer) - 1) % len(e.buffer)
}

// isAtBottom returns true if the table is currently at the bottom of the data
// (i.e., the last row in the buffer is nil, indicating we've reached the end)
func (e *Editor) isAtBottom() bool {
	return e.buffer[e.lastRowIdx()].data == nil
}

func (e *Editor) incrPtr(n int) {
	e.pointer = (e.pointer + n) % len(e.buffer)
}

// prevRows fetches the previous i rows (scrolling backwards in the circular buffer)
// returns whether rows were moved (false means you'ved the edge)
func (e *Editor) prevRows(i int) (bool, error) {
	// Only reuse query if it's in the correct direction (!scrollDown)
	e.queryMu.Lock()
	needNewQuery := e.query == nil || e.scrollDown
	if needNewQuery && e.query != nil {
		// Close existing query if it's in the wrong direction
		e.query.Close()
		e.query = nil
	}
	e.queryMu.Unlock()

	if needNewQuery {
		// Stop refresh timer when starting a new query
		e.stopRefreshTimer()
		if len(e.buffer) == 0 || e.buffer[e.pointer].data == nil {
			return false, nil // Can't query from nil or empty records
		}
		params := make([]any, len(e.relation.Key))
		for i, keyIdx := range e.relation.Key {
			if keyIdx < len(e.buffer[e.pointer].data) {
				params[i] = e.buffer[e.pointer].data[keyIdx]
			}
		}
		headers := e.table.GetHeaders()
		selectCols := make([]string, len(headers))
		for i, col := range headers {
			selectCols[i] = col.Name
		}
		newQuery, err := e.relation.QueryRows(selectCols, nil, params, false, false)
		if err != nil {
			return false, err
		}
		e.queryMu.Lock()
		e.query = newQuery
		e.scrollDown = false
		e.queryMu.Unlock()
		e.startRowsTimer()
	}

	// Signal timer reset
	if e.rowsTimerReset != nil {
		select {
		case e.rowsTimerReset <- struct{}{}:
		default:
		}
	}

	colCount := len(e.table.GetHeaders())
	if colCount == 0 {
		return false, nil
	}

	// Get a local reference to the query to avoid holding the lock during scanning
	e.queryMu.Lock()
	query := e.query
	e.queryMu.Unlock()

	if query == nil {
		return false, fmt.Errorf("query is nil")
	}

	scanTargets := make([]any, colCount)

	rowsFetched := 0
	for ; rowsFetched < i && query.Next(); rowsFetched++ {
		row := make([]any, colCount)
		for j := 0; j < colCount; j++ {
			scanTargets[j] = &row[j]
		}
		if err := query.Scan(scanTargets...); err != nil {
			return false, err
		}
		e.pointer = e.lastRowIdx() // Move pointer backwards in the circular buffer
		e.buffer[e.pointer] = Row{state: RowStateNormal, data: row}
	}

	// Update previousRows to match current buffer view (for accurate diff on next refresh)
	e.previousRows = make([]Row, 0, len(e.buffer))
	for i := 0; i < len(e.buffer); i++ {
		row := e.buffer[(e.pointer+i)%len(e.buffer)]
		if row.data == nil {
			break // Stop at nil sentinel
		}
		e.previousRows = append(e.previousRows, row)
	}

	e.renderData()
	return rowsFetched < i, query.Err()
}

// startRowsTimer starts or restarts the timer for auto-closing queries
func (e *Editor) startRowsTimer() {
	// Stop existing timer if any
	if e.rowsTimer != nil {
		e.rowsTimer.Stop()
	}
	if e.rowsTimerReset != nil {
		close(e.rowsTimerReset)
	}

	// Start new timer
	e.rowsTimerReset = make(chan struct{})
	e.rowsTimer = time.NewTimer(RowsTimerInterval)

	// Timer goroutine to handle resets
	go func() {
		resetChan := e.rowsTimerReset
		timer := e.rowsTimer
		if timer == nil {
			return
		}
		for {
			select {
			case _, ok := <-resetChan:
				if !ok {
					// Channel closed, exit goroutine
					return
				}
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(RowsTimerInterval)
			case <-timer.C:
				e.stopRowsTimer()
				return
			}
		}
	}()
}

// stopRowsTimer stops the timer and closes the rows if active
func (e *Editor) stopRowsTimer() {
	if e.rowsTimer != nil {
		e.rowsTimer.Stop()
		e.rowsTimer = nil
	}
	if e.rowsTimerReset != nil {
		close(e.rowsTimerReset)
		e.rowsTimerReset = nil
	}
	e.queryMu.Lock()
	if e.query != nil {
		if err := e.query.Close(); err != nil {
			panic(err)
		}
		e.query = nil
	}
	e.queryMu.Unlock()

	e.startRefreshTimer()
}

// startRefreshTimer starts a timer that refreshes data every 300ms
func (e *Editor) startRefreshTimer() {
	if e.app == nil {
		return
	}

	// Stop existing refresh timer if any
	e.stopRefreshTimer()

	// Start new refresh timer
	e.refreshTimerStop = make(chan struct{})
	e.refreshTimer = time.NewTimer(RefreshTimerInterval)

	// Refresh timer goroutine
	go func() {
		stopChan := e.refreshTimerStop
		timer := e.refreshTimer
		app := e.app
		if stopChan == nil || timer == nil {
			return
		}
		for {
			select {
			case _, ok := <-stopChan:
				if !ok {
					// Channel closed, exit goroutine
					return
				}
			case <-timer.C:
				if app != nil && e.relation != nil && e.relation.DB != nil {
					// Calculate data height before queueing the update
					terminalHeight := getTerminalHeight()
					dataHeight := terminalHeight - 3 // 3 lines for picker bar, status bar, command palette
					app.QueueUpdateDraw(func() {
						// Update table rowsHeight before loading rows from database
						e.table.UpdateRowsHeightFromRect(dataHeight)
						e.refresh()
					})
				}
				timer.Reset(RefreshTimerInterval)
			}
		}
	}()
}

// stopRefreshTimer stops the refresh timer if active
func (e *Editor) stopRefreshTimer() {
	if e.refreshTimer != nil {
		e.refreshTimer.Stop()
		e.refreshTimer = nil
	}
	if e.refreshTimerStop != nil {
		close(e.refreshTimerStop)
		e.refreshTimerStop = nil
	}
}
