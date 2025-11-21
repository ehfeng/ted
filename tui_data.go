package main

import (
	"database/sql"
	"fmt"
	"slices"
	"time"
)

// e.table.SetData based on e.records and e.pointer
func (e *Editor) renderData() {
	// When in insert mode, we need to reserve one slot for the insert mode row
	// that will be rendered by TableView
	dataCount := len(e.buffer)
	if len(e.table.insertRow) > 0 {
		// Find the last non-nil record
		lastIdx := len(e.buffer) - 1
		for lastIdx >= 0 && e.buffer[lastIdx].data == nil {
			lastIdx--
		}
		dataCount = lastIdx + 1 // Only pass real data, TableView will add insert mode row
	}

	normalizedRows := make([]Row, dataCount)
	for i := 0; i < dataCount; i++ {
		ptr := (i + e.pointer) % len(e.buffer)
		// insert mode row needs "space" at the top to still be able to render
		// the last db row
		if len(e.table.insertRow) > 0 && i == 0 {
			ptr++
		}
		normalizedRows[i] = e.buffer[ptr] // Reference to Row, not a copy
	}
	if len(e.table.insertRow) > 0 {
		normalizedRows = slices.Delete(normalizedRows, 0, 1)
	}
	e.table.SetDataReferences(normalizedRows)
}

// extractKeys returns a copy of the key values from a row
func (e *Editor) extractKeys(row []any) []any {
	if row == nil || len(row) < len(e.relation.key) {
		return nil
	}
	keys := make([]any, len(e.relation.key))
	copy(keys, row[0:len(e.relation.key)])
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

// id can be nil, in which case load from the top or bottom
// refreshing indicates whether to apply diff highlighting logic
func (e *Editor) loadFromRowId(id []any, fromTop bool, focusColumn int, refreshing bool) error {
	debugLog("loadFromRowId: starting, fromTop=%v, focusColumn=%d, id=%v\n", fromTop, focusColumn, id)
	if e.relation == nil || e.relation.DB == nil {
		return fmt.Errorf("no database connection available")
	}

	// Stop refresh timer when starting a new query
	e.stopRefreshTimer()

	selectCols := make([]string, len(e.columns))
	for i, col := range e.columns {
		selectCols[i] = col.Name
	}

	colCount := len(e.columns)
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
		e.nextQuery = rows
		e.startRowsTimer()

		// Scan rows into e.records starting from pointer
		e.pointer = 0
		rowsLoaded := 0
		scanTargets := make([]any, colCount)

		// Scan all new rows into temporary storage
		var newRows []Row
		var oldKeyMap map[string]int
		var oldKeyData map[string][]any
		var oldKeyStrings []string
		var matchedOldKeys map[string]bool

		if refreshing {
			// Build a map of old keys to check if new rows existed before
			// Also store old row data for diffing
			oldKeyMap = make(map[string]int)    // key string -> index
			oldKeyData = make(map[string][]any) // key string -> row data
			oldKeyStrings = make([]string, len(e.buffer))
			for i := 0; i < len(e.buffer); i++ {
				if e.buffer[i].data == nil {
					break
				}
				// Clear modified state from previous refresh
				e.buffer[i].modified = nil
				// Skip already deleted rows
				if e.buffer[i].state == RowStateDeleted {
					continue
				}
				oldKeys := e.extractKeys(e.buffer[i].data)
				if oldKeys != nil {
					keyStr := fmt.Sprintf("%v", oldKeys)
					oldKeyStrings[i] = keyStr
					oldKeyMap[keyStr] = i
					oldKeyData[keyStr] = e.buffer[i].data
				}
			}
			// Track which old keys are still present in new data
			matchedOldKeys = make(map[string]bool)
		}

		for rows.Next() {
			row := make([]any, colCount)
			for j := 0; j < colCount; j++ {
				scanTargets[j] = &row[j]
			}
			if err := rows.Scan(scanTargets...); err != nil {
				return err
			}

			newState := RowStateNormal
			var modified []int

			if refreshing {
				// Determine state by checking if key existed in old buffer
				newKeys := e.extractKeys(row)
				keyStr := fmt.Sprintf("%v", newKeys)

				if _, existedBefore := oldKeyMap[keyStr]; existedBefore {
					// This key existed before - mark it as matched
					matchedOldKeys[keyStr] = true
					// Diff old and new rows to find modified columns
					if oldData := oldKeyData[keyStr]; oldData != nil {
						modified = diffRows(oldData, row)
					}
				} else if len(oldKeyMap) > 0 {
					// This key didn't exist in old buffer (and there were old rows) - it's new
					newState = RowStateNew
				}
				// else: first load (no old rows), keep as Normal
			}

			newRows = append(newRows, Row{state: newState, data: row, modified: modified})
		}

		// Build final buffer: merge new rows with deleted old rows (or just use new rows if not refreshing)
		var finalRecords []Row
		if refreshing {
			finalRecords = make([]Row, 0, len(e.buffer))
			newRowIdx := 0

			for i := 0; i < len(e.buffer) && i < len(oldKeyStrings); i++ {
				if e.buffer[i].data == nil {
					break
				}

				oldKeyStr := oldKeyStrings[i]
				if oldKeyStr == "" || e.buffer[i].state == RowStateDeleted {
					// Skip empty or already deleted rows
					continue
				}

				if !matchedOldKeys[oldKeyStr] {
					// This old row doesn't exist in new data - mark as deleted
					e.buffer[i].state = RowStateDeleted
					finalRecords = append(finalRecords, e.buffer[i])
				} else if newRowIdx < len(newRows) {
					// Insert corresponding new row
					finalRecords = append(finalRecords, newRows[newRowIdx])
					newRowIdx++
				}
			}

			// Append any remaining new rows
			for newRowIdx < len(newRows) {
				finalRecords = append(finalRecords, newRows[newRowIdx])
				newRowIdx++
			}
		} else {
			// Not refreshing, just use new rows directly
			finalRecords = newRows
		}

		// Copy final records back to e.records
		for i := 0; i < len(finalRecords) && i < len(e.buffer); i++ {
			e.buffer[i] = finalRecords[i]
			rowsLoaded++
		}

		// If we have more final records than buffer size, truncate
		if len(finalRecords) > len(e.buffer) {
			rowsLoaded = len(e.buffer)
		}

		// Mark end with nil if we didn't fill the buffer
		if rowsLoaded < len(e.buffer) {
			e.buffer[rowsLoaded] = Row{}
			e.buffer = e.buffer[:rowsLoaded+1]
		}
	} else {
		// Load from bottom: use QueryRows with inclusive true, scrollDown false
		rows, err = e.relation.QueryRows(selectCols, nil, id, true, false)
		if err != nil {
			return err
		}
		e.prevQuery = rows
		e.startRowsTimer()

		// Scan rows into e.records in reverse, starting from end of buffer
		e.pointer = 0
		scanTargets := make([]any, colCount)
		rowsLoaded := 0

		// Scan all new rows into temporary storage
		var newRows []Row
		var oldKeyMap map[string]int
		var oldKeyData map[string][]any
		var oldKeyStrings []string
		var matchedOldKeys map[string]bool

		if refreshing {
			// Build a map of old keys to check if new rows existed before
			// Also store old row data for diffing
			oldKeyMap = make(map[string]int)    // key string -> index
			oldKeyData = make(map[string][]any) // key string -> row data
			oldKeyStrings = make([]string, len(e.buffer))
			for i := 0; i < len(e.buffer); i++ {
				if e.buffer[i].data == nil {
					break
				}
				// Clear modified state from previous refresh
				e.buffer[i].modified = nil
				// Skip already deleted rows
				if e.buffer[i].state == RowStateDeleted {
					continue
				}
				oldKeys := e.extractKeys(e.buffer[i].data)
				if oldKeys != nil {
					keyStr := fmt.Sprintf("%v", oldKeys)
					oldKeyStrings[i] = keyStr
					oldKeyMap[keyStr] = i
					oldKeyData[keyStr] = e.buffer[i].data
				}
			}
			// Track which old keys are still present in new data
			matchedOldKeys = make(map[string]bool)
		}

		for rows.Next() {
			row := make([]any, colCount)
			for j := 0; j < colCount; j++ {
				scanTargets[j] = &row[j]
			}
			if err := rows.Scan(scanTargets...); err != nil {
				return err
			}

			newState := RowStateNormal
			var modified []int

			if refreshing {
				// Determine state by checking if key existed in old buffer
				newKeys := e.extractKeys(row)
				keyStr := fmt.Sprintf("%v", newKeys)

				if _, existedBefore := oldKeyMap[keyStr]; existedBefore {
					// This key existed before - mark it as matched
					matchedOldKeys[keyStr] = true
					// Diff old and new rows to find modified columns
					if oldData := oldKeyData[keyStr]; oldData != nil {
						modified = diffRows(oldData, row)
					}
				} else if len(oldKeyMap) > 0 {
					// This key didn't exist in old buffer (and there were old rows) - it's new
					newState = RowStateNew
				}
				// else: first load (no old rows), keep as Normal
			}

			newRows = append(newRows, Row{state: newState, data: row, modified: modified})
			if len(newRows) >= len(e.buffer)-1 {
				break
			}
		}

		// Build final buffer: merge new rows with deleted old rows (or just use new rows if not refreshing)
		var finalRecords []Row
		if refreshing {
			finalRecords = make([]Row, 0, len(e.buffer))
			newRowIdx := 0

			for i := 0; i < len(e.buffer) && i < len(oldKeyStrings); i++ {
				if e.buffer[i].data == nil {
					break
				}

				oldKeyStr := oldKeyStrings[i]
				if oldKeyStr == "" || e.buffer[i].state == RowStateDeleted {
					// Skip empty or already deleted rows
					continue
				}

				if !matchedOldKeys[oldKeyStr] {
					// This old row doesn't exist in new data - mark as deleted
					e.buffer[i].state = RowStateDeleted
					finalRecords = append(finalRecords, e.buffer[i])
				} else if newRowIdx < len(newRows) {
					// Insert corresponding new row
					finalRecords = append(finalRecords, newRows[newRowIdx])
					newRowIdx++
				}
			}

			// Append any remaining new rows
			for newRowIdx < len(newRows) {
				finalRecords = append(finalRecords, newRows[newRowIdx])
				newRowIdx++
			}

			// Reverse the final records for fromBottom
			for i := 0; i < len(finalRecords)/2; i++ {
				j := len(finalRecords) - 1 - i
				finalRecords[i], finalRecords[j] = finalRecords[j], finalRecords[i]
			}
		} else {
			// Not refreshing, just use new rows directly and reverse them
			finalRecords = newRows
			for i := 0; i < len(finalRecords)/2; i++ {
				j := len(finalRecords) - 1 - i
				finalRecords[i], finalRecords[j] = finalRecords[j], finalRecords[i]
			}
		}

		// Copy final records back to e.records
		for i := 0; i < len(finalRecords) && i < len(e.buffer); i++ {
			e.buffer[i] = finalRecords[i]
			rowsLoaded++
		}

		// If we have more final records than buffer size, truncate
		if len(finalRecords) > len(e.buffer) {
			rowsLoaded = len(e.buffer)
		}

		e.buffer[len(e.buffer)-1] = Row{}

		// Mark end with nil if we didn't fill the buffer
		if rowsLoaded < len(e.buffer) {
			e.buffer[rowsLoaded] = Row{}
		}
		e.buffer[len(e.buffer)-1] = Row{}
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
	// Check if we're already at the end (last record is nil)
	if len(e.buffer) == 0 || e.buffer[e.lastRowIdx()].data == nil {
		return false, nil // No-op, already at end of data
	}

	if e.nextQuery == nil {
		// Stop refresh timer when starting a new query
		e.stopRefreshTimer()

		params := make([]any, len(e.relation.key))
		lastRecordIdx := (e.pointer - 1 + len(e.buffer)) % len(e.buffer)
		if e.buffer[lastRecordIdx].data == nil {
			return false, nil // Can't query from nil record
		}
		for i := range e.relation.key {
			params[i] = e.buffer[lastRecordIdx].data[i]
		}
		selectCols := make([]string, len(e.columns))
		for i, col := range e.columns {
			selectCols[i] = col.Name
		}
		var err error
		e.nextQuery, err = e.relation.QueryRows(selectCols, nil, params, false, true)
		if err != nil {
			return false, err
		}
		e.startRowsTimer()
	}

	// Signal timer reset
	if e.rowsTimerReset != nil {
		select {
		case e.rowsTimerReset <- struct{}{}:
		default:
		}
	}

	colCount := len(e.columns)
	if colCount == 0 {
		return false, nil
	}

	scanTargets := make([]any, colCount)

	rowsFetched := 0
	for ; rowsFetched < i && e.nextQuery.Next(); rowsFetched++ {
		row := make([]any, colCount)
		for j := 0; j < colCount; j++ {
			scanTargets[j] = &row[j]
		}
		if err := e.nextQuery.Scan(scanTargets...); err != nil {
			return false, err
		}
		e.buffer[(e.pointer+rowsFetched)%len(e.buffer)] = Row{state: RowStateNormal, data: row}
	}
	// new pointer position
	e.incrPtr(rowsFetched)
	// If we fetched fewer rows than requested, we've reached the end
	if rowsFetched < i {
		e.incrPtr(1)
		e.buffer[e.lastRowIdx()] = Row{} // Mark end of data
	}
	e.renderData()
	return rowsFetched < i, e.nextQuery.Err()
}

func (e *Editor) lastRowIdx() int {
	return (e.pointer + len(e.buffer) - 1) % len(e.buffer)
}

func (e *Editor) incrPtr(n int) {
	e.pointer = (e.pointer + n) % len(e.buffer)
}

// prevRows fetches the previous i rows (scrolling backwards in the circular buffer)
// returns whether rows were moved (false means you'ved the edge)
func (e *Editor) prevRows(i int) (bool, error) {
	if e.prevQuery == nil {
		// Stop refresh timer when starting a new query
		e.stopRefreshTimer()
		if len(e.buffer) == 0 || e.buffer[e.pointer].data == nil {
			return false, nil // Can't query from nil or empty records
		}
		params := make([]any, len(e.relation.key))
		for i := range e.relation.key {
			params[i] = e.buffer[e.pointer].data[i]
		}
		selectCols := make([]string, len(e.columns))
		for i, col := range e.columns {
			selectCols[i] = col.Name
		}
		var err error
		e.prevQuery, err = e.relation.QueryRows(selectCols, nil, params, false, false)
		if err != nil {
			return false, err
		}
		e.startRowsTimer()
	}

	// Signal timer reset
	if e.rowsTimerReset != nil {
		select {
		case e.rowsTimerReset <- struct{}{}:
		default:
		}
	}

	colCount := len(e.columns)
	if colCount == 0 {
		return false, nil
	}

	scanTargets := make([]any, colCount)

	rowsFetched := 0
	for ; rowsFetched < i && e.prevQuery.Next(); rowsFetched++ {
		row := make([]any, colCount)
		for j := 0; j < colCount; j++ {
			scanTargets[j] = &row[j]
		}
		if err := e.prevQuery.Scan(scanTargets...); err != nil {
			return false, err
		}
		e.pointer = e.lastRowIdx() // Move pointer backwards in the circular buffer
		e.buffer[e.pointer] = Row{state: RowStateNormal, data: row}
	}

	e.renderData()
	return rowsFetched < i, e.prevQuery.Err()
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
	if e.nextQuery != nil {
		if err := e.nextQuery.Close(); err != nil {
			panic(err)
		}
		e.nextQuery = nil
	}
	if e.prevQuery != nil {
		if err := e.prevQuery.Close(); err != nil {
			panic(err)
		}
		e.prevQuery = nil
	}

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
						id := e.buffer[e.pointer].data[:len(e.relation.key)]
						e.loadFromRowId(id, true, e.table.selectedCol, true)
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
	if e.prevQuery != nil {
		if err := e.prevQuery.Close(); err != nil {
			panic(err)
		}
		e.prevQuery = nil
	}
	if e.nextQuery != nil {
		if err := e.nextQuery.Close(); err != nil {
			panic(err)
		}
		e.nextQuery = nil
	}
}
