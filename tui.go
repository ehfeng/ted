package main

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const (
	DefaultColumnWidth = 8
)

// database: table, attribute, record
// sheet: sheet, column, row
// row id should be file line number, different than lookup key
type Relation struct {
	DB     *sql.DB
	DBType DatabaseType

	// name metadata
	name           string
	lookupKey      []string
	attributes     map[string]attribute
	attributeOrder []string
	attributeIndex map[string]int
	// attributes indices
	uniques    [][]int
	references [][]int
}

// this is a config concept
// width = 0 means column is not selected
type column struct {
	Name  string
	Width int
}

type attribute struct {
	Name      string
	Type      string
	Nullable  bool
	Generated bool // TODO if computed column, read-only
}

// columns are display, relation.attributes stores the database attributes
type Editor struct {
	app      *tview.Application
	pages    *tview.Pages
	table    *HeaderTable
	columns  []column
	relation *Relation
	config   *Config

	statusBar               *tview.TextView
	commandPalette          *tview.InputField
	layout                  *tview.Flex
	paletteMode             PaletteMode
	preEditMode             PaletteMode
	placeholderStyleDefault tcell.Style
	placeholderStyleItalic  tcell.Style
	kittySequenceActive     bool
	kittySequenceBuffer     string

	// selection
	currentRow       int
	currentCol       int
	editMode         bool
	originalEditText string
	selectedRows     map[int]bool
	selectedCols     map[int]bool

	// data
	rows        *sql.Rows
	pointer     int             // pointer to the current record
	records     [][]interface{} // columns are keyed off e.columns
	whereClause string
	orderBy     string

	// timer for auto-closing rows
	rowsTimer      *time.Timer
	rowsTimerReset chan struct{}
}

// PaletteMode represents the current mode of the command palette
type PaletteMode int

const (
	PaletteModeDefault PaletteMode = iota
	PaletteModeCommand
	PaletteModeSQL
	PaletteModeFind
	PaletteModeUpdate
)

func (m PaletteMode) Glyph() string {
	switch m {
	case PaletteModeDefault:
		return "# "
	case PaletteModeCommand:
		return "> "
	case PaletteModeSQL:
		return "` "
	case PaletteModeFind:
		return "/ "
	case PaletteModeUpdate:
		return "= "
	default:
		return "> "
	}
}

func NewRelation(db *sql.DB, dbType DatabaseType, tableName string,
	configCols []column) (*Relation, error) {

	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

	relation := &Relation{
		DB:             db,
		DBType:         dbType,
		name:           tableName,
		attributes:     make(map[string]attribute),
		attributeIndex: make(map[string]int),
	}

	if err := loadTableSchema(db, dbType, relation); err != nil {
		return nil, fmt.Errorf("failed to load table schema: %w", err)
	}

	selected := []string{}
	if configCols == nil {
		selected = append(selected, relation.attributeOrder...)
	} else {
		// Create map of selected column names for efficient lookup
		selectedMap := make(map[string]bool)
		for _, col := range configCols {
			selectedMap[col.Name] = true
		}

		// Add intersection of selectedColumns and lookupKey first
		for _, key := range relation.lookupKey {
			if selectedMap[key] {
				selected = append(selected, key)
			}
		}

		// Add remaining selected columns that aren't in lookupKey
		for _, col := range configCols {
			isLookupKey := false
			for _, key := range relation.lookupKey {
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

	return relation, nil
}

func loadTableSchema(db *sql.DB, dbType DatabaseType, relation *Relation) error {
	var query string

	switch dbType {
	case SQLite:
		query = fmt.Sprintf("PRAGMA table_info(%s)", relation.name)
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

	rows, err := db.Query(query, relation.name)
	if err != nil {
		return err
	}
	defer rows.Close()

	relation.attributes = make(map[string]attribute)
	relation.attributeOrder = relation.attributeOrder[:0]
	relation.attributeIndex = make(map[string]int)

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

		idx := len(relation.attributeOrder)
		relation.attributeOrder = append(relation.attributeOrder, attr.Name)
		relation.attributes[attr.Name] = attr
		relation.attributeIndex[attr.Name] = idx
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// Consolidated lookup key selection: choose shortest lookup key
	lookupCols, err := getShortestLookupKey(db, dbType, relation.name)
	if err != nil {
		return err
	}
	primaryKeyColumns = lookupCols

	// TODO if lookup key not found, use databaseFeature.systemId if available

	// If not nullable unique constraint is found, error
	if len(primaryKeyColumns) == 0 {
		return fmt.Errorf("no primary key found")
	}
	relation.lookupKey = make([]string, len(primaryKeyColumns))
	copy(relation.lookupKey, primaryKeyColumns)

	// Build a quick name->index map for attribute lookup
	attrIndex := relation.attributeIndex

	// Populate foreign key column indices (supports multicolumn FKs)
	switch dbType {
	case SQLite:
		// PRAGMA foreign_key_list returns one row per referencing column
		// cols: id, seq, table, from, to, on_update, on_delete, match
		fkRows, err := db.Query(fmt.Sprintf("PRAGMA foreign_key_list(%s)", relation.name))
		if err == nil {
			type fkCol struct {
				seq int
				col string
			}
			byID := map[int][]fkCol{}
			for fkRows.Next() {
				var id, seq int
				var refTable, fromCol, toCol, onUpd, onDel, match string
				if scanErr := fkRows.Scan(&id, &seq, &refTable, &fromCol, &toCol, &onUpd, &onDel, &match); scanErr != nil {
					continue
				}
				byID[id] = append(byID[id], fkCol{seq: seq, col: fromCol})
			}
			fkRows.Close()
			// Assemble ordered index slices
			for _, cols := range byID {
				sort.Slice(cols, func(i, j int) bool { return cols[i].seq < cols[j].seq })
				idxs := make([]int, 0, len(cols))
				valid := true
				for _, c := range cols {
					if idx, ok := attrIndex[c.col]; ok {
						idxs = append(idxs, idx)
					} else {
						valid = false
						break
					}
				}
				if valid && len(idxs) > 0 {
					relation.references = append(relation.references, idxs)
				}
			}
		}

	case PostgreSQL:
		// Use pg_catalog to get correct column order within FK
		schema := "public"
		rel := relation.name
		if dot := strings.IndexByte(rel, '.'); dot != -1 {
			schema = rel[:dot]
			rel = rel[dot+1:]
		}
		fkQuery := `
            SELECT con.oid::text AS id, att.attname AS col, u.ord AS ord
            FROM pg_constraint con
            JOIN unnest(con.conkey) WITH ORDINALITY AS u(attnum, ord) ON true
            JOIN pg_class rel ON rel.oid = con.conrelid
            JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
            JOIN pg_attribute att ON att.attrelid = rel.oid AND att.attnum = u.attnum
            WHERE con.contype = 'f' AND rel.relname = $1 AND nsp.nspname = $2
            ORDER BY con.oid, u.ord`
		fkRows, err := db.Query(fkQuery, rel, schema)
		if err == nil {
			type fkCol struct {
				ord int
				col string
			}
			byID := map[string][]fkCol{}
			for fkRows.Next() {
				var id, col string
				var ord int
				if scanErr := fkRows.Scan(&id, &col, &ord); scanErr != nil {
					continue
				}
				byID[id] = append(byID[id], fkCol{ord: ord, col: col})
			}
			fkRows.Close()
			for _, cols := range byID {
				sort.Slice(cols, func(i, j int) bool { return cols[i].ord < cols[j].ord })
				idxs := make([]int, 0, len(cols))
				valid := true
				for _, c := range cols {
					if idx, ok := attrIndex[c.col]; ok {
						idxs = append(idxs, idx)
					} else {
						valid = false
						break
					}
				}
				if valid && len(idxs) > 0 {
					relation.references = append(relation.references, idxs)
				}
			}
		}

	case MySQL:
		// Use information_schema to identify FK columns with ordering
		fkQuery := `
            SELECT CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL
            ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION`
		fkRows, err := db.Query(fkQuery, relation.name)
		if err == nil {
			type fkCol struct {
				ord int
				col string
			}
			byName := map[string][]fkCol{}
			for fkRows.Next() {
				var cname, col string
				var ord int
				if scanErr := fkRows.Scan(&cname, &col, &ord); scanErr != nil {
					continue
				}
				byName[cname] = append(byName[cname], fkCol{ord: ord, col: col})
			}
			fkRows.Close()
			for _, cols := range byName {
				sort.Slice(cols, func(i, j int) bool { return cols[i].ord < cols[j].ord })
				idxs := make([]int, 0, len(cols))
				valid := true
				for _, c := range cols {
					if idx, ok := attrIndex[c.col]; ok {
						idxs = append(idxs, idx)
					} else {
						valid = false
						break
					}
				}
				if valid && len(idxs) > 0 {
					relation.references = append(relation.references, idxs)
				}
			}
		}
	}
	return nil
}

// BuildUpdatePreview constructs a SQL UPDATE statement as a string with literal
// values inlined for preview purposes. It mirrors UpdateDBValue but does not
// execute any SQL. Intended only for UI preview.
func (sheet *Relation) BuildUpdatePreview(records [][]interface{}, rowIdx int, colName string, newValue string) string {
	if rowIdx < 0 || rowIdx >= len(records) || len(sheet.lookupKey) == 0 {
		return ""
	}

	// Convert raw text to DB-typed value (mirrors UpdateDBValue's toDBValue)
	toDBValue := func(colName, raw string) interface{} {
		attrType := ""
		if attr, ok := sheet.attributes[colName]; ok {
			attrType = strings.ToLower(attr.Type)
		}
		if raw == "\\N" {
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

	// Render a literal value for preview (no placeholders)
	literal := func(val interface{}, attrType string) string {
		if val == nil {
			return "NULL"
		}
		at := strings.ToLower(attrType)
		switch v := val.(type) {
		case bool:
			if sheet.DBType == PostgreSQL {
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

	// Where clause with literal values
	whereParts := make([]string, 0, len(sheet.lookupKey))
	for _, lookupKeyCol := range sheet.lookupKey {
		qKeyName := quoteIdent(sheet.DBType, lookupKeyCol)
		idx, ok := sheet.attributeIndex[lookupKeyCol]
		if !ok || rowIdx >= len(records) {
			return ""
		}
		row := records[rowIdx]
		if idx < 0 || idx >= len(row) {
			return ""
		}
		attr, ok := sheet.attributes[lookupKeyCol]
		if !ok {
			return ""
		}
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", qKeyName, literal(row[idx], attr.Type)))
	}

	// SET clause literal
	targetAttrType := ""
	if attr, ok := sheet.attributes[colName]; ok {
		targetAttrType = attr.Type
	}
	valueArg := toDBValue(colName, newValue)
	quotedTarget := quoteIdent(sheet.DBType, colName)
	setClause := fmt.Sprintf("%s = %s", quotedTarget, literal(valueArg, targetAttrType))

	returningCols := make([]string, len(sheet.attributeOrder))
	for i, name := range sheet.attributeOrder {
		returningCols[i] = quoteIdent(sheet.DBType, name)
	}
	if len(sheet.lookupKey) == 1 {
		if sheet.lookupKey[0] == "rowid" {
			returningCols = append(returningCols, "rowid")
		}
		if sheet.lookupKey[0] == "ctid" {
			returningCols = append(returningCols, "ctid")
		}
	}
	returning := strings.Join(returningCols, ", ")

	quotedTable := quoteQualified(sheet.DBType, sheet.name)
	useReturning := databaseFeatures[sheet.DBType].returning
	if useReturning {
		return fmt.Sprintf("UPDATE %s SET %s WHERE %s RETURNING %s", quotedTable, setClause, strings.Join(whereParts, " AND "), returning)
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s", quotedTable, setClause, strings.Join(whereParts, " AND "))
}

// UpdateDBValue updates a single cell in the underlying database using the
// sheet's lookupKey columns to identify the row. It returns the refreshed row
// values ordered by sheet.selectCols. If no row is updated, returns an error.
func (sheet *Relation) UpdateDBValue(records [][]interface{}, rowIdx int, colName string, newValue string) ([]interface{}, error) {
	if rowIdx < 0 || rowIdx >= len(records) {
		return nil, fmt.Errorf("index out of range")
	}
	if len(sheet.lookupKey) == 0 {
		return nil, fmt.Errorf("no lookup key configured")
	}

	// Convert string to appropriate DB value
	toDBValue := func(colName, raw string) interface{} {
		attrType := ""
		if attr, ok := sheet.attributes[colName]; ok {
			attrType = strings.ToLower(attr.Type)
		}
		if raw == "\\N" {
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
		switch sheet.DBType {
		case PostgreSQL:
			return fmt.Sprintf("$%d", i)
		default:
			return "?"
		}
	}

	// Build SET and WHERE clauses and args
	valueArg := toDBValue(colName, newValue)
	keyArgs := make([]interface{}, 0, len(sheet.lookupKey))
	whereParts := make([]string, 0, len(sheet.lookupKey))
	for i := range sheet.lookupKey {
		lookupKeyCol := sheet.lookupKey[i]
		qKeyName := quoteIdent(sheet.DBType, lookupKeyCol)
		var ph string
		if sheet.DBType == PostgreSQL {
			ph = placeholder(2 + i)
		} else {
			ph = placeholder(0)
		}
		whereParts = append(whereParts, fmt.Sprintf("%s = %s", qKeyName, ph))
		colIdx, ok := sheet.attributeIndex[lookupKeyCol]
		if !ok || rowIdx >= len(records) {
			return nil, fmt.Errorf("lookup key column %s not found in records", lookupKeyCol)
		}
		row := records[rowIdx]
		if colIdx < 0 || colIdx >= len(row) {
			return nil, fmt.Errorf("lookup key column %s not loaded", lookupKeyCol)
		}
		keyArgs = append(keyArgs, row[colIdx])
	}

	// SET clause placeholder
	var setClause string
	quotedTarget := quoteIdent(sheet.DBType, colName)
	if sheet.DBType == PostgreSQL {
		setClause = fmt.Sprintf("%s = %s", quotedTarget, placeholder(1))
	} else {
		setClause = fmt.Sprintf("%s = %s", quotedTarget, placeholder(0))
	}

	returningCols := make([]string, len(sheet.attributeOrder))
	for i, name := range sheet.attributeOrder {
		returningCols[i] = quoteIdent(sheet.DBType, name)
	}

	if len(sheet.lookupKey) == 1 {
		if sheet.lookupKey[0] == "rowid" {
			returningCols = append(returningCols, "rowid")
		}
		if sheet.lookupKey[0] == "ctid" {
			returningCols = append(returningCols, "ctid")
		}
	}
	returning := strings.Join(returningCols, ", ")

	// Build full query
	var query string
	useReturning := databaseFeatures[sheet.DBType].returning
	quotedTable := quoteQualified(sheet.DBType, sheet.name)
	if useReturning {
		query = fmt.Sprintf("UPDATE %s SET %s WHERE %s RETURNING %s", quotedTable, setClause, strings.Join(whereParts, " AND "), returning)
		// Combine args: value + keys
		args := make([]interface{}, 0, 1+len(keyArgs))
		args = append(args, valueArg)
		args = append(args, keyArgs...)

		// Scan into pointers to capture returned values
		rowVals := make([]interface{}, len(returningCols))
		scanArgs := make([]interface{}, len(returningCols))
		for i := range rowVals {
			scanArgs[i] = &rowVals[i]
		}

		if err := sheet.DB.QueryRow(query, args...).Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("update failed: %w", err)
		}
		return rowVals, nil
	} else {
		// For database servers that don't support RETURNING, use a transaction
		// to perform the UPDATE followed by a SELECT of the updated row.
		query = fmt.Sprintf("UPDATE %s SET %s WHERE %s", quotedTable, setClause, strings.Join(whereParts, " AND "))

		// Begin transaction
		tx, err := sheet.DB.Begin()
		if err != nil {
			return nil, fmt.Errorf("begin tx failed: %w", err)
		}

		// Execute update inside the transaction
		args := make([]interface{}, 0, 1+len(keyArgs))
		args = append(args, valueArg)
		args = append(args, keyArgs...)
		res, err := tx.Exec(query, args...)
		if err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("update failed: %w", err)
		}
		if ra, _ := res.RowsAffected(); ra == 0 {
			_ = tx.Rollback()
			return nil, fmt.Errorf("no rows updated")
		}

		// Re-select the updated row within the same transaction
		selQuery := fmt.Sprintf("SELECT %s FROM %s WHERE %s", returning, quotedTable, strings.Join(whereParts, " AND "))
		row := tx.QueryRow(selQuery, keyArgs...)
		rowVals := make([]interface{}, len(returningCols))
		scanArgs := make([]interface{}, len(rowVals))
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
}

func runEditor(config *Config, dbname, tablename string) error {
	tview.Styles.ContrastBackgroundColor = tcell.ColorBlack

	db, dbType, err := config.connect()
	if err != nil {
		return err
	}
	defer db.Close()

	// Get terminal height for optimal data loading
	terminalHeight := getTerminalHeight()
	tableDataHeight := terminalHeight - 5 // 5 lines for header, status bar, command palette

	var configColumns []column // TODO derive from config
	relation, err := NewRelation(db, dbType, tablename, configColumns)
	if err != nil {
		return err
	}

	columns := make([]column, len(relation.attributeOrder))
	for i, name := range relation.attributeOrder {
		columns[i] = column{
			Name:  name,
			Width: DefaultColumnWidth,
		}
	}

	editor := &Editor{
		app:          tview.NewApplication().SetTitle(fmt.Sprintf("ted %s/%s %s", dbname, tablename, databaseIcons[dbType])),
		pages:        tview.NewPages(),
		table:        NewHeaderTable(),
		columns:      columns,
		relation:     relation,
		config:       config,
		selectedRows: make(map[int]bool),
		selectedCols: make(map[int]bool),
		paletteMode:  PaletteModeDefault,
		preEditMode:  PaletteModeDefault,
		pointer:      0,
		records:      make([][]interface{}, tableDataHeight),
		whereClause:  config.Where,
		orderBy:      config.OrderBy,
	}

	editor.setupTable()
	editor.setupKeyBindings()
	editor.setupStatusBar()
	editor.setupCommandPalette()
	editor.setupLayout()
	editor.refreshData()
	editor.pages.AddPage("table", editor.layout, true, true)

	if err := editor.app.SetRoot(editor.pages, true).EnableMouse(true).Run(); err != nil {
		return err
	}
	return nil
}

func (e *Editor) setupTable() {
	// Create headers for HeaderTable
	headers := make([]HeaderColumn, len(e.columns))
	for i, col := range e.columns {
		headers[i] = HeaderColumn{
			Name:  col.Name,
			Width: col.Width,
		}
	}

	// Configure the table
	e.table.SetHeaders(headers).SetData(e.records).SetSelectable(true)
}

func (e *Editor) setupStatusBar() {
	e.statusBar = tview.NewTextView().
		SetDynamicColors(true).
		SetRegions(true).
		SetWrap(false)

	e.statusBar.SetBackgroundColor(tcell.ColorLightGray)
	e.statusBar.SetTextColor(tcell.ColorBlack)
	e.statusBar.SetText("Ready")
}

func (e *Editor) setupCommandPalette() {
	inputField := tview.NewInputField()
	e.commandPalette = inputField.
		SetLabel("").
		SetFieldBackgroundColor(tcell.ColorBlack).
		SetFieldTextColor(tcell.ColorWhite)

	e.commandPalette.SetBackgroundColor(tcell.ColorBlack)
	e.placeholderStyleDefault = e.commandPalette.GetPlaceholderStyle()
	e.placeholderStyleItalic = e.placeholderStyleDefault.Italic(true)

	// Default palette mode shows keybinding help
	e.setPaletteMode(PaletteModeDefault, false)

	e.commandPalette.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		key := event.Key()
		rune := event.Rune()
		mod := event.Modifiers()

		if e.consumeKittyCSI(key, rune, mod) {
			return nil
		}
		if !e.kittySequenceActive {
			if key == tcell.KeyRune && mod&tcell.ModCtrl != 0 && rune == '`' {
				e.kittySequenceBuffer = "ctrl+`"
			} else {
				e.kittySequenceBuffer = ""
			}
		}
		if !e.kittySequenceActive && e.kittySequenceBuffer == "ctrl+`" {
			e.kittySequenceBuffer = ""
			e.setPaletteMode(PaletteModeSQL, true)
			return nil
		}

		switch {
		case (rune == 'f' || rune == 6) && mod&tcell.ModCtrl != 0:
			e.setPaletteMode(PaletteModeFind, true)
			return nil
		case (rune == 'p' || rune == 16) && mod&tcell.ModCtrl != 0:
			e.setPaletteMode(PaletteModeCommand, true)
			return nil
		case (rune == '`' || rune == 0) && mod&tcell.ModCtrl != 0:
			e.setPaletteMode(PaletteModeSQL, true)
			return nil
		}

		switch event.Key() {
		case tcell.KeyEnter:
			command := e.commandPalette.GetText()
			switch e.getPaletteMode() {
			case PaletteModeCommand:
				e.executeCommand(command)
			case PaletteModeSQL:
				// Placeholder for future SQL execution
				if strings.TrimSpace(command) != "" {
					e.SetStatusLog("SQL: " + strings.TrimSpace(command))
				}
			case PaletteModeFind:
				// Placeholder for future find implementation
				if strings.TrimSpace(command) != "" {
					e.SetStatusLog("Find: " + strings.TrimSpace(command))
				}
			}
			e.setPaletteMode(PaletteModeDefault, false)
			e.app.SetFocus(e.table)
			return nil
		case tcell.KeyEscape:
			e.setPaletteMode(PaletteModeDefault, false)
			e.app.SetFocus(e.table)
			return nil
		}
		return event
	})
}

func (e *Editor) setupLayout() {
	e.layout = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(e.table, 0, 1, true).
		AddItem(e.statusBar, 1, 0, false).
		AddItem(e.commandPalette, 1, 0, false)
}

func (e *Editor) setupKeyBindings() {
	e.table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		key := event.Key()
		rune := event.Rune()
		mod := event.Modifiers()
		row, col := e.table.GetSelection()

		if e.consumeKittyCSI(key, rune, mod) {
			return nil
		}
		if !e.kittySequenceActive {
			if key == tcell.KeyRune && mod&tcell.ModCtrl != 0 && rune == '`' {
				e.kittySequenceBuffer = "ctrl+`"
			} else {
				e.kittySequenceBuffer = ""
			}
		}

		if !e.kittySequenceActive && e.kittySequenceBuffer == "ctrl+`" {
			e.kittySequenceBuffer = ""
			e.setPaletteMode(PaletteModeSQL, true)
			return nil
		}

		switch {
		case key == tcell.KeyEnter:
			e.enterEditMode(row, col)
			return nil
		case key == tcell.KeyEscape:
			if e.app.GetFocus() == e.commandPalette {
				e.setPaletteMode(PaletteModeDefault, false)
				e.app.SetFocus(e.table)
				return nil
			}
			e.exitEditMode()
			return nil
		case key == tcell.KeyTab:
			e.navigateTab(false)
			return nil
		case key == tcell.KeyBacktab:
			e.navigateTab(true)
			return nil
		case key == tcell.KeyHome:
			e.table.Select(row, 0)
			return nil
		case key == tcell.KeyEnd:
			e.table.Select(row, len(e.columns)-1)
			return nil
		case key == tcell.KeyPgUp:
			newRow := max(0, row-10)
			e.table.Select(newRow, col)
			return nil
		case key == tcell.KeyPgDn:
			newRow := min(len(e.records)-1, row+10)
			e.table.Select(newRow, col)
			return nil
		case rune == ' ' && mod&tcell.ModShift != 0:
			e.toggleRowSelection(row)
			return nil
		case rune == ' ' && mod&tcell.ModCtrl != 0:
			e.toggleColSelection(col)
			return nil
		case (rune == 'r' || rune == 18) && mod&tcell.ModCtrl != 0: // Ctrl+R sends DC2 (18) or 'r' depending on terminal
			e.refreshData()
			return nil
		case (rune == 'f' || rune == 6) && mod&tcell.ModCtrl != 0: // Ctrl+F sends ACK (6) or 'f' depending on terminal
			e.setPaletteMode(PaletteModeFind, true)
			return nil
		case (rune == 'p' || rune == 16) && mod&tcell.ModCtrl != 0: // Ctrl+P sends DLE (16) or 'p' depending on terminal
			e.setPaletteMode(PaletteModeCommand, true)
			return nil
		case (rune == '`' || rune == 0) && mod&tcell.ModCtrl != 0: // Ctrl+` sends BEL (0) or '`' depending on terminal
			e.setPaletteMode(PaletteModeSQL, true)
			return nil
		case key == tcell.KeyLeft && mod&tcell.ModAlt != 0:
			e.moveColumn(col, -1)
			return nil
		case key == tcell.KeyRight && mod&tcell.ModAlt != 0:
			e.moveColumn(col, 1)
			return nil
		case key == tcell.KeyLeft && mod&tcell.ModCtrl != 0:
			e.adjustColumnWidth(col, -2)
			return nil
		case key == tcell.KeyRight && mod&tcell.ModCtrl != 0:
			e.adjustColumnWidth(col, 2)
			return nil
		case key == tcell.KeyLeft && mod&tcell.ModMeta != 0:
			e.table.Select(row, 0)
			return nil
		case key == tcell.KeyRight && mod&tcell.ModMeta != 0:
			e.table.Select(row, len(e.columns)-1)
			return nil
		case key == tcell.KeyUp && mod&tcell.ModMeta != 0:
			e.table.Select(0, col)
			return nil
		case key == tcell.KeyDown && mod&tcell.ModMeta != 0:
			e.table.Select(len(e.records)-1, col)
			return nil
		default:
			if key == tcell.KeyRune && rune != 0 &&
				mod&(tcell.ModAlt|tcell.ModCtrl|tcell.ModMeta) == 0 {
				e.enterEditModeWithInitialValue(row, col, string(rune))
				return nil
			}
		}

		return event
	})
}

func (e *Editor) consumeKittyCSI(key tcell.Key, r rune, mod tcell.ModMask) bool {
	if e.kittySequenceActive {
		if key != tcell.KeyRune {
			e.kittySequenceActive = false
			e.kittySequenceBuffer = ""
			return false
		}

		if r == 'u' {
			seq := e.kittySequenceBuffer
			e.kittySequenceActive = false
			e.kittySequenceBuffer = ""
			parts := strings.SplitN(seq, ";", 2)
			if len(parts) == 2 {
				codepoint, err1 := strconv.Atoi(parts[0])
				modifier, err2 := strconv.Atoi(parts[1])
				if err1 == nil && err2 == nil {
					mask := modifier - 1
					if mask&4 != 0 && codepoint == 96 {
						e.setPaletteMode(PaletteModeSQL, true)
					}
				}
			}
			return true
		}

		e.kittySequenceBuffer += string(r)
		return true
	}

	if key == tcell.KeyRune && r == '[' {
		e.kittySequenceActive = true
		e.kittySequenceBuffer = ""
		return true
	}

	return false
}

func (e *Editor) navigateTab(reverse bool) {
	row, col := e.table.GetSelection()

	if reverse {
		if col > 0 {
			e.table.Select(row, col-1)
		} else if row > 0 {
			e.table.Select(row-1, len(e.columns)-1)
		}
	} else {
		if col < len(e.columns)-1 {
			e.table.Select(row, col+1)
		} else if row < len(e.records)-1 {
			e.table.Select(row+1, 0)
		}
	}
}

func (e *Editor) enterEditMode(row, col int) {
	currentValue := e.table.GetCell(row, col)
	currentText := formatCellValue(currentValue)
	e.enterEditModeWithInitialValue(row, col, currentText)
}

func (e *Editor) enterEditModeWithInitialValue(row, col int, initialText string) {
	if row < 0 || row >= len(e.records) {
		return
	}
	if col < 0 || col >= len(e.columns) || col >= len(e.records[row]) {
		return
	}

	// Remember the palette mode so we can restore it after editing
	e.preEditMode = e.getPaletteMode()
	originalValue := e.table.GetCell(row, col)
	e.originalEditText = formatCellValue(originalValue)

	// Create textarea for editing with proper styling
	textArea := tview.NewTextArea().
		SetText(initialText, true).
		SetWrap(true).
		SetOffset(0, 0)

	textArea.SetBorder(false)

	// Store references for dynamic resizing
	var modal tview.Primitive
	e.currentRow = row
	e.currentCol = col

	// Function to resize textarea based on current content
	resizeTextarea := func() {
		e.pages.RemovePage("editor")
		modal = e.createCellEditOverlay(textArea, row, col, textArea.GetText())
		e.pages.AddPage("editor", modal, true, true)
		textArea.SetOffset(0, 0)
	}

	// Handle textarea input capture for save/cancel
	textArea.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEnter:
			// Check if Alt/Option is pressed with Enter
			if event.Modifiers()&tcell.ModAlt != 0 {
				// Alt+Enter: only allow newlines for text-compatible column types
				if e.isMultilineColumnType(col) {
					// Manually insert newline instead of relying on default handler
					currentText := textArea.GetText()
					// Simply append newline at the end for now
					newText := currentText + "\n"
					textArea.SetText(newText, true)
					resizeTextarea()
					return nil
					// return event
				}
				// For other types, treat as regular Enter (save and exit)
				newText := textArea.GetText()
				e.updateCell(row, col, newText)
				return nil
			}
			// Plain Enter: save and exit
			newText := textArea.GetText()
			e.updateCell(row, col, newText)
			return nil
		case tcell.KeyEscape:
			e.exitEditMode()
			return nil
		}
		return event
	})

	// Set up dynamic resizing on text changes and update SQL preview
	textArea.SetChangedFunc(func() {
		resizeTextarea()
		e.updateEditPreview(textArea.GetText())
	})

	// Position the textarea to align with the cell
	modal = e.createCellEditOverlay(textArea, row, col, initialText)
	e.pages.AddPage("editor", modal, true, true)

	// Set up native cursor positioning using terminal escapes
	e.app.SetAfterDrawFunc(func(screen tcell.Screen) {
		if !e.editMode {
			return
		}

		// Hide the default tcell cursor first
		screen.HideCursor()
		screen.SetCursorStyle(tcell.CursorStyleBlinkingBar)

		// Get cursor position from textarea
		_, _, toRow, toCol := textArea.GetCursor()
		offsetRow, offsetColumn := textArea.GetOffset()

		// Calculate screen position based on original cell position
		// Use the same calculation as createCellEditOverlay
		tableRow := row + 3 // Convert data row to table display row
		leftOffset := 1     // Left table border "│"
		for i := 0; i < col; i++ {
			leftOffset += e.table.GetColumnWidth(i) + 2 + 1 // width + " │ " padding + separator
		}
		leftOffset += 1 // Cell padding (space after "│ ")

		// Calculate cursor position relative to the cell content area
		cursorX := leftOffset + toCol - offsetColumn
		cursorY := tableRow + toRow - offsetRow

		screen.ShowCursor(cursorX, cursorY)
	})

	// Set cursor to bar style (style 5 = blinking bar)
	e.setCursorStyle(5)

	e.app.SetFocus(textArea)
	e.editMode = true

	e.setPaletteMode(PaletteModeUpdate, false)
}

func (e *Editor) createCellEditOverlay(textArea *tview.TextArea, row, col int, currentText string) tview.Primitive {
	// Get the current text to calculate minimum size
	currentText = formatCellValue(currentText)

	// Calculate the position where the cell content appears on screen
	// HeaderTable structure: top border (row 0) + header (row 1) + separator (row 2) + data rows (3+)
	tableRow := row + 3 // Convert data row to table display row

	// Calculate horizontal position: left border + previous columns + cell padding
	leftOffset := 1 // Left table border "│"
	for i := 0; i < col; i++ {
		leftOffset += e.table.GetColumnWidth(i) + 2 + 1 // width + " │ " padding + separator
	}
	leftOffset += 1 // Cell padding (space after "│ ")
	leftOffset -= 1 // Move overlay one position to the left

	// Calculate vertical position relative to table
	topOffset := tableRow

	// Calculate minimum textarea size based on column width
	cellWidth := e.table.GetColumnWidth(col)

	// Calculate total table width for maximum textarea width
	totalTableWidth := 0
	for i := 0; i < len(e.columns); i++ {
		totalTableWidth += e.table.GetColumnWidth(i)
	}
	// Add space for borders and separators: left border + (n-1 separators * 3) + right border
	totalTableWidth += 1 + (len(e.columns)-1)*3 + 1

	// Calculate the maximum available width for the textarea
	maxAvailableWidth := totalTableWidth - leftOffset + 1 // Account for right border

	// First, try with cell width to see if content fits
	textLines := strings.Split(currentText, "\n")
	longestLine := 0
	for _, line := range textLines {
		if len(line) > longestLine {
			longestLine = len(line)
		}
	}

	// Calculate desired width based on content
	desiredWidth := max(cellWidth, longestLine) + 2
	textAreaWidth := min(desiredWidth, maxAvailableWidth)
	// If we're using the capped width, recalculate text lines with the actual textarea width
	// if textAreaWidth < desiredWidth {
	// 	textLines = splitTextToLines(currentText, textAreaWidth-2) // Account for padding
	// }

	// Minimum height: number of lines needed, minimum 1
	textAreaHeight := max(len(textLines), 1)

	// Create positioned overlay that aligns text with the original cell
	leftPadding := tview.NewBox()

	return tview.NewFlex().
		AddItem(nil, leftOffset, 0, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, topOffset, 0, false).
			AddItem(tview.NewFlex().
				AddItem(leftPadding, 1, 0, false).           // Left padding
				AddItem(textArea, textAreaWidth-1, 0, true), // Text area
				textAreaHeight, 0, true).
			AddItem(nil, 0, 1, false), textAreaWidth, 0, true).
		AddItem(nil, 0, 1, false)
}

func (e *Editor) setCursorStyle(style int) {
	fmt.Printf("\033[%d q", style)
}

func (e *Editor) exitEditMode() {
	if e.editMode {
		e.pages.RemovePage("editor")
		e.app.SetAfterDrawFunc(nil) // Clear the cursor function
		e.setCursorStyle(0)         // Reset to default cursor style
		e.app.SetFocus(e.table)
		e.editMode = false
		// Return palette to default mode after editing
		e.setPaletteMode(PaletteModeDefault, false)
		e.originalEditText = ""
	}
}

// Mode management helpers
func (e *Editor) getPaletteMode() PaletteMode {
	return e.paletteMode
}

func (e *Editor) setPaletteMode(mode PaletteMode, focus bool) {
	e.paletteMode = mode
	e.commandPalette.SetLabel(mode.Glyph())
	// Clear input when switching modes
	e.commandPalette.SetText("")
	style := e.placeholderStyleItalic
	e.commandPalette.SetPlaceholderStyle(style)
	if e.editMode {
		// Editing contexts manage their own placeholder text
		e.commandPalette.SetPlaceholder("UPDATE preview... (Esc to exit)")
		if focus {
			e.app.SetFocus(e.commandPalette)
		}
		return
	} else {
		switch mode {
		case PaletteModeDefault:
			e.commandPalette.SetPlaceholder("Ctrl+P: Command   Ctrl+`: SQL   Ctrl+F: Find")
		case PaletteModeCommand:
			e.commandPalette.SetPlaceholder("Command… (Esc to exit)")
		case PaletteModeSQL:
			e.commandPalette.SetPlaceholder("SQL… (Esc to exit)")
		case PaletteModeFind:
			e.commandPalette.SetPlaceholder("Find… (Esc to exit)")
		case PaletteModeUpdate:
			// No placeholder in update mode
			e.commandPalette.SetPlaceholder("")
		}
	}
	if focus {
		e.app.SetFocus(e.commandPalette)
	}
}

// Update the command palette to show a SQL UPDATE preview while editing
func (e *Editor) updateEditPreview(newText string) {
	if e.relation == nil || !e.editMode {
		return
	}
	colName := e.columns[e.currentCol].Name
	preview := e.relation.BuildUpdatePreview(e.records, e.currentRow, colName, newText)
	e.commandPalette.SetPlaceholderStyle(e.placeholderStyleDefault)
	e.commandPalette.SetPlaceholder(preview)
}

func (e *Editor) updateCell(row, col int, newValue string) {
	// Basic bounds check against current data
	if row < 0 || row >= len(e.records) || col < 0 || col >= len(e.records[row]) {
		e.exitEditMode()
		return
	}

	// Delegate DB work to database.go
	updated, err := e.relation.UpdateDBValue(e.records, row, e.columns[col].Name, newValue)
	if err != nil {
		e.exitEditMode()
		return
	}
	// Update in-memory data and refresh table
	ptr := (row + e.pointer) % len(e.records)
	e.records[ptr] = updated

	normalizedRecords := make([][]interface{}, len(e.records))
	for i := 0; i < len(e.records); i++ {
		ptr := (i + e.pointer) % len(e.records)
		normalizedRecords[i] = e.records[ptr]
	}
	e.table.SetData(normalizedRecords)
	e.exitEditMode()
}

func (e *Editor) toggleRowSelection(row int) {
	if row == 0 {
		return
	}

	if e.selectedRows[row] {
		delete(e.selectedRows, row)
		e.unhighlightRow(row)
	} else {
		e.selectedRows[row] = true
		e.highlightRow(row)
	}
}

func (e *Editor) toggleColSelection(col int) {
	if e.selectedCols[col] {
		delete(e.selectedCols, col)
		e.unhighlightColumn(col)
	} else {
		e.selectedCols[col] = true
		e.highlightColumn(col)
	}
}

func (e *Editor) highlightRow(row int) {
	// HeaderTable handles selection highlighting internally
	// This is a placeholder for future row highlighting implementation
}

func (e *Editor) unhighlightRow(row int) {
	// HeaderTable handles selection highlighting internally
	// This is a placeholder for future row highlighting implementation
}

func (e *Editor) highlightColumn(col int) {
	// HeaderTable handles selection highlighting internally
	// This is a placeholder for future column highlighting implementation
}

func (e *Editor) unhighlightColumn(col int) {
	// HeaderTable handles selection highlighting internally
	// This is a placeholder for future column highlighting implementation
}

func (e *Editor) refreshData() {
	if e.rows != nil || e.relation == nil || e.relation.DB == nil {
		return
	}

	colCount := len(e.columns)
	if colCount == 0 || len(e.records) == 0 {
		return
	}

	selectCols := make([]string, colCount)
	for i, col := range e.columns {
		selectCols[i] = quoteIdent(e.relation.DBType, col.Name)
	}

	var builder strings.Builder
	builder.Grow(64 + colCount*16) // rough heuristic to minimize reallocations
	builder.WriteString("SELECT ")
	builder.WriteString(strings.Join(selectCols, ", "))
	builder.WriteString(" FROM ")
	builder.WriteString(quoteQualified(e.relation.DBType, e.relation.name))

	if e.whereClause != "" {
		builder.WriteString(" WHERE ")
		builder.WriteString(e.whereClause)
	}
	if e.orderBy != "" {
		builder.WriteString(" ORDER BY ")
		builder.WriteString(e.orderBy)
	}

	rows, err := e.relation.DB.Query(builder.String())
	if err != nil {
		if e.statusBar != nil {
			e.statusBar.SetText("[red]ERROR: " + err.Error() + "[white]")
		}
		return
	}
	e.rows = rows

	for i := 0; i < len(e.records); i++ {
		ptr := (i + e.pointer) % len(e.records)
		e.records[ptr] = make([]interface{}, len(e.columns))
		scanTargets := make([]interface{}, len(e.columns))
		for j := 0; j < len(e.columns); j++ {
			scanTargets[j] = &e.records[ptr][j]
		}
		if rows.Next() {
			if err := rows.Scan(scanTargets...); err != nil {
				rows.Close()
				e.rows = nil
				if e.statusBar != nil {
					e.statusBar.SetText("[red]ERROR: " + err.Error() + "[white]")
				}
				return
			}
		}
	}
	if err := e.rows.Err(); err != nil {
		if e.statusBar != nil {
			e.statusBar.SetText("[red]ERROR: " + err.Error() + "[white]")
		}
		return
	}
	// Start interruptible timer
	e.rowsTimerReset = make(chan struct{})
	e.rowsTimer = time.AfterFunc(100*time.Millisecond, func() {
		e.app.QueueUpdateDraw(func() {
			e.stopRowsTimer()
		})
	})

	// Timer goroutine to handle resets
	go func() {
		resetChan := e.rowsTimerReset
		timer := e.rowsTimer
		for {
			select {
			case <-resetChan:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(100 * time.Millisecond)
			case <-timer.C:
				return
			}
		}
	}()

	normalizedRecords := make([][]interface{}, len(e.records))
	for i := 0; i < len(e.records); i++ {
		ptr := (i + e.pointer) % len(e.records)
		normalizedRecords[i] = e.records[ptr]
	}
	e.table.SetData(normalizedRecords)
}

// nextRows fetches the next i rows from e.rows and resets the auto-close timer
func (e *Editor) nextRows(i int) error {
	if e.rows == nil {
		// TODO re-initiate query
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
		return nil
	}

	scanTargets := make([]interface{}, colCount)

	for n := 0; n < i && e.rows.Next(); n++ {
		row := make([]interface{}, colCount)
		for j := 0; j < colCount; j++ {
			scanTargets[j] = &row[j]
		}
		if err := e.rows.Scan(scanTargets...); err != nil {
			return err
		}
		e.records = append(e.records, row)
		e.pointer++
	}

	return e.rows.Err()
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
	if e.rows != nil {
		_ = e.rows.Close()
		e.rows = nil
	}
}

func (e *Editor) moveColumn(col, direction int) {
	if col < 0 || col >= len(e.columns) {
		return
	}

	newIdx := col + direction
	if newIdx < 0 || newIdx >= len(e.columns) {
		return
	}

	e.columns[col], e.columns[newIdx] = e.columns[newIdx], e.columns[col]

	for i := range e.records {
		e.records[i][col], e.records[i][newIdx] = e.records[i][newIdx], e.records[i][col]
	}

	e.setupTable()
	e.table.Select(e.currentRow, col+direction)
}

func (e *Editor) adjustColumnWidth(col, delta int) {
	if col < 0 || col >= len(e.columns) {
		return
	}

	newWidth := max(3, e.columns[col].Width+delta)
	e.columns[col].Width = newWidth

	// Update the table column width and re-render
	e.table.SetColumnWidth(col, newWidth)
}

func formatCellValue(value interface{}) string {
	if value == nil {
		return "\\N"
	}

	switch v := value.(type) {
	case []byte:
		return string(v)
	case string:
		if v == "" {
			return ""
		}
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

func (e *Editor) isMultilineColumnType(col int) bool {
	if e.relation == nil || col < 0 || col >= len(e.columns) {
		return false
	}

	column := e.columns[col]
	attrType := ""
	if attr, ok := e.relation.attributes[column.Name]; ok {
		attrType = attr.Type
	}

	// Normalize type for consistent matching
	attrType = strings.ToLower(attrType)

	// Check for text-compatible data types that support multiline content
	return strings.Contains(attrType, "char") ||
		strings.Contains(attrType, "varchar") ||
		strings.Contains(attrType, "text") ||
		attrType == "json" // json but not jsonb
}

// getTerminalHeight returns the height of the terminal in rows
func getTerminalHeight() int {
	type winsize struct {
		Row    uint16
		Col    uint16
		Xpixel uint16
		Ypixel uint16
	}

	ws := &winsize{}
	retCode, _, errno := syscall.Syscall(syscall.SYS_IOCTL,
		uintptr(syscall.Stdin),
		uintptr(syscall.TIOCGWINSZ),
		uintptr(unsafe.Pointer(ws)))

	if int(retCode) == -1 {
		// If we can't get terminal size, return a reasonable default
		_ = errno // avoid unused variable error
		return 24 // Standard terminal height
	}
	return int(ws.Row)
}

// Status bar API methods
func (e *Editor) SetStatusMessage(message string) {
	if e.statusBar != nil {
		e.statusBar.SetText(message)
		e.app.Draw()
	}
}

func (e *Editor) SetStatusError(message string) {
	if e.statusBar != nil {
		e.statusBar.SetText("[red]ERROR: " + message + "[white]")
		e.app.Draw()
	}
}

func (e *Editor) SetStatusLog(message string) {
	if e.statusBar != nil {
		e.statusBar.SetText("[blue]LOG: " + message + "[white]")
		e.app.Draw()
	}
}

// Command execution
func (e *Editor) executeCommand(command string) {
	if command == "" {
		return
	}

	// Split command into parts
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return
	}

	cmd := parts[0]
	args := parts[1:]

	switch cmd {
	case "quit", "q":
		e.app.Stop()
	case "refresh", "r":
		e.SetStatusMessage("Refreshing data...")
		e.refreshData()
		e.SetStatusMessage("Data refreshed")
	case "help", "h":
		e.SetStatusMessage("Commands: quit, refresh, help")
	case "log":
		if len(args) > 0 {
			e.SetStatusLog(strings.Join(args, " "))
		} else {
			e.SetStatusMessage("Usage: log <message>")
		}
	case "error":
		if len(args) > 0 {
			e.SetStatusError(strings.Join(args, " "))
		} else {
			e.SetStatusMessage("Usage: error <message>")
		}
	default:
		e.SetStatusError("Unknown command: " + cmd)
	}
}
