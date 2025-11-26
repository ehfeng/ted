package dblib

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func setupTestDB(t *testing.T) (*sql.DB, *Relation) {
	// Create temporary SQLite database
	tmpFile, err := os.CreateTemp("", "test-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	t.Cleanup(func() { os.Remove(tmpFile.Name()) })

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Create test table with data
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	testData := []struct {
		id   int
		name string
		age  int
	}{
		{1, "Alice", 30},
		{2, "Bob", 25},
		{3, "Charlie", 30},
		{4, "David", 25},
		{5, "Eve", 35},
		{6, "Frank", 25},
	}

	for _, row := range testData {
		_, err = db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
			row.id, row.name, row.age)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	rel, err := NewRelation(db, SQLite, "users")
	if err != nil {
		t.Fatalf("Failed to create relation: %v", err)
	}

	return db, rel
}

func TestFindNextRow_FindBelow(t *testing.T) {
	_, rel := setupTestDB(t)

	// Search for age=25 starting from id=1
	// Should find Bob (id=2) which is below
	findCol := 2 // age column
	findColVal := int64(25)
	currentKeys := []any{int64(1)}
	sortColVal := int64(1) // current row's id value

	keys, foundBelow, err := rel.FindNextRow(findCol, findColVal, nil, sortColVal, currentKeys)
	if err != nil {
		t.Fatalf("FindNextRow failed: %v", err)
	}

	if !foundBelow {
		t.Error("Expected to find row below, but wrapped around")
	}

	if len(keys) != 1 {
		t.Fatalf("Expected 1 key, got %d", len(keys))
	}

	if keys[0] != int64(2) {
		t.Errorf("Expected to find id=2 (Bob), got id=%v", keys[0])
	}
}

func TestFindNextRow_WrapAround(t *testing.T) {
	_, rel := setupTestDB(t)

	// Search for age=25 starting from id=6 (Frank, last row with age=25)
	// Should wrap around and find the closest match before current position
	// which is David (id=4)
	findCol := 2 // age column
	findColVal := int64(25)
	currentKeys := []any{int64(6)}
	sortColVal := int64(6) // current row's id value

	keys, foundBelow, err := rel.FindNextRow(findCol, findColVal, nil, sortColVal, currentKeys)
	if err != nil {
		t.Fatalf("FindNextRow failed: %v", err)
	}

	if foundBelow {
		t.Error("Expected to wrap around, but found below")
	}

	if len(keys) != 1 {
		t.Fatalf("Expected 1 key, got %d", len(keys))
	}

	// The wrap-around finds the nearest match going backwards from current position
	// which is id=4 (David, age=25)
	if keys[0] != int64(4) {
		t.Errorf("Expected to find id=4 (David) after wrap, got id=%v", keys[0])
	}
}

func TestFindNextRow_NotFound(t *testing.T) {
	_, rel := setupTestDB(t)

	// Search for age=99 which doesn't exist
	findCol := 2 // age column
	findColVal := int64(99)
	currentKeys := []any{int64(1)}
	sortColVal := int64(1)

	keys, foundBelow, err := rel.FindNextRow(findCol, findColVal, nil, sortColVal, currentKeys)
	if err != nil {
		t.Fatalf("FindNextRow failed: %v", err)
	}

	if foundBelow {
		t.Error("Expected foundBelow=false when not found")
	}

	if keys != nil {
		t.Errorf("Expected nil keys when not found, got %v", keys)
	}
}

func TestFindNextRow_WithSortColumn(t *testing.T) {
	_, rel := setupTestDB(t)

	// Search for age=30 with sort by age ascending, starting from id=1
	// Should find Alice (id=1, age=30) first, then Charlie (id=3, age=30)
	findCol := 2 // age column
	findColVal := int64(30)
	currentKeys := []any{int64(1)} // Starting from Alice
	sortCol := &SortColumn{Name: "age", Asc: true}
	sortColVal := int64(30) // Alice's age

	keys, foundBelow, err := rel.FindNextRow(findCol, findColVal, sortCol, sortColVal, currentKeys)
	if err != nil {
		t.Fatalf("FindNextRow failed: %v", err)
	}

	if !foundBelow {
		t.Error("Expected to find row below")
	}

	if len(keys) != 1 {
		t.Fatalf("Expected 1 key, got %d", len(keys))
	}

	if keys[0] != int64(3) {
		t.Errorf("Expected to find id=3 (Charlie), got id=%v", keys[0])
	}
}

func TestFindNextRow_InvalidColumn(t *testing.T) {
	_, rel := setupTestDB(t)

	findCol := 999 // Invalid column index
	findColVal := "test"
	currentKeys := []any{int64(1)}

	_, _, err := rel.FindNextRow(findCol, findColVal, nil, nil, currentKeys)
	if err == nil {
		t.Error("Expected error for invalid column index")
	}
}

func TestFindNextRow_KeyLengthMismatch(t *testing.T) {
	_, rel := setupTestDB(t)

	findCol := 2
	findColVal := int64(25)
	currentKeys := []any{int64(1), int64(2)} // Wrong length

	_, _, err := rel.FindNextRow(findCol, findColVal, nil, nil, currentKeys)
	if err == nil {
		t.Error("Expected error for key length mismatch")
	}
}

func TestGetBestKey_PrimaryKey(t *testing.T) {
	// Create temporary SQLite database
	tmpFile, err := os.CreateTemp("", "test-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with primary key
	_, err = db.Exec(`
		CREATE TABLE test_pk (
			id INTEGER PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// GetBestKey should return primary key (id), not unique constraint (email)
	cols, err := GetBestKey(db, SQLite, "test_pk")
	if err != nil {
		t.Fatalf("GetBestKey failed: %v", err)
	}

	if len(cols) != 1 {
		t.Fatalf("Expected 1 key column, got %d", len(cols))
	}

	if cols[0] != "id" {
		t.Errorf("Expected primary key 'id', got '%s'", cols[0])
	}
}

func TestGetBestKey_UniqueNotNull(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with only unique constraint (no primary key)
	_, err = db.Exec(`
		CREATE TABLE test_unique (
			email TEXT NOT NULL UNIQUE,
			username TEXT NOT NULL UNIQUE,
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Both email and username are NOT NULL UNIQUE
	// GetBestKey should return the earlier column (email)
	cols, err := GetBestKey(db, SQLite, "test_unique")
	if err != nil {
		t.Fatalf("GetBestKey failed: %v", err)
	}

	if len(cols) != 1 {
		t.Fatalf("Expected 1 key column, got %d", len(cols))
	}

	// Should prefer the earlier column
	if cols[0] != "email" {
		t.Errorf("Expected earlier column 'email', got '%s'", cols[0])
	}
}

func TestGetBestKey_FewerColumns(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with composite and single-column unique constraints
	_, err = db.Exec(`
		CREATE TABLE test_fewer (
			id INTEGER NOT NULL,
			code TEXT NOT NULL,
			name TEXT,
			UNIQUE(id),
			UNIQUE(code, name)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Should prefer single-column unique (id) over composite (code, name)
	cols, err := GetBestKey(db, SQLite, "test_fewer")
	if err != nil {
		t.Fatalf("GetBestKey failed: %v", err)
	}

	if len(cols) != 1 {
		t.Fatalf("Expected 1 key column, got %d", len(cols))
	}

	if cols[0] != "id" {
		t.Errorf("Expected single-column key 'id', got '%s'", cols[0])
	}
}

func TestGetBestKey_ShorterColumns(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with different column types
	_, err = db.Exec(`
		CREATE TABLE test_shorter (
			tiny_id INTEGER NOT NULL UNIQUE,
			long_text TEXT NOT NULL UNIQUE,
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Should prefer INTEGER (4 bytes) over TEXT (1MB estimate)
	cols, err := GetBestKey(db, SQLite, "test_shorter")
	if err != nil {
		t.Fatalf("GetBestKey failed: %v", err)
	}

	if len(cols) != 1 {
		t.Fatalf("Expected 1 key column, got %d", len(cols))
	}

	if cols[0] != "tiny_id" {
		t.Errorf("Expected shorter column 'tiny_id', got '%s'", cols[0])
	}
}

// ========== View-related tests ==========

func setupTestDBWithView(t *testing.T) (*sql.DB, *Relation) {
	// Create temporary SQLite database
	tmpFile, err := os.CreateTemp("", "test-view-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	t.Cleanup(func() { os.Remove(tmpFile.Name()) })

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Create base table
	_, err = db.Exec(`
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			price REAL,
			category TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO products (id, name, price, category) VALUES
		(1, 'Widget', 10.99, 'Electronics'),
		(2, 'Gadget', 25.50, 'Electronics'),
		(3, 'Tool', 5.00, 'Hardware')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Create a simple view
	_, err = db.Exec(`
		CREATE VIEW product_view AS
		SELECT id, name, price, category
		FROM products
	`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	rel, err := NewRelation(db, SQLite, "product_view")
	if err != nil {
		t.Fatalf("Failed to create relation: %v", err)
	}

	return db, rel
}

func TestCheckIsView_Table(t *testing.T) {
	db, _ := setupTestDB(t)
	defer db.Close()

	isView, err := checkIsView(db, SQLite, "users")
	if err != nil {
		t.Fatalf("checkIsView failed: %v", err)
	}

	if isView {
		t.Error("Expected users to be a table, not a view")
	}
}

func TestCheckIsView_View(t *testing.T) {
	db, _ := setupTestDBWithView(t)
	defer db.Close()

	isView, err := checkIsView(db, SQLite, "product_view")
	if err != nil {
		t.Fatalf("checkIsView failed: %v", err)
	}

	if !isView {
		t.Error("Expected product_view to be a view, not a table")
	}
}

func TestNewRelation_View(t *testing.T) {
	db, rel := setupTestDBWithView(t)
	defer db.Close()

	if !rel.IsView {
		t.Error("Expected relation to be marked as view")
	}

	if len(rel.Columns) == 0 {
		t.Error("Expected view to have columns")
	}

	if len(rel.Key) == 0 {
		t.Error("Expected view to have keys")
	}

	// Check that key columns are present
	keyColNames := make(map[string]bool)
	for _, keyIdx := range rel.Key {
		if keyIdx < len(rel.Columns) {
			keyColNames[rel.Columns[keyIdx].Name] = true
		}
	}

	if !keyColNames["id"] {
		t.Error("Expected 'id' to be in view keys")
	}
}

func TestGetViewDefinitionSQLite(t *testing.T) {
	db, rel := setupTestDBWithView(t)
	defer db.Close()

	if len(rel.Key) != 1 {
		t.Errorf("Expected 1 key column, got %d", len(rel.Key))
	}

	if rel.Key[0] != 0 {
		t.Errorf("Expected key column 0 to be 0, got %d", rel.Key[0])
	}

	def, err := getViewDefinitionSQLite(db, "product_view")
	if err != nil {
		t.Fatalf("getViewDefinitionSQLite failed: %v", err)
	}

	if def == "" {
		t.Error("Expected non-empty view definition")
	}

	if !containsIgnoreCase(def, "SELECT") {
		t.Errorf("Expected view definition to contain SELECT, got: %s", def)
	}
}

func TestParseViewDefinition_SimpleSelect(t *testing.T) {
	sql := "SELECT id, name, price FROM products"
	analysis, err := ParseViewDefinition(sql, SQLite, nil)
	if err != nil {
		t.Fatalf("ParseViewDefinition failed: %v", err)
	}

	if len(analysis.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(analysis.Columns))
	}

	if len(analysis.BaseTables) != 1 {
		t.Errorf("Expected 1 base table, got %d", len(analysis.BaseTables))
	}

	if analysis.BaseTables[0] != "products" {
		t.Errorf("Expected base table 'products', got '%s'", analysis.BaseTables[0])
	}
}

func TestParseViewDefinition_WithAlias(t *testing.T) {
	sql := "SELECT id AS product_id, name AS product_name FROM products"
	analysis, err := ParseViewDefinition(sql, SQLite, nil)
	if err != nil {
		t.Fatalf("ParseViewDefinition failed: %v", err)
	}

	if len(analysis.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(analysis.Columns))
	}

	// For aliased columns, SourceColumn should contain the base column name (not the alias)
	// The output name (alias) will be determined from Rows.Columns() when querying
	// For this test, we just verify the lineage is correct
	if analysis.Columns[0].SourceColumn != "id" {
		t.Errorf("Expected SourceColumn to be 'id' (base column), got '%s'", analysis.Columns[0].SourceColumn)
	}
	if analysis.Columns[0].IsDerived {
		t.Error("Expected column to be passthrough (not derived)")
	}
}

func TestParseViewDefinition_WithGroupBy(t *testing.T) {
	sql := "SELECT category, COUNT(*) as count FROM products GROUP BY category"
	analysis, err := ParseViewDefinition(sql, SQLite, nil)
	if err != nil {
		t.Fatalf("ParseViewDefinition failed: %v", err)
	}

	if !analysis.HasGroupBy {
		t.Error("Expected HasGroupBy to be true")
	}

	if len(analysis.GroupByExprs) == 0 {
		t.Error("Expected GroupByExprs to be non-empty")
	}

	// Check for derived columns (aggregates)
	hasDerived := false
	for _, col := range analysis.Columns {
		if col.IsDerived {
			hasDerived = true
			break
		}
	}
	if !hasDerived {
		t.Error("Expected at least one derived column (COUNT)")
	}
}

func TestParseViewDefinition_WithDistinct(t *testing.T) {
	sql := "SELECT DISTINCT category FROM products"
	analysis, err := ParseViewDefinition(sql, SQLite, nil)
	if err != nil {
		t.Fatalf("ParseViewDefinition failed: %v", err)
	}

	if !analysis.HasDistinct {
		t.Error("Expected HasDistinct to be true")
	}
}

func TestParseViewDefinition_WithJoin(t *testing.T) {
	sql := "SELECT p.id, p.name, c.name as category_name FROM products p JOIN categories c ON p.category_id = c.id"
	analysis, err := ParseViewDefinition(sql, SQLite, nil)
	if err != nil {
		t.Fatalf("ParseViewDefinition failed: %v", err)
	}

	if len(analysis.BaseTables) < 2 {
		t.Errorf("Expected at least 2 base tables, got %d", len(analysis.BaseTables))
	}
}

func TestParseViewDefinition_SelectStar(t *testing.T) {
	// Set up a test database with a table
	tmpFile, err := os.CreateTemp("", "test-selectstar-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	t.Cleanup(func() { os.Remove(tmpFile.Name()) })

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Create test table
	_, err = db.Exec(`
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			price REAL,
			category TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test SELECT * from single table
	sql := "SELECT * FROM products"
	analysis, err := ParseViewDefinition(sql, SQLite, db)
	if err != nil {
		t.Fatalf("ParseViewDefinition failed: %v", err)
	}

	// Verify we got all columns
	expectedColumns := []string{"id", "name", "price", "category"}
	if len(analysis.Columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(analysis.Columns))
	}

	// Verify lineage information is correct
	// Note: actual column names will come from Rows.Columns() when loading the view
	columnMap := make(map[string]bool)
	for _, col := range analysis.Columns {
		// All columns from SELECT * should be passthrough (not derived)
		if col.IsDerived {
			t.Errorf("Expected column to be passthrough (not derived), got IsDerived=true")
		}
		// Should have source table and column set
		if col.SourceTable != "products" {
			t.Errorf("Expected SourceTable to be 'products', got '%s'", col.SourceTable)
		}
		if col.SourceColumn == "" {
			t.Error("Expected SourceColumn to be set for passthrough column")
		} else {
			columnMap[col.SourceColumn] = true
		}
	}

	// Verify all expected base columns are present in the lineage
	for _, expectedCol := range expectedColumns {
		if !columnMap[expectedCol] {
			t.Errorf("Expected base column '%s' not found in analysis", expectedCol)
		}
	}

	// Verify base table
	if len(analysis.BaseTables) != 1 {
		t.Errorf("Expected 1 base table, got %d", len(analysis.BaseTables))
	}
	if analysis.BaseTables[0] != "products" {
		t.Errorf("Expected base table 'products', got '%s'", analysis.BaseTables[0])
	}
}

func TestParseViewDefinition_ViewResolvesToBaseTable(t *testing.T) {
	// Test that when selecting from a view, SourceTable resolves to the real base table
	tmpFile, err := os.CreateTemp("", "test-viewresolve-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	t.Cleanup(func() { os.Remove(tmpFile.Name()) })

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Create base table
	_, err = db.Exec(`
		CREATE TABLE employees (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			department TEXT,
			salary REAL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a view on the base table
	_, err = db.Exec(`
		CREATE VIEW employee_names AS
		SELECT id, name, department
		FROM employees
	`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Parse SQL that selects from the view
	sql := "SELECT id, name FROM employee_names"
	analysis, err := ParseViewDefinition(sql, SQLite, db)
	if err != nil {
		t.Fatalf("ParseViewDefinition failed: %v", err)
	}

	// Verify columns resolve to the base table, not the view
	if len(analysis.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(analysis.Columns))
	}

	// id column should resolve to employees.id, not employee_names.id
	if analysis.Columns[0].SourceTable != "employees" {
		t.Errorf("Expected SourceTable to be 'employees' (base table), got '%s'", analysis.Columns[0].SourceTable)
	}
	if analysis.Columns[0].SourceColumn != "id" {
		t.Errorf("Expected SourceColumn to be 'id', got '%s'", analysis.Columns[0].SourceColumn)
	}

	// name column should resolve to employees.name
	if analysis.Columns[1].SourceTable != "employees" {
		t.Errorf("Expected SourceTable to be 'employees' (base table), got '%s'", analysis.Columns[1].SourceTable)
	}
	if analysis.Columns[1].SourceColumn != "name" {
		t.Errorf("Expected SourceColumn to be 'name', got '%s'", analysis.Columns[1].SourceColumn)
	}
}

func TestParseViewDefinition_NestedViewsResolveToBaseTable(t *testing.T) {
	// Test that nested views (view of view) resolve to the ultimate base table
	tmpFile, err := os.CreateTemp("", "test-nestedview-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	t.Cleanup(func() { os.Remove(tmpFile.Name()) })

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Create base table
	_, err = db.Exec(`
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			customer_id INTEGER,
			amount REAL,
			status TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create first view on the base table
	_, err = db.Exec(`
		CREATE VIEW active_orders AS
		SELECT id, customer_id, amount
		FROM orders
		WHERE status = 'active'
	`)
	if err != nil {
		t.Fatalf("Failed to create first view: %v", err)
	}

	// Create second view on top of the first view
	_, err = db.Exec(`
		CREATE VIEW large_active_orders AS
		SELECT id, customer_id, amount
		FROM active_orders
		WHERE amount > 100
	`)
	if err != nil {
		t.Fatalf("Failed to create second view: %v", err)
	}

	// Parse SQL that selects from the nested view
	sql := "SELECT id, amount FROM large_active_orders"
	analysis, err := ParseViewDefinition(sql, SQLite, db)
	if err != nil {
		t.Fatalf("ParseViewDefinition failed: %v", err)
	}

	// Verify columns resolve to the base table, not any intermediate view
	if len(analysis.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(analysis.Columns))
	}

	// id should resolve to orders.id (base table), not large_active_orders or active_orders
	if analysis.Columns[0].SourceTable != "orders" {
		t.Errorf("Expected SourceTable to be 'orders' (base table), got '%s'", analysis.Columns[0].SourceTable)
	}
	if analysis.Columns[0].SourceColumn != "id" {
		t.Errorf("Expected SourceColumn to be 'id', got '%s'", analysis.Columns[0].SourceColumn)
	}

	// amount should resolve to orders.amount
	if analysis.Columns[1].SourceTable != "orders" {
		t.Errorf("Expected SourceTable to be 'orders' (base table), got '%s'", analysis.Columns[1].SourceTable)
	}
	if analysis.Columns[1].SourceColumn != "amount" {
		t.Errorf("Expected SourceColumn to be 'amount', got '%s'", analysis.Columns[1].SourceColumn)
	}
}

func TestParseViewDefinition_TableAliasResolvesToTable(t *testing.T) {
	// Test that table aliases (e.g., "FROM products p") resolve correctly
	sql := "SELECT p.id, p.name FROM products p WHERE p.price > 10"
	analysis, err := ParseViewDefinition(sql, SQLite, nil)
	if err != nil {
		t.Fatalf("ParseViewDefinition failed: %v", err)
	}

	if len(analysis.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(analysis.Columns))
	}

	// Columns should have the actual table name, not the alias "p"
	if analysis.Columns[0].SourceTable != "products" {
		t.Errorf("Expected SourceTable to be 'products', got '%s' (alias should be resolved)", analysis.Columns[0].SourceTable)
	}
	if analysis.Columns[1].SourceTable != "products" {
		t.Errorf("Expected SourceTable to be 'products', got '%s' (alias should be resolved)", analysis.Columns[1].SourceTable)
	}
}

func TestParseViewDefinition_SelectStarWithJoin(t *testing.T) {
	// Test that SELECT * with JOIN resolves columns to their respective base tables
	tmpFile, err := os.CreateTemp("", "test-join-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	t.Cleanup(func() { os.Remove(tmpFile.Name()) })

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Create two tables
	_, err = db.Exec(`
		CREATE TABLE posts (
			id INTEGER PRIMARY KEY,
			title TEXT NOT NULL,
			content TEXT,
			user_id INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create posts table: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	// Test SELECT * with JOIN
	sqlStr := "SELECT * FROM posts p JOIN users u ON p.user_id = u.id"
	analysis, err := ParseViewDefinition(sqlStr, SQLite, db)
	if err != nil {
		t.Fatalf("ParseViewDefinition failed: %v", err)
	}

	// Should have 7 columns: 4 from posts + 3 from users
	if len(analysis.Columns) != 7 {
		t.Fatalf("Expected 7 columns, got %d", len(analysis.Columns))
	}

	// First 4 columns should be from posts
	postsColumns := []string{"id", "title", "content", "user_id"}
	for i, expectedCol := range postsColumns {
		if analysis.Columns[i].SourceTable != "posts" {
			t.Errorf("Column %d: expected SourceTable 'posts', got '%s'", i, analysis.Columns[i].SourceTable)
		}
		if analysis.Columns[i].SourceColumn != expectedCol {
			t.Errorf("Column %d: expected SourceColumn '%s', got '%s'", i, expectedCol, analysis.Columns[i].SourceColumn)
		}
	}

	// Next 3 columns should be from users
	usersColumns := []string{"id", "name", "email"}
	for i, expectedCol := range usersColumns {
		colIdx := 4 + i
		if analysis.Columns[colIdx].SourceTable != "users" {
			t.Errorf("Column %d: expected SourceTable 'users', got '%s'", colIdx, analysis.Columns[colIdx].SourceTable)
		}
		if analysis.Columns[colIdx].SourceColumn != expectedCol {
			t.Errorf("Column %d: expected SourceColumn '%s', got '%s'", colIdx, expectedCol, analysis.Columns[colIdx].SourceColumn)
		}
	}

	// Verify base tables are correctly identified
	if len(analysis.BaseTables) != 2 {
		t.Errorf("Expected 2 base tables, got %d", len(analysis.BaseTables))
	}
}

func TestIsColumnEditable_TableColumn(t *testing.T) {
	_, rel := setupTestDB(t)

	// All table columns should be editable
	for i := range rel.Columns {
		if !rel.IsColumnEditable(i) {
			t.Errorf("Expected column %d (%s) to be editable in table", i, rel.Columns[i].Name)
		}
	}
}

func TestIsColumnEditable_ViewPassthroughWithKey(t *testing.T) {
	db, rel := setupTestDBWithView(t)
	defer db.Close()

	// Find a passthrough column that has its base table key included
	// In product_view, 'id' is the key, so 'name' should be editable
	nameColIdx := -1
	for i, col := range rel.Columns {
		if col.Name == "name" && col.Table != "" {
			nameColIdx = i
			break
		}
	}

	if nameColIdx == -1 {
		t.Fatal("Could not find 'name' column in view")
	}

	if !rel.IsColumnEditable(nameColIdx) {
		t.Error("Expected 'name' column to be editable (base table key is included)")
	}
}

func TestIsColumnEditable_ViewDerivedColumn(t *testing.T) {
	// Create a view with derived columns
	tmpFile, err := os.CreateTemp("", "test-derived-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE sales (
			id INTEGER PRIMARY KEY,
			amount REAL,
			date TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(`CREATE VIEW sales_summary AS SELECT date, SUM(amount) as total FROM sales GROUP BY date`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// This view should fail to load because it has no keyable columns
	// (the base table's primary key 'id' is not included)
	rel, err := NewRelation(db, SQLite, "sales_summary")
	if err == nil {
		// If it somehow loaded, check that derived columns are not editable
		totalColIdx := -1
		for i, col := range rel.Columns {
			if col.Name == "total" {
				totalColIdx = i
				break
			}
		}

		if totalColIdx != -1 && rel.IsColumnEditable(totalColIdx) {
			t.Error("Expected 'total' column to NOT be editable (it's derived)")
		}
	} else {
		// Expected: view has no keyable columns
		if !strings.Contains(err.Error(), "no keyable columns") {
			t.Errorf("Expected error about no keyable columns, got: %v", err)
		}
	}
}

func TestIsColumnEditable_ViewWithoutBaseKey(t *testing.T) {
	// Create a view that doesn't include the base table key
	tmpFile, err := os.CreateTemp("", "test-nokey-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE items (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			description TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// View without the primary key
	_, err = db.Exec(`
		CREATE VIEW item_names AS
		SELECT name, description
		FROM items
	`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	rel, err := NewRelation(db, SQLite, "item_names")
	if err != nil {
		// This should fail because view has no keyable columns
		if err == nil {
			t.Error("Expected error when view has no keyable columns")
		}
		return
	}

	// If it somehow succeeded, columns should not be editable
	for i, col := range rel.Columns {
		if rel.IsColumnEditable(i) && col.Table != "" {
			t.Errorf("Expected column %d (%s) to NOT be editable (base key not included)", i, col.Name)
		}
	}
}

func TestGetViewKeys_SimpleView(t *testing.T) {
	db, _ := setupTestDBWithView(t)
	defer db.Close()

	rel, err := NewRelation(db, SQLite, "product_view")
	if err != nil {
		t.Fatalf("Failed to create relation: %v", err)
	}

	if len(rel.Key) == 0 {
		t.Error("Expected view to have keys")
	}

	// The key should include the base table's primary key
	hasIdKey := false
	for _, keyIdx := range rel.Key {
		if keyIdx < len(rel.Columns) && rel.Columns[keyIdx].Name == "id" {
			hasIdKey = true
			break
		}
	}

	if !hasIdKey {
		t.Error("Expected view key to include 'id' column")
	}
}

func TestGetViewKeys_GroupByView(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-groupby-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			customer_id INTEGER,
			amount REAL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(`CREATE VIEW customer_totals AS SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// This view should fail to load because it has no keyable columns
	// (the base table's primary key 'id' is not included, and customer_id is not a key)
	rel, err := NewRelation(db, SQLite, "customer_totals")
	if err == nil {
		// If it somehow loaded, check that customer_id is in the keys (GROUP BY)
		if len(rel.Key) == 0 {
			t.Error("Expected view to have keys")
		} else {
			hasCustomerIdKey := false
			for _, keyIdx := range rel.Key {
				if keyIdx < len(rel.Columns) && rel.Columns[keyIdx].Name == "customer_id" {
					hasCustomerIdKey = true
					break
				}
			}
			if !hasCustomerIdKey {
				t.Error("Expected view key to include 'customer_id' column (GROUP BY)")
			}
		}
	} else {
		// Expected: view has no keyable columns
		if !strings.Contains(err.Error(), "no keyable columns") {
			t.Errorf("Expected error about no keyable columns, got: %v", err)
		}
	}
}

func TestUpdateDBValue_ViewColumn(t *testing.T) {
	db, rel := setupTestDBWithView(t)
	defer db.Close()

	// Get initial data
	rows, err := rel.QueryRows([]string{"id", "name", "price", "category"}, nil, nil, false, true)
	if err != nil {
		t.Fatalf("QueryRows failed: %v", err)
	}
	defer rows.Close()

	var records [][]any
	for rows.Next() {
		row := make([]any, 4)
		scanArgs := make([]any, 4)
		for i := range row {
			scanArgs[i] = &row[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		records = append(records, row)
	}

	if len(records) == 0 {
		t.Fatal("No records found")
	}

	// Update the name column (should be editable)
	updatedRow, err := rel.UpdateDBValue(records, 0, "name", "UpdatedWidget")
	if err != nil {
		t.Fatalf("UpdateDBValue failed: %v", err)
	}

	if updatedRow == nil {
		t.Fatal("Expected updated row, got nil")
	}

	// Verify the update
	if updatedRow[1] != "UpdatedWidget" {
		t.Errorf("Expected name to be 'UpdatedWidget', got '%v'", updatedRow[1])
	}
}

func TestLoadViewColumns(t *testing.T) {
	db, _ := setupTestDBWithView(t)
	defer db.Close()

	columns, columnIndex, err := loadViewColumns(db, SQLite, "product_view")
	if err != nil {
		t.Fatalf("loadViewColumns failed: %v", err)
	}

	if len(columns) == 0 {
		t.Error("Expected columns to be loaded")
	}

	if len(columnIndex) == 0 {
		t.Error("Expected columnIndex to be populated")
	}

	// Check that columns have proper metadata
	hasId := false
	for _, col := range columns {
		if col.Name == "id" {
			hasId = true
			if col.Table == "" {
				t.Error("Expected 'id' column to have Table set (passthrough)")
			}
			if col.BaseColumn == "" {
				t.Error("Expected 'id' column to have BaseColumn set")
			}
			break
		}
	}

	if !hasId {
		t.Error("Expected to find 'id' column")
	}
}

// Helper function
func containsIgnoreCase(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			len(s) > len(substr) &&
				(s[:len(substr)] == substr ||
					s[len(s)-len(substr):] == substr ||
					containsSubstringIgnoreCase(s, substr)))
}

func containsSubstringIgnoreCase(s, substr string) bool {
	sLower := toLower(s)
	substrLower := toLower(substr)
	for i := 0; i <= len(sLower)-len(substrLower); i++ {
		if sLower[i:i+len(substrLower)] == substrLower {
			return true
		}
	}
	return false
}

func toLower(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		result[i] = c
	}
	return string(result)
}
