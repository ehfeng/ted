package dblib

import (
	"database/sql"
	"os"
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
