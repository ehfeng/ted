package main

import (
	"testing"

	"github.com/rivo/tview"
)

func TestValidateAndCleanSQL(t *testing.T) {
	// Create a minimal Editor instance for testing
	e := &Editor{
		app: tview.NewApplication(),
	}

	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "simple query without semicolon",
			input:    "SELECT * FROM users",
			expected: "SELECT * FROM users",
			wantErr:  false,
		},
		{
			name:     "query with trailing semicolon",
			input:    "SELECT * FROM users;",
			expected: "SELECT * FROM users",
			wantErr:  false,
		},
		{
			name:     "query with trailing semicolon and whitespace",
			input:    "SELECT * FROM users;  ",
			expected: "SELECT * FROM users",
			wantErr:  false,
		},
		{
			name:     "multiple statements",
			input:    "SELECT * FROM users; DELETE FROM users",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "multiple statements with trailing semicolon",
			input:    "SELECT * FROM users; DELETE FROM users;",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "query with leading whitespace",
			input:    "  SELECT * FROM users",
			expected: "SELECT * FROM users",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := e.validateAndCleanSQL(tt.input)
			if tt.wantErr {
				if result != "" {
					t.Errorf("validateAndCleanSQL() expected empty string for invalid input, got %q", result)
				}
			} else {
				if result != tt.expected {
					t.Errorf("validateAndCleanSQL() = %q, want %q", result, tt.expected)
				}
			}
		})
	}
}
