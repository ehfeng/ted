package main

import (
	"testing"
)

func TestPrefixMatchPriority(t *testing.T) {
	// Create a fuzzy selector with test tables
	tables := []string{"orders", "test_users", "users", "user_profiles", "my_users"}

	fs := NewFuzzySelector(tables, "", nil, nil)

	tests := []struct {
		search   string
		expected []string
	}{
		{
			search:   "user",
			expected: []string{"users", "user_profiles", "test_users", "my_users"}, // prefix matches first, then fuzzy
		},
		{
			search:   "test",
			expected: []string{"test_users"}, // only prefix match
		},
		{
			search:   "usr",
			expected: []string{"test_users", "users", "user_profiles", "my_users"}, // all fuzzy matches in original order
		},
		{
			search:   "ord",
			expected: []string{"orders"}, // prefix match
		},
		{
			search:   "",
			expected: []string{"orders", "test_users", "users", "user_profiles", "my_users"}, // all tables
		},
	}

	for _, tt := range tests {
		t.Run(tt.search, func(t *testing.T) {
			filtered, _, _ := fs.calculateFiltered(tt.search)

			if len(filtered) != len(tt.expected) {
				t.Errorf("search %q: expected %d results, got %d", tt.search, len(tt.expected), len(filtered))
				t.Errorf("expected: %v", tt.expected)
				t.Errorf("got: %v", filtered)
				return
			}

			for i, expected := range tt.expected {
				if filtered[i] != expected {
					t.Errorf("search %q: at position %d, expected %q, got %q", tt.search, i, expected, filtered[i])
				}
			}
		})
	}
}

func TestIsPrefixMatch(t *testing.T) {
	tests := []struct {
		search   string
		text     string
		expected bool
	}{
		{"user", "users", true},
		{"user", "user_profiles", true},
		{"user", "test_users", false},
		{"test", "test_users", true},
		{"usr", "users", false},
		{"", "users", true}, // empty string is prefix of everything
		{"USERS", "users", true}, // case insensitive
		{"users", "USERS", true}, // case insensitive
	}

	for _, tt := range tests {
		t.Run(tt.search+":"+tt.text, func(t *testing.T) {
			result := isPrefixMatch(tt.search, tt.text)
			if result != tt.expected {
				t.Errorf("isPrefixMatch(%q, %q) = %v, expected %v", tt.search, tt.text, result, tt.expected)
			}
		})
	}
}
