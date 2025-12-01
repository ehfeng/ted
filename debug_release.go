//go:build !debug

package main

// debugLog is a no-op in release builds (zero overhead)
func debugLog(format string, args ...interface{}) {
	// Intentionally empty - this will be optimized away by the compiler
}

// debugLogKeys is a no-op in release builds
func debugLogKeys(prefix string, keys []any) {
	// Intentionally empty
}

// debugLogRow is a no-op in release builds
func debugLogRow(prefix string, row []any) {
	// Intentionally empty
}
