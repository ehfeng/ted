//go:build !debug

package dblib

// debugLog is a no-op in release builds
func debugLog(format string, args ...interface{}) {}
