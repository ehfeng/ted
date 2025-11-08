//go:build debug

package main

import (
	"fmt"
	"os"
)

// debugLog writes debug messages to stderr when built with -tags debug
func debugLog(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[DEBUG] "+format, args...)
}
