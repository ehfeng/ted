//go:build debug

package main

import (
	"fmt"
	"os"
	"sync"
	"time"
)

var (
	debugFile *os.File
	debugMu   sync.Mutex
)

func init() {
	var err error
	debugFile, err = os.OpenFile("/tmp/ted.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open debug log file: %v\n", err)
		os.Exit(1)
	}
}

// debugLog writes debug messages to /tmp/ted.log when built with -tags debug
func debugLog(format string, args ...interface{}) {
	debugMu.Lock()
	defer debugMu.Unlock()

	timestamp := time.Now().Format("15:04:05.000")
	fmt.Fprintf(debugFile, "[%s] "+format, append([]interface{}{timestamp}, args...)...)
}

// debugLogKeys formats and logs multi-column keys
func debugLogKeys(prefix string, keys []any) {
	if keys == nil {
		debugLog("%s: nil\n", prefix)
		return
	}
	debugLog("%s: %v (len=%d)\n", prefix, keys, len(keys))
}

// debugLogRow formats and logs row data (first few columns only to avoid spam)
func debugLogRow(prefix string, row []any) {
	if row == nil {
		debugLog("%s: nil\n", prefix)
		return
	}
	if len(row) <= 5 {
		debugLog("%s: %v\n", prefix, row)
	} else {
		debugLog("%s: %v... (len=%d)\n", prefix, row[:5], len(row))
	}
}
