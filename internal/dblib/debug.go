//go:build debug

package dblib

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
