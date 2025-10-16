package main

// getTerminalHeight returns the height of the terminal in rows
func getTerminalHeight() int {
	// Windows doesn't support the same syscall interface
	// Return a reasonable default
	return 24
}
