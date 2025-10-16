package main

import (
	"syscall"
	"unsafe"
)

// getTerminalHeight returns the height of the terminal in rows
func getTerminalHeight() int {
	type winsize struct {
		Row    uint16
		Col    uint16
		Xpixel uint16
		Ypixel uint16
	}

	ws := &winsize{}
	_, _, _ = syscall.Syscall(syscall.SYS_IOCTL,
		uintptr(syscall.Stdin),
		uintptr(syscall.TIOCGWINSZ),
		uintptr(unsafe.Pointer(ws)))

	if ws.Row == 0 {
		// If we can't get terminal size, return a reasonable default
		return 24 // Standard terminal height
	}
	return int(ws.Row)
}
