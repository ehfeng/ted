package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
)

// BreadcrumbType represents the type of breadcrumb event
type BreadcrumbType string

const (
	BreadcrumbKeyboard   BreadcrumbType = "keyboard"
	BreadcrumbMouse      BreadcrumbType = "mouse"
	BreadcrumbNavigation BreadcrumbType = "navigation"
	BreadcrumbDatabase   BreadcrumbType = "database"
)

// BreadcrumbEntry represents a single breadcrumb event
type BreadcrumbEntry struct {
	Type      BreadcrumbType
	Message   string
	Data      map[string]interface{}
	Timestamp time.Time
	Level     sentry.Level
}

// BreadcrumbBuffer is a thread-safe circular buffer for breadcrumbs with aggregation
type BreadcrumbBuffer struct {
	entries        []BreadcrumbEntry
	maxSize        int
	currentIndex   int
	count          int
	mu             sync.RWMutex
	lastAggregated *BreadcrumbEntry
}

// NewBreadcrumbBuffer creates a new breadcrumb buffer with the given max size
func NewBreadcrumbBuffer(maxSize int) *BreadcrumbBuffer {
	return &BreadcrumbBuffer{
		entries:        make([]BreadcrumbEntry, maxSize),
		maxSize:        maxSize,
		count:          0,
		lastAggregated: nil,
	}
}

// addEntry adds an entry to the buffer, aggregating if possible
func (b *BreadcrumbBuffer) addEntry(entry BreadcrumbEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Try to aggregate with the last entry
	if b.canAggregate(b.lastAggregated, &entry) {
		// Update the last entry's message to reflect aggregation
		if b.count > 0 {
			lastIdx := (b.currentIndex - 1 + b.maxSize) % b.maxSize
			b.entries[lastIdx].Message = fmt.Sprintf("%s (x%d)", entry.Message, 2)
		}
		b.lastAggregated = &entry
		return
	}

	// Add as a new entry
	b.entries[b.currentIndex] = entry
	b.lastAggregated = &entry
	b.currentIndex = (b.currentIndex + 1) % b.maxSize
	if b.count < b.maxSize {
		b.count++
	}
}

// canAggregate checks if two breadcrumbs can be aggregated
func (b *BreadcrumbBuffer) canAggregate(last *BreadcrumbEntry, current *BreadcrumbEntry) bool {
	if last == nil || current == nil {
		return false
	}

	// Only aggregate same type breadcrumbs
	if last.Type != current.Type {
		return false
	}

	// Only aggregate if within 100ms of each other
	if current.Timestamp.Sub(last.Timestamp) > 100*time.Millisecond {
		return false
	}

	// For keyboard events, aggregate if similar keys
	if current.Type == BreadcrumbKeyboard {
		lastKey, ok1 := last.Data["key"].(string)
		currentKey, ok2 := current.Data["key"].(string)
		if ok1 && ok2 && lastKey == currentKey {
			return true
		}
	}

	// For navigation, aggregate same mode changes
	if current.Type == BreadcrumbNavigation {
		lastMode, ok1 := last.Data["mode"].(string)
		currentMode, ok2 := current.Data["mode"].(string)
		if ok1 && ok2 && lastMode == currentMode {
			return true
		}
	}

	return false
}

// RecordKeyboard records a keyboard event
func (b *BreadcrumbBuffer) RecordKeyboard(key string, modifiers string) {
	entry := BreadcrumbEntry{
		Type:      BreadcrumbKeyboard,
		Message:   fmt.Sprintf("Key: %s", key),
		Timestamp: time.Now(),
		Level:     sentry.LevelDebug,
		Data: map[string]interface{}{
			"key":       key,
			"modifiers": modifiers,
		},
	}
	b.addEntry(entry)
}

// RecordMouse records a mouse event
func (b *BreadcrumbBuffer) RecordMouse(action string) {
	entry := BreadcrumbEntry{
		Type:      BreadcrumbMouse,
		Message:   fmt.Sprintf("Mouse: %s", action),
		Timestamp: time.Now(),
		Level:     sentry.LevelDebug,
		Data: map[string]interface{}{
			"action": action,
		},
	}
	b.addEntry(entry)
}

// RecordNavigation records a navigation event (e.g., palette mode change)
func (b *BreadcrumbBuffer) RecordNavigation(mode string, description string) {
	entry := BreadcrumbEntry{
		Type:      BreadcrumbNavigation,
		Message:   fmt.Sprintf("Navigation: %s - %s", mode, description),
		Timestamp: time.Now(),
		Level:     sentry.LevelInfo,
		Data: map[string]interface{}{
			"mode":        mode,
			"description": description,
		},
	}
	b.addEntry(entry)
}

// RecordDatabase records a database operation
func (b *BreadcrumbBuffer) RecordDatabase(operation string) {
	entry := BreadcrumbEntry{
		Type:      BreadcrumbDatabase,
		Message:   fmt.Sprintf("DB: %s", operation),
		Timestamp: time.Now(),
		Level:     sentry.LevelInfo,
		Data: map[string]interface{}{
			"operation": operation,
		},
	}
	b.addEntry(entry)
}

// Flush sends breadcrumbs to Sentry, aggregating consecutive identical events
func (b *BreadcrumbBuffer) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.count == 0 {
		return
	}

	// Collect entries in chronological order
	var entries []BreadcrumbEntry

	if b.count < b.maxSize {
		// Buffer not full, entries are in order from 0 to currentIndex-1
		for i := 0; i < b.count; i++ {
			entries = append(entries, b.entries[i])
		}
	} else {
		// Buffer is full (circular), entries wrap around
		for i := 0; i < b.maxSize; i++ {
			idx := (b.currentIndex + i) % b.maxSize
			entries = append(entries, b.entries[idx])
		}
	}

	// Aggregate consecutive identical events
	var sentryBreadcrumbs []*sentry.Breadcrumb
	i := 0
	for i < len(entries) {
		current := entries[i]
		count := 1

		// Count consecutive identical events
		for i+count < len(entries) && entries[i+count].Type == current.Type &&
			entries[i+count].Message == current.Message {
			count++
		}

		// Create breadcrumb (with count if aggregated)
		message := current.Message
		data := current.Data
		if count > 1 {
			message = fmt.Sprintf("%s (x%d)", current.Message, count)
			data = make(map[string]interface{})
			for k, v := range current.Data {
				data[k] = v
			}
			data["count"] = count
		}

		sentryBreadcrumbs = append(sentryBreadcrumbs, &sentry.Breadcrumb{
			Message:   message,
			Category:  string(current.Type),
			Data:      data,
			Timestamp: current.Timestamp,
			Level:     current.Level,
		})

		i += count
	}

	// Add breadcrumbs to Sentry scope
	sentry.ConfigureScope(func(scope *sentry.Scope) {
		for _, bc := range sentryBreadcrumbs {
			scope.AddBreadcrumb(bc, 100)
		}
	})

	// Clear the buffer
	b.entries = make([]BreadcrumbEntry, b.maxSize)
	b.currentIndex = 0
	b.count = 0
	b.lastAggregated = nil
}

// Global breadcrumb buffer instance
var breadcrumbs *BreadcrumbBuffer

// InitBreadcrumbs initializes the global breadcrumb buffer
func InitBreadcrumbs(maxSize int) {
	breadcrumbs = NewBreadcrumbBuffer(maxSize)
}
