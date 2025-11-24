package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
)

func (e *Editor) setupKeyBindings() {
	e.table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		key := event.Key()
		rune := event.Rune()
		mod := event.Modifiers()

		row, col := e.table.GetSelection()

		// Disable all selection navigation when in delete mode (except Enter to confirm and Escape to cancel)
		if e.paletteMode == PaletteModeDelete {
			switch key {
			case tcell.KeyEnter, tcell.KeyEscape:
				// Allow these to fall through for delete mode handling
			case tcell.KeyUp, tcell.KeyDown, tcell.KeyLeft, tcell.KeyRight,
				tcell.KeyHome, tcell.KeyEnd, tcell.KeyPgUp, tcell.KeyPgDn,
				tcell.KeyTab, tcell.KeyBacktab, tcell.KeyBackspace, tcell.KeyBackspace2,
				tcell.KeyDelete:
				// Block all navigation and editing keys
				return nil
			default:
				// Block any other keys except control keys
				if key == tcell.KeyRune {
					return nil
				}
			}
		}

		// Record keyboard event in breadcrumbs (but not during edit mode or command input)
		if breadcrumbs != nil && !e.editing {
			keyStr := fmt.Sprintf("%v", key)
			if key == tcell.KeyRune {
				keyStr = string(rune)
			}
			modStr := ""
			if mod&tcell.ModCtrl != 0 {
				modStr += "Ctrl+"
			}
			if mod&tcell.ModShift != 0 {
				modStr += "Shift+"
			}
			if mod&tcell.ModAlt != 0 {
				modStr += "Alt+"
			}
			breadcrumbs.RecordKeyboard(keyStr, modStr)
		}

		if e.consumeKittyCSI(key, rune, mod) {
			return nil
		}
		if !e.kittySequenceActive {
			if key == tcell.KeyRune && mod&tcell.ModCtrl != 0 && rune == '`' {
				e.kittySequenceBuffer = "ctrl+`"
			} else {
				e.kittySequenceBuffer = ""
			}
		}

		if !e.kittySequenceActive && e.kittySequenceBuffer == "ctrl+`" {
			e.kittySequenceBuffer = ""
			e.setPaletteMode(PaletteModeSQL, true)
			return nil
		}

		// Ctrl+S: execute INSERT in insert mode (save and insert)
		if (rune == 's' || rune == 19) && mod&tcell.ModCtrl != 0 {
			if len(e.table.insertRow) > 0 && !e.editing {
				e.executeInsert()
				return nil
			}
		}

		// Ctrl+O: open/close table picker
		if (rune == 'o' || rune == 15) && mod&tcell.ModCtrl != 0 {
			e.pages.ShowPage(pagePicker)
			// Set focus on the selector, which will forward focus to the input field
			e.app.SetFocus(e.tablePicker)
			// Set cursor style to blinking bar
			e.app.SetAfterDrawFunc(func(screen tcell.Screen) {
				screen.SetCursorStyle(tcell.CursorStyleBlinkingBar)
			})
			return nil
		}
		// Ctrl+R: Insert mode
		if (rune == 'r' || rune == 18) && mod&tcell.ModCtrl != 0 {
			// Ctrl+I: Jump to end and enable insert mode
			e.loadFromRowId(nil, false, 0)
			go func() {
				e.app.QueueUpdateDraw(func() { e.nextRows(1) })
			}()
			e.table.SetupInsertRow()
			e.updateStatusForInsertMode()
			e.renderData()
			_, col := e.table.GetSelection()
			e.table.Select(e.table.GetDataLength(), col)
			return nil
		}

		// Alt+0: set cell to null for nullable columns in insert mode
		if rune == '0' && mod&tcell.ModAlt != 0 {
			if len(e.table.insertRow) > 0 && !e.editing && e.relation != nil {
				// Check if the selected column is nullable
				colName := e.columns[col].Name
				if attr, ok := e.relation.Attributes[colName]; ok && attr.Nullable {
					e.table.insertRow[col] = nil
					e.renderData()
				}
				return nil
			}
		}

		switch {
		case key == tcell.KeyEnter:
			if len(e.table.insertRow) > 0 && !e.editing && mod&tcell.ModAlt != 0 {
				e.executeInsert()
				return nil
			}
			// Execute delete in delete mode
			if e.paletteMode == PaletteModeDelete {
				// Check if we're at the bottom of the table
				e.executeDelete()
				e.setPaletteMode(PaletteModeDefault, false)
				e.app.SetFocus(e.table)
				e.table.Select(row, col)
				return nil
			}
			// Enter: enter edit mode
			e.enterEditMode(row, col)
			return nil
		case key == tcell.KeyEscape:
			if e.app.GetFocus() == e.commandPalette {
				e.setPaletteMode(PaletteModeDefault, false)
				e.app.SetFocus(e.table)
				return nil
			}

			if e.paletteMode == PaletteModeDelete {
				e.setPaletteMode(PaletteModeDefault, false)
				e.app.SetFocus(e.table)
				e.SetStatusMessage("Ready")
				return nil
			}

			if len(e.table.insertRow) > 0 {
				// Exit insert mode and select last real row
				e.table.ClearInsertRow()

				// Find the last non-nil record
				lastIdx := len(e.buffer) - 1
				for lastIdx >= 0 && e.buffer[lastIdx].data == nil {
					lastIdx--
				}

				e.renderData()
				_, col := e.table.GetSelection()
				e.table.Select(lastIdx, col)
				return nil
			}
			e.exitEditMode()
			return nil
		case key == tcell.KeyTab:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			e.navigateTab(false)
			return nil
		case key == tcell.KeyBacktab:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			e.navigateTab(true)
			return nil
		case key == tcell.KeyHome:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			if mod&tcell.ModCtrl != 0 {
				if len(e.table.insertRow) > 0 {
					return nil // Disable vertical navigation in insert mode
				}
				// Ctrl+Home: jump to first row
				e.loadFromRowId(nil, true, col)
				e.table.Select(0, col)
				return nil
			}
			e.table.Select(row, 0)
			return nil
		case key == tcell.KeyEnd:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			if mod&tcell.ModCtrl != 0 {
				if len(e.table.insertRow) > 0 {
					return nil // Disable vertical navigation in insert mode
				}
				// Ctrl+End: jump to last row
				e.loadFromRowId(nil, false, col)
				e.table.Select(e.lastRowIdx()-1, col) // -1 because the last row is the bottom border
				return nil
			}
			e.table.Select(row, len(e.columns)-1)
			return nil
		case key == tcell.KeyPgUp:
			if len(e.table.insertRow) > 0 || e.paletteMode == PaletteModeDelete {
				return nil // Disable vertical navigation in insert mode or delete mode
			}
			// Page up: scroll data backward while keeping selection in same visual position
			pageSize := max(1, e.table.rowsHeight-1)
			e.prevRows(pageSize)
			return nil
		case key == tcell.KeyPgDn:
			if len(e.table.insertRow) > 0 || e.paletteMode == PaletteModeDelete {
				return nil // Disable vertical navigation in insert mode or delete mode
			}
			// Page down: scroll data forward while keeping selection in same visual position
			pageSize := max(1, e.table.rowsHeight-1)
			// Keep selection at same position, just fetch next rows
			go func() {
				e.app.QueueUpdateDraw(func() { e.nextRows(pageSize) })
			}()
			return nil
		// Ctrl+F sends ACK (6) or 'f' depending on terminal
		case (rune == 'f' || rune == 6) && mod&tcell.ModCtrl != 0:
			e.setPaletteMode(PaletteModeFind, true)
			return nil
		// Ctrl+P sends DLE (16) or 'p' depending on terminal
		case (rune == 'p' || rune == 16) && mod&tcell.ModCtrl != 0:
			e.setPaletteMode(PaletteModeCommand, true)
			return nil
		// Ctrl+` sends BEL (0) or '`' depending on terminal
		case (rune == '`' || rune == 0) && mod&tcell.ModCtrl != 0:
			e.setPaletteMode(PaletteModeSQL, true)
			return nil
		case key == tcell.KeyLeft && mod&tcell.ModAlt != 0:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			e.moveColumn(col, -1)
			return nil
		case key == tcell.KeyRight && mod&tcell.ModAlt != 0:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			e.moveColumn(col, 1)
			return nil
		case key == tcell.KeyRune && rune == '=' && mod&tcell.ModCtrl != 0:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			e.adjustColumnWidth(col, 1)
			return nil
		case key == tcell.KeyRune && rune == '-' && mod&tcell.ModCtrl != 0:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			e.adjustColumnWidth(col, -1)
			return nil
		case key == tcell.KeyLeft && mod&tcell.ModMeta != 0:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			e.table.Select(row, 0)
			return nil
		case key == tcell.KeyRight && mod&tcell.ModMeta != 0:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			e.table.Select(row, len(e.columns)-1)
			return nil
		case key == tcell.KeyLeft:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
		case key == tcell.KeyRight:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
		case key == tcell.KeyUp:
			if len(e.table.insertRow) > 0 || e.paletteMode == PaletteModeDelete {
				return nil // Disable vertical navigation in insert mode or delete mode
			}
			if mod&tcell.ModMeta != 0 {
				e.table.Select(0, col)
				return nil
			} else {
				if row == 0 {
					go func() {
						e.app.QueueUpdateDraw(func() { e.prevRows(1) })
					}()
				} else {
					e.table.Select(row-1, col)
				}
				return nil
			}
		case key == tcell.KeyDown:
			if len(e.table.insertRow) > 0 || e.paletteMode == PaletteModeDelete {
				return nil // Disable vertical navigation in insert mode or delete mode
			}
			if mod&tcell.ModMeta != 0 {
				if len(e.buffer[len(e.buffer)-1].data) == 0 {
					e.table.Select(len(e.buffer)-2, col)
				} else {
					e.table.Select(len(e.buffer)-1, col)
				}
				return nil
			} else {
				if row == len(e.buffer)-1 {
					go func() {
						e.app.QueueUpdateDraw(func() { e.nextRows(1) })
					}()
				} else {
					if len(e.buffer[row+1].data) == 0 {
						e.table.Select(row+2, col)
					} else {
						e.table.Select(row+1, col)
					}
				}
				return nil
			}
		case key == tcell.KeyBackspace || key == tcell.KeyBackspace2 || key == tcell.KeyDEL || key == tcell.KeyDelete:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			// Backspace or Delete: start editing with empty string
			e.enterEditModeWithInitialValue(row, col, "")
			return nil
		case (rune == 'd' || rune == 4) && mod&tcell.ModCtrl != 0:
			// Ctrl+D: enter delete mode
			e.enterDeleteMode(row, col)
			return nil
		// Vim mode keybindings
		case e.vimMode && key == tcell.KeyRune && rune == 'h' && mod == 0:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			// h: move left
			if col > 0 {
				e.table.Select(row, col-1)
			}
			return nil
		case e.vimMode && key == tcell.KeyRune && rune == 'l' && mod == 0:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			// l: move right
			if col < len(e.columns)-1 {
				e.table.Select(row, col+1)
			}
			return nil
		case e.vimMode && key == tcell.KeyRune && rune == 'j' && mod == 0:
			if len(e.table.insertRow) > 0 || e.paletteMode == PaletteModeDelete {
				return nil // Disable vertical navigation in insert mode or delete mode
			}
			// j: move down
			if row == len(e.buffer)-1 {
				go func() {
					e.app.QueueUpdateDraw(func() { e.nextRows(1) })
				}()
			} else {
				if len(e.buffer[row+1].data) == 0 {
					e.table.Select(row+2, col)
				} else {
					e.table.Select(row+1, col)
				}
			}
			return nil
		case e.vimMode && key == tcell.KeyRune && rune == 'k' && mod == 0:
			if len(e.table.insertRow) > 0 || e.paletteMode == PaletteModeDelete {
				return nil // Disable vertical navigation in insert mode or delete mode
			}
			// k: move up
			if row == 0 {
				e.prevRows(1)
			} else {
				e.table.Select(row-1, col)
			}
			return nil
		case e.vimMode && key == tcell.KeyRune && rune == 'g' && mod == 0:
			// Disable in insert/delete mode
			if len(e.table.insertRow) > 0 || e.paletteMode == PaletteModeDelete {
				return nil
			}
			// Check if this is a double 'g' press (gg)
			if time.Since(e.lastGPress) < 500*time.Millisecond {
				// gg: jump to first row
				e.loadFromRowId(nil, true, col)
				e.table.Select(0, col)
				e.lastGPress = time.Time{} // Reset
			} else {
				// Single g also jumps to first row (per spec)
				e.loadFromRowId(nil, true, col)
				e.table.Select(0, col)
				e.lastGPress = time.Now() // Track for potential gg
			}
			return nil
		case e.vimMode && key == tcell.KeyRune && rune == 'G':
			// Disable in insert/delete mode
			if len(e.table.insertRow) > 0 || e.paletteMode == PaletteModeDelete {
				return nil
			}
			// G: jump to last row
			e.loadFromRowId(nil, false, col)
			e.table.Select(e.lastRowIdx()-1, col)
			return nil
		case e.vimMode && key == tcell.KeyRune && (rune == '0' || rune == '^') && mod == 0:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			// 0 or ^: jump to first column
			e.table.Select(row, 0)
			return nil
		case e.vimMode && key == tcell.KeyRune && rune == '$' && mod&tcell.ModShift != 0:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			// $: jump to last column
			e.table.Select(row, len(e.columns)-1)
			return nil
		case e.vimMode && key == tcell.KeyRune && rune == 'i' && mod == 0:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			// i: enter edit mode with all text selected
			e.enterEditModeWithSelection(row, col, true)
			return nil
		case e.vimMode && key == tcell.KeyRune && rune == 'a' && mod == 0:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return nil
			}
			// a: enter edit mode with cursor at end
			e.enterEditModeWithSelection(row, col, false)
			return nil
		case e.vimMode && (rune == 'b' || rune == 2) && mod&tcell.ModCtrl != 0:
			if len(e.table.insertRow) > 0 || e.paletteMode == PaletteModeDelete {
				return nil // Disable vertical navigation in insert mode or delete mode
			}
			// Ctrl+B: page up (already handled by KeyPgUp, but vim users might use Ctrl+B)
			pageSize := max(1, e.table.rowsHeight-1)
			e.prevRows(pageSize)
			return nil
		default:
			// Disable in delete mode
			if e.paletteMode == PaletteModeDelete {
				return event
			}
			// In vim mode, don't auto-enter edit mode on typing
			// (use 'i' or 'a' instead)
			if !e.vimMode && key == tcell.KeyRune && rune != 0 &&
				mod&(tcell.ModAlt|tcell.ModCtrl|tcell.ModMeta) == 0 {
				e.enterEditModeWithInitialValue(row, col, string(rune))
				return nil
			}
		}

		return event
	})
}

func (e *Editor) consumeKittyCSI(key tcell.Key, r rune, mod tcell.ModMask) bool {
	if e.kittySequenceActive {
		if key != tcell.KeyRune {
			e.kittySequenceActive = false
			e.kittySequenceBuffer = ""
			return false
		}

		if r == 'u' {
			seq := e.kittySequenceBuffer
			e.kittySequenceActive = false
			e.kittySequenceBuffer = ""
			parts := strings.SplitN(seq, ";", 2)
			if len(parts) == 2 {
				codepoint, err1 := strconv.Atoi(parts[0])
				modifier, err2 := strconv.Atoi(parts[1])
				if err1 == nil && err2 == nil {
					mask := modifier - 1
					// Check if Ctrl is pressed (bit 2, value 4)
					if mask&4 != 0 {
						_, col := e.table.GetSelection()
						switch codepoint {
						case 96: // Ctrl+` (backtick)
							e.setPaletteMode(PaletteModeSQL, true)
						case 105: // Ctrl+I: Jump to end and enable insert mode
							e.table.SetupInsertRow()
							e.loadFromRowId(nil, false, 0)
							e.updateStatusForInsertMode()
							// Select the insert mode row (which is at index len(data))
							e.table.Select(e.table.GetDataLength(), col)
						case 61: // Ctrl+= (increase column width)
							if e.paletteMode != PaletteModeDelete {
								e.adjustColumnWidth(col, 1)
							}
						case 45: // Ctrl+- (decrease column width)
							if e.paletteMode != PaletteModeDelete {
								e.adjustColumnWidth(col, -1)
							}
						}
					}
				}
			}
			return true
		}

		e.kittySequenceBuffer += string(r)
		return true
	}

	if key == tcell.KeyRune && r == '[' {
		e.kittySequenceActive = true
		e.kittySequenceBuffer = ""
		return true
	}

	return false
}

func (e *Editor) navigateTab(reverse bool) {
	row, col := e.table.GetSelection()

	if reverse {
		if col > 0 {
			e.table.Select(row, col-1)
		} else if row > 0 {
			e.table.Select(row-1, len(e.columns)-1)
		}
	} else {
		if col < len(e.columns)-1 {
			e.table.Select(row, col+1)
		} else if row < len(e.buffer)-1 {
			e.table.Select(row+1, 0)
		}
	}
}
