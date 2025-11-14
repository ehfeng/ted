# Architecture for ted

## cli

cobra app for parsing flags, shell completion, aggregating config and constructing the editor

## Editor (tview app)

```go
type Editor struct {
  config Config

  // references
  app *tview.Application
  pages *tview.Pages
  relation *Relation
  db *sql.DB

  // internal editor state
  table string
  editing bool
  mode Mode
  status string
  kittySequenceActive     bool
	kittySequenceBuffer     string

	// timer for auto-closing rows
	rowsTimer      *time.Timer
	rowsTimerReset chan struct{}

	// timer for auto-refreshing data
	refreshTimer     *time.Timer
	refreshTimerStop chan struct{}
}

type Mode int

const (
	ModeView Mode = iota
	ModeCommand
	ModeSQL
	ModeGoto
	ModeUpdate
	ModeInsert
	ModeDelete
  ModeOpen
)

type Column struct {
  name string
  width int
}

type Config struct {
  VimMode bool
  Columns []Column
}
```

pages
- (table only visible if table is set) table page
- fuzzy selector page (overlays)
- cell editor (overlays, only visible in editing mode)

Use `screen.SetCursorStyle` to set cursor style to 5 when focused on editing or searching tables.

### FuzzySelector Primitive

```go
type FuzzySelector struct {
  *tview.Box

  items    []string
  selected int
  search   string
}


func NewFuzzySelector() *FuzzySelector

// calculates fuzzy search match positions based
func (*FuzzySelector) Draw(tcell.Screen)
```

After an item is selected from FuzzySelector, HidePage on the FuzzySelector page and 

### `Database` struct


