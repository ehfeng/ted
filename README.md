# ted: tabular editor

ted displays database tables as markdown table and provides spreadsheet-like editing, including mouse support for selecting/editing cells.

![ted demo](ted.gif)

```sh
ted [dbname] [tbl]
ted test users
```

`dbname` can either be a database file (sqlite or duckdb) or a database name.

## Installation

```sh
brew tap ehfeng/ted
brew install ted
```

## Common flags

### Connection info

- `-d` or `--database`
- `-h` or `--host`
- `-p` or `--port`
- `-u` or `--username`
- `--password`

### Database shorthands

- `--pg`
- `--my` or `--mysql`

## Supported keyboard shortcuts

### Navigation

1. tab
1. shift+tab
1. arrows
1. home
1. end
1. page up
1. page down
1. cmd+arrow

### Data

1. enter: edit/down/new row (if at bottom)
1. shift+enter: edit/up (if at top)
1. esc: exit from editing, discarding changes
1. shift+del: delete row (shift+space selects row in Excel)
1. ctrl+r: refresh data
1. alt+←/→: rearranges column order
1. alt+ꜛ/↓: toggles sort
1. ctrl+del: hides column
1. ctrl+</>: increase/decrease column width
1. ctrl+c: exit

## Mouse

Lets you interact with elements, selecting cells, resizing or unhiding columns. Selecting a range of cells lets you copy a csv to clipboard.

1. click: select cell, change column sort, adjust col widths, show hidden cols
1. scroll

## Table UI

Get terminal size to size column widths and how many rows to display.

Focusing a cell always brings its full width into the view.

Each column has a default width of 8 characters + 2 spaces + pipe = 11 characters. 7 columns + 1 pipe = 78 characters. Column minimum width is 3 characters. `…` indicates overflow. Additional columns flow off screen, just like spreadsheets. Focusing a cell minimally scrolls to fit the cell into the editor window.

When editing values with overflow, it overlays _on top_ of the table with a light background. Overflow overlay flows to the right and below the cell. When value overflows the editor window, it word wraps. The editing overlay _never_ exceeds the terminal window size.

Default null sentinel is `\N` (configurable). `\\N` to input actual string.

JSON is treated as text. JSONB is pretty printed.

## Status Bar

Status bar in the bottom row where contextual information can be displayed, like warnings, errors, contextual information.

In view mode, show column data type, constraints. In editing mode, input-specific context, including whether input is "valid"

- run check constraints, test against unique index
- valid int, bool, float, json
- enums
- date time format
- references: foreign row preview

When highlighting or editing foreign key references, the status bar shows a logfmt preview. Say we highlight `accounts.owner`, which references the users table: `users: id=1 name='Eric' plan='free'…`. Multi-column references only work if both columns are shown. Show info message (other column hidden) otherwise.

If a table lacks a key (no primary, unique index with not null columns or nulls not distinct), the status bar shows read-only and reason.

If an update fails, status bar shows error message.

## Command palette

At the bottom, command input. Command palette has different modes: sql, goto, etc.

## Data flow

Use streaming to fetch results and split pipe results to display and a temporary file.

The "view" of the table is always just a cache.

Updates are run with `RETURNING *` clause, attempt to update just the row and *not* refresh the entire table.

When updating cells, identify a table's primary keys or unique constraints (even if they are multicolumn). If none exists, warn that updates are "best effort" and are made by `WHERE`'ing matching values. If the number of rows updated >1, message in the status bar.

## Nice to have's

Undo/redo with `cmd+z` and `cmd+shift+z` shortcuts.

If primary key does not exist, use sqlite/duckdb `rowid` or postgres `ctid`. This approach is vulnerable to non-exclusive access (updates) or VACUUMs. Also requires modifying update `RETURNING *, rowid|ctid`. It does not work for mysql or clickhouse. Throw a warning in the status bar if using this approach.

Tabs, split views

## Non-goals

Transactions. This is an editor, not a sql editor or a psql replacement.

Column filtering or sorting. You're re-implementing SQL at this point.

Support for views. Incredibly difficult to trace a view column to its source table. Editing via views is not useful. `WHERE`, `ORDER BY` and `LIMIT` are supported as flags, which should cover most cases.

DDL. This is for editing data, not schemas. DDL is best done with SQL.

Real-time database updates. This is not possible without modifying the database (adding triggers). You also do not want to lock the table from external writes: this should not be necessary in local development and is never a good idea in production.

Pasting cells (separate from pasting values). Unclear how you'd map things, especially if focus is not on the first column.

## Development

```sh
watchexec --restart --exts go,mod,sum -- 'go install'
ted test.db users 2>/tmp/ted.log
tail -f /tmp/ted.log
```

