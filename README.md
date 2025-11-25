![ted logo](.github/logo.png)

ted displays database tables as markdown table and provides spreadsheet-like editing, including mouse support for selecting/editing cells.

![ted demo](ted.gif)

```sh
ted [dbname] [tbl]
ted test users
ted --pg test users
```

`dbname` can either be a database file (sqlite or duckdb) or a database name.

## Installation

```sh
brew tap ehfeng/ted
brew install ted
```

## Common cli flags

### Connection info

- `-d` or `--database`
- `-h` or `--host`
- `-p` or `--port`
- `-u` or `--username`
- `--password`

### Database shorthands

- `--postgres` or `--pg`
- `--mysql` or `--my`

## Supported keyboard shortcuts

### Navigation

1. tab
1. shift+tab
1. arrows
1. home
1. end
1. page up
1. page down
1. ctrl+home/end*
1. home/end
1. cmd+page up/down (fn+cmd+up/down for mac users)

*cmd+up/down are captured by Ghostty

### Data

1. enter: edit/down/new row (if at bottom)
1. shift+enter: edit/up (if at top)
1. esc: exit from editing, discarding changes
1. ctrl+f: find in column
1. ctrl+r: start insert row
1. ctrl+enter: execute insert row
1. ctrl+del: delete row
1. alt+←/→: rearranges column display order
1. ctrl+</>: increase/decrease column width
1. ctrl+q: exit
1. shift+enter: start tx
1. shift+enter: commit tx
1. ctrl+c: rollback tx

### vim

1. hjkl: arrows
1. g: home
1. G: end
1. ctrl+b: page up

1. i: edit mode
1. a: append mode

## Mouse

You can select cells, resize columns, and scroll with the mouse.

## Non-goals

Schema changes. ted is for browsing and editing data, schema changes better done in dedicated clients

Filtering: better to use Find to jump to the value.

Sorting: better to use views and ORDER BY

## Development

```sh
# initial
go install
ted completion zsh > /usr/local/share/zsh/site-functions/_ted
exec zsh

# development
make watch
ted test.db users 2>/tmp/ted.log
tail -f /tmp/ted.log

# testing completions
ted __complete "pg" "t" 2>&1
```

### Release

```sh
git tag -a v0.1.2 -m "Release notes"
git push origin v0.1.2
```
