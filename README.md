# ted

ted is a tabular editor. It displays database tables as markdown table and provides spreadsheet-like editing, including mouse support for selecting cells (with Opt key) or adjust column widths.

```sh
ted [dbname] [tbl.col,col]
ted [dbname] [tbl.col,col]
ted test users.id,name
ted test -c "select * from users where name = 'eric'"
```

`[dbname]` is a filename (sqlite or duckdb) or a database name in the `.ted.yml` config. If no matching name is found and database type flag is not set, suggest `ted init`. Support for tab-completing database (from either local files or config), table and column names.

## Common flags

You can directly provide connection data with flags.

- `-d` or `--database`
- `-h` or `--host`
- `-p` or `--port`
- `-U` or `--username`
- `-W` or `--password`
- `-c` or `--command`

## Supported keyboard shortcuts

### Navigation

1. tab
1. shift+tab
1. arrows
1. home
1. end
1. page up
1. page down
1. hjkl "arrow" keys
1. cmd+arrow: jump to edge

### Data

1. enter: edit/down/new row (if at bottom)
1. esc: exit from editing, discarding changes
1. shift+enter: edit/up (if at top)
1. shift+space: select row (for deletion)
1. ctrl+space: select column (for deletion)
1. cmd+n: new row
1. cmd+r: refresh data
1. cmd+f: find
1. alt+↑/↓: moves row up/down
1. alt+←/→: rearranges column order
1. ctrl+←/→: increase/decrease column width

## `.ted.yml` format

```yml
databases:
  [name]: <postgres|mysql|clickhouse>
  [name]:
    type: <postgres|mysql|clickhouse>
    host: [host] # opt
    port: [port] # opt
    user: [user] # optional, assumes system username
    dbname: [dbname] # opt, assumes name

    format: # database specific formats
      int: 3 # database type format
      users: # table specific col selection, order, size
        - id: 4
        - name # default width
        - preferences: 20

format:
  int: 3
  text: 12
```

### Example

```yml
databases:
  test: postgres
  bloggy:
    type: postgres
    username: bloggy
```

## Table UI

Get terminal size to size column widths and how many rows to display.

Focusing a cell always brings its full width into the view.

Each column has a default width of 8 characters + 2 spaces + pipe = 11 characters. 7 columns + 1 pipe = 78 characters. Column minimum width is 3 characters. `…` indicates overflow. Additional columns flow off screen, just like spreadsheets. Focusing a cell minimally scrolls to fit the cell into the editor window.

When editing values with overflow, it overlays _on top_ of the table with a light background. Overflow overlay flows to the right and below the cell. When value overflows the editor window, it word wraps. The editing overlay _never_ exceeds the terminal window size.

JSON is treated as text. JSONB is pretty printed.

```md
| greet…| numb…| boo…|
|:----- | ----:| ---:|
| hello…|    1 |   t |
| goodb…|    2 |   f |
| see y…|…3456 |   f |

| gree… | numb…| boo…|
|:0---- | ----:| ---:|
| hello kind sir   t |
| fare thee well?█ f |
| see … |    3 |   f |
```

## Status Bar

Status bar in the bottom row where contextual information can be displayed, like warnings, errors, contextual information.

The status bar default shows the current database.table and the row number/total in the bottom right.

During viewing, show summary stats. During editing, show column data type, constraints.

When highlighting or editing foreign key references, the status bar shows a logfmt preview. Say we highlight `accounts.owner`, which references the users table: `users: id=1 name='Eric' plan='free'…`.

If the sheet does not have a primary key, the status bar will show warning upon opening the file or updating data.

If an update fails, status bar shows error message.

## Data flow

Use streaming to fetch results and split pipe results to display and a temporary file.

The "view" of the table is always just a cache.

Updates are run with `RETURNING *` clause, attempt to update just the row and *not* refresh the entire table.

When updating cells, identify a table's primary keys or unique constraints (even if they are multicolumn). If none exists, warn that updates are "best effort" and are made by `WHERE`'ing matching values. If the number of rows updated >1, message in the status bar.

## Nice to have's

Support for editing rows in a view or query as long as the primary key is included. This requires query parsing to determine which rows are read-only (computed or join values cannot be edited) and how to change the relevant rows (which columns are that value's primary key).

Undo/redo.

## Non-goals

Transactions and multicolumn sort. This is an editor, not a sql editor or a psql replacement.

DDL. This is for editing data, not schemas. DDL is best done with SQL.

Real-time database updates. This is not possible without modifying the database (adding triggers). You also do not want to lock the table from external writes: this should not be necessary in local development and is never a good idea in production.

Column filtering or sorting. You're re-implementing SQL at this point.

Pasting. Unclear how you'd map things, especially if focus is not on the first column.
