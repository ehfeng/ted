# ted

tabular editor

```sh
ted [db] [tbl.col,col]

ted test users.id,name
```

`[db]` is assumed to be a filename (sqlite or duckdb). If no matching file is found, assume it's Postgres or MySQL and attempt to connect. If `.ted.yml` is present in any parent directory, it will search for named databases and use the connection parameters. `~/.ted.yml` will also be consulted.

Table and column arguments are optional. You will be prompted for table after, with some autocomplete.

## Common flags

- `-h` or `--host`
- `-p` or `--port`
- `-U` or `--username`
- `-W` or `--password`

## Supported keys

1. tab
1. shift+tab
1. arrow
1. cmd+arrow: jump to edge
1. enter: edit/down/new row (if at bottom)
1. shift+enter: edit/up (if at top)
1. home
1. end
1. page up
1. page down
1. shift+space
1. cmd+c, cmd+v for copy paste

bottom row: foreign key lookups, errors (for violating constraints)

## Nice to have's

Support for view as long as the primary key is included (requires parsing table and view definitions).
