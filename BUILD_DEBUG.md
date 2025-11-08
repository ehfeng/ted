# Debug Build Instructions

Ted now supports conditional debug logging using Go build tags.

## Building

### Development Build (with debug logging)
```bash
go build -tags debug
```

This enables comprehensive debug output to stderr that traces:
- Database connection and table loading
- SQL query generation and execution
- Row scanning progress
- Data rendering pipeline

### Release Build (no debug overhead)
```bash
go build
```

This completely removes all debug code at compile time (zero overhead).

## Using Debug Output

When running a debug build, all debug messages go to stderr:

```bash
# View debug output in terminal
./ted mydb mytable

# Save debug output to file
./ted mydb mytable 2> debug.log

# View only debug output (hide UI)
./ted mydb mytable 2>&1 > /dev/null
```

## Debug Output Format

All debug messages follow the pattern:
```
[DEBUG] function: message
```

Example output when loading a table:
```
[DEBUG] runEditor: starting for table users
[DEBUG] runEditor: connecting to database
[DEBUG] loadFromRowId: starting, fromTop=true, focusColumn=0, id=[]
[DEBUG] QueryRows: generating query, sortCol=<nil>, params=[], inclusive=true, scrollDown=true
[DEBUG] QueryRows: executing query: SELECT id, name, email FROM users ORDER BY id ASC
[DEBUG] loadFromRowId: scanning row 0
[DEBUG] loadFromRowId: Scan succeeded for row 0
```

## Troubleshooting Freezes

If ted freezes when opening a Postgres table, the last debug message shows where it stopped:

- Freeze after "calling DB.Query" → Query execution hanging
- Freeze after "scanning row N" → Row N+1 scan hanging
- Freeze after "calling Scan" → Data type conversion issue

Use this information to diagnose database-specific issues.
