# mdsql

SQL queries for markdown tables.

> **Warning**: This is an experimental program that modifies files in-place.
> Always use version control (git) or back up your markdown files before running
> INSERT or DELETE operations.

## Install

```bash
cargo install mdsql
```

From source:

```bash
cargo install --path .
```

## Usage

```bash
# List tables in a file
mdsql tables data.md

# Query first table
mdsql query "SELECT * FROM 0" data.md

# Filter and sort
mdsql query "SELECT name, score FROM 0 WHERE score > 50 ORDER BY score DESC" data.md

# Output as JSON
mdsql query --format json "SELECT * FROM 0" data.md

# Insert a row
mdsql insert "INSERT INTO 0 (name, age, city) VALUES ('Alice', '30', 'NYC')" data.md

# Delete rows
mdsql delete "DELETE FROM 0 WHERE name = 'Alice'" data.md
```

## Supported SQL

- `SELECT` with column filtering and `*`
- `WHERE` with `=`, `!=`, `<`, `>`, `<=`, `>=`, `AND`, `OR`
- `ORDER BY` (ASC/DESC)
- `LIMIT`
- `INSERT INTO ... VALUES`
- `DELETE FROM ... WHERE`

Tables are referenced by 0-based index: `FROM 0`, `INTO 0`, etc.

## Output Formats

- `columns` (default) - Column-aligned like `df -h`, unix-friendly
- `markdown` - Markdown table
- `csv` - Comma-separated values
- `json` - JSON array of objects
- `tsv` - Tab-separated values

## Roadmap

- Split into `mdsql` library crate and `mdsql-cli` binary crate
- FFI-first library for multi-language use (Python first)
- `UPDATE ... SET ... WHERE`
- Aggregate functions (`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`)
- `GROUP BY`
- Infer table names from preceding markdown headers

## See Also

[mdtsql](https://github.com/noborus/mdtsql) is a similar Go-based tool for querying markdown tables with SQL.

| Feature              | mdsql (this project)           | mdtsql                              |
| -------------------- | ------------------------------ | ----------------------------------- |
| Table references     | Index-based (`FROM 0`)         | File notation (`file.md::1`)        |
| File paths in SQL    | No (file is CLI argument)      | Yes (SQL references file paths)     |
| Named tables         | Planned                        | Yes (`--caption`)                   |
| Mutation operations  | INSERT, DELETE                 | INSERT, UPDATE                      |
| Default output       | Column-aligned terminal format | Markdown                            |
| Library availability | Planned (FFI, Python focus)    | Go package only                     |
| CLI style            | Subcommands (`query`, `tables`)| Flags                               |

Why mdsql exists: I wanted a Rust implementation with a simpler subcommand-based CLI, column-aligned terminal output by default, SQL that never embeds filesystem paths, and DELETE support for my note-taking workflows.
