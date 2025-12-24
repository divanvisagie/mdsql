use comrak::nodes::NodeValue;
use comrak::{parse_document, Arena, Options};

use crate::table::Table;

/// A table with its source position in the markdown
pub struct LocatedTable {
    pub table: Table,
    pub start_line: usize,
    pub end_line: usize,
}

pub fn extract_tables(content: &str) -> Vec<Table> {
    extract_tables_with_positions(content)
        .into_iter()
        .map(|lt| lt.table)
        .collect()
}

pub fn extract_tables_with_positions(content: &str) -> Vec<LocatedTable> {
    let arena = Arena::new();
    let mut options = Options::default();
    options.extension.table = true;

    let root = parse_document(&arena, content, &options);
    let mut tables = Vec::new();

    for node in root.descendants() {
        if let NodeValue::Table(_) = node.data.borrow().value {
            let data = node.data.borrow();
            let start_line = data.sourcepos.start.line;
            let end_line = data.sourcepos.end.line;

            let table = parse_table_node(node);
            tables.push(LocatedTable {
                table,
                start_line,
                end_line,
            });
        }
    }

    tables
}

/// Rewrite markdown content with a modified table
pub fn rewrite_table(content: &str, table_index: usize, new_table: &Table) -> Option<String> {
    let located_tables = extract_tables_with_positions(content);
    let located = located_tables.get(table_index)?;

    let lines: Vec<&str> = content.lines().collect();

    let mut result = String::new();

    // Add lines before the table
    for line in lines.iter().take(located.start_line - 1) {
        result.push_str(line);
        result.push('\n');
    }

    // Add the new table
    result.push_str(&table_to_markdown(new_table));

    // Add lines after the table
    for line in lines.iter().skip(located.end_line) {
        result.push_str(line);
        result.push('\n');
    }

    Some(result)
}

/// Convert a table back to markdown format
pub fn table_to_markdown(table: &Table) -> String {
    if table.columns.is_empty() {
        return String::new();
    }

    // Calculate column widths
    let mut widths: Vec<usize> = table.columns.iter().map(|c| c.len()).collect();
    for row in &table.rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    let mut output = String::new();

    // Header row
    output.push('|');
    for (i, col) in table.columns.iter().enumerate() {
        output.push_str(&format!(" {:width$} |", col, width = widths[i]));
    }
    output.push('\n');

    // Separator row
    output.push('|');
    for width in &widths {
        output.push_str(&format!(" {} |", "-".repeat(*width)));
    }
    output.push('\n');

    // Data rows
    for row in &table.rows {
        output.push('|');
        for (i, cell) in row.iter().enumerate() {
            let width = widths.get(i).copied().unwrap_or(cell.len());
            output.push_str(&format!(" {:width$} |", cell, width = width));
        }
        output.push('\n');
    }

    output
}

fn parse_table_node<'a>(node: &'a comrak::nodes::AstNode<'a>) -> Table {
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    let mut is_header = true;

    for child in node.children() {
        if let NodeValue::TableRow(header) = child.data.borrow().value {
            let row = parse_row(child);

            if header || is_header {
                columns = row;
                is_header = false;
            } else {
                rows.push(row);
            }
        }
    }

    Table::new(columns, rows)
}

fn parse_row<'a>(row_node: &'a comrak::nodes::AstNode<'a>) -> Vec<String> {
    let mut cells = Vec::new();

    for cell in row_node.children() {
        if let NodeValue::TableCell = cell.data.borrow().value {
            let text = extract_text(cell);
            cells.push(text.trim().to_string());
        }
    }

    cells
}

fn extract_text<'a>(node: &'a comrak::nodes::AstNode<'a>) -> String {
    let mut text = String::new();

    for child in node.descendants() {
        if let NodeValue::Text(ref t) = child.data.borrow().value {
            text.push_str(t);
        }
    }

    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_simple_table() {
        let md = r#"
| Name | Age |
|------|-----|
| Alice | 30 |
| Bob | 25 |
"#;
        let tables = extract_tables(md);
        assert_eq!(tables.len(), 1);

        let table = &tables[0];
        assert_eq!(table.columns, vec!["Name", "Age"]);
        assert_eq!(table.rows.len(), 2);
        assert_eq!(table.rows[0], vec!["Alice", "30"]);
        assert_eq!(table.rows[1], vec!["Bob", "25"]);
    }

    #[test]
    fn test_extract_multiple_tables() {
        let md = r#"
# First Table
| A | B |
|---|---|
| 1 | 2 |

# Second Table
| X | Y | Z |
|---|---|---|
| a | b | c |
"#;
        let tables = extract_tables(md);
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].columns, vec!["A", "B"]);
        assert_eq!(tables[1].columns, vec!["X", "Y", "Z"]);
    }
}
