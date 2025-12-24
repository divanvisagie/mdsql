use sqlparser::ast::{
    Expr, ObjectName, OrderBy, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::{MdsqlError, Result};
use crate::table::{QueryResult, Table};

/// Convert bare integer table references to quoted identifiers.
/// e.g., "FROM 1" -> "FROM \"1\"", "INTO 1" -> "INTO \"1\""
fn preprocess_table_refs(sql: &str) -> String {
    use regex::Regex;
    // Handle FROM and INTO clauses
    let re_from = Regex::new(r"(?i)\bFROM\s+(\d+)").unwrap();
    let re_into = Regex::new(r"(?i)\bINTO\s+(\d+)").unwrap();

    let sql = re_from
        .replace_all(sql, |caps: &regex::Captures| {
            format!("FROM \"{}\"", &caps[1])
        })
        .to_string();

    re_into
        .replace_all(&sql, |caps: &regex::Captures| {
            format!("INTO \"{}\"", &caps[1])
        })
        .to_string()
}

pub fn execute(sql: &str, tables: &[Table]) -> Result<QueryResult> {
    // Preprocess: convert bare integer table refs (FROM 1) to quoted (FROM "1")
    let sql = preprocess_table_refs(sql);

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, &sql)
        .map_err(|e| MdsqlError::SqlParse(e.to_string()))?;

    if statements.is_empty() {
        return Err(MdsqlError::SqlParse("No SQL statement found".to_string()));
    }

    let statement = &statements[0];

    match statement {
        Statement::Query(query) => execute_query(query, tables),
        _ => Err(MdsqlError::Query("Only SELECT queries are supported".to_string())),
    }
}

/// Result of an INSERT operation
pub struct InsertResult {
    pub table_index: usize,
    pub new_row: Vec<String>,
}

/// Execute an INSERT statement, returning the table index and new row
pub fn execute_insert(sql: &str, tables: &[Table]) -> Result<InsertResult> {
    let sql = preprocess_table_refs(sql);

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, &sql)
        .map_err(|e| MdsqlError::SqlParse(e.to_string()))?;

    if statements.is_empty() {
        return Err(MdsqlError::SqlParse("No SQL statement found".to_string()));
    }

    let statement = &statements[0];

    match statement {
        Statement::Insert(insert) => {
            let table_index = get_table_index_from_name(&insert.table_name)?;
            let table = tables
                .get(table_index)
                .ok_or(MdsqlError::TableNotFound(table_index))?;

            // Get column order from INSERT statement, or use table columns
            let insert_columns: Vec<String> = if insert.columns.is_empty() {
                table.columns.clone()
            } else {
                insert.columns.iter().map(|c| c.value.clone()).collect()
            };

            // Validate columns exist
            for col in &insert_columns {
                if !table.columns.contains(col) {
                    return Err(MdsqlError::ColumnNotFound(col.clone()));
                }
            }

            // Extract values from the INSERT statement
            let values = extract_insert_values(&insert.source)?;

            if values.len() != insert_columns.len() {
                return Err(MdsqlError::Query(format!(
                    "Column count ({}) doesn't match value count ({})",
                    insert_columns.len(),
                    values.len()
                )));
            }

            // Build the row in table column order
            let mut new_row = vec![String::new(); table.columns.len()];
            for (col, val) in insert_columns.iter().zip(values.iter()) {
                if let Some(idx) = table.columns.iter().position(|c| c == col) {
                    new_row[idx] = val.clone();
                }
            }

            Ok(InsertResult {
                table_index,
                new_row,
            })
        }
        _ => Err(MdsqlError::Query("Expected INSERT statement".to_string())),
    }
}

/// Result of a DELETE operation
pub struct DeleteResult {
    pub table_index: usize,
    pub rows_deleted: usize,
    pub remaining_rows: Vec<Vec<String>>,
}

/// Execute a DELETE statement, returning the table index and remaining rows
pub fn execute_delete(sql: &str, tables: &[Table]) -> Result<DeleteResult> {
    let sql = preprocess_table_refs(sql);

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, &sql)
        .map_err(|e| MdsqlError::SqlParse(e.to_string()))?;

    if statements.is_empty() {
        return Err(MdsqlError::SqlParse("No SQL statement found".to_string()));
    }

    let statement = &statements[0];

    match statement {
        Statement::Delete(delete) => {
            use sqlparser::ast::FromTable;
            let table_index = match &delete.from {
                FromTable::WithFromKeyword(tables) => {
                    if let Some(from) = tables.first() {
                        match &from.relation {
                            TableFactor::Table { name, .. } => get_table_index_from_name(name)?,
                            _ => return Err(MdsqlError::Query("Unsupported FROM clause".to_string())),
                        }
                    } else {
                        return Err(MdsqlError::Query("DELETE requires FROM clause".to_string()));
                    }
                }
                FromTable::WithoutKeyword(tables) => {
                    if let Some(from) = tables.first() {
                        match &from.relation {
                            TableFactor::Table { name, .. } => get_table_index_from_name(name)?,
                            _ => return Err(MdsqlError::Query("Unsupported FROM clause".to_string())),
                        }
                    } else {
                        return Err(MdsqlError::Query("DELETE requires FROM clause".to_string()));
                    }
                }
            };

            let table = tables
                .get(table_index)
                .ok_or(MdsqlError::TableNotFound(table_index))?;

            // Find rows that DON'T match the WHERE clause (these are kept)
            let mut remaining_rows = Vec::new();
            let mut rows_deleted = 0;

            for row in &table.rows {
                let should_delete = if let Some(selection) = &delete.selection {
                    evaluate_where(selection, &table.columns, row)?
                } else {
                    // No WHERE clause means delete all rows
                    true
                };

                if should_delete {
                    rows_deleted += 1;
                } else {
                    remaining_rows.push(row.clone());
                }
            }

            Ok(DeleteResult {
                table_index,
                rows_deleted,
                remaining_rows,
            })
        }
        _ => Err(MdsqlError::Query("Expected DELETE statement".to_string())),
    }
}

fn get_table_index_from_name(name: &ObjectName) -> Result<usize> {
    let table_name = name.to_string();
    let table_name = table_name.trim_matches('"');
    table_name
        .parse::<usize>()
        .map_err(|_| MdsqlError::Query(format!(
            "Table identifier must be a number (0, 1, 2...), got: {}",
            table_name
        )))
}

fn extract_insert_values(source: &Option<Box<Query>>) -> Result<Vec<String>> {
    let source = source
        .as_ref()
        .ok_or_else(|| MdsqlError::Query("INSERT requires VALUES".to_string()))?;

    let SetExpr::Values(values) = source.body.as_ref() else {
        return Err(MdsqlError::Query("INSERT requires VALUES clause".to_string()));
    };

    if values.rows.is_empty() {
        return Err(MdsqlError::Query("No values provided".to_string()));
    }

    // Take first row of values
    let row = &values.rows[0];
    let mut result = Vec::new();

    for expr in row {
        let val = match expr {
            Expr::Value(v) => v.to_string().trim_matches('\'').to_string(),
            Expr::Identifier(id) => id.value.clone(),
            _ => return Err(MdsqlError::Query(format!("Unsupported value expression: {:?}", expr))),
        };
        result.push(val);
    }

    Ok(result)
}

fn execute_query(query: &Query, tables: &[Table]) -> Result<QueryResult> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(MdsqlError::Query("Only simple SELECT queries are supported".to_string()));
    };

    // Get the table index from FROM clause (0-based)
    let table_index = get_table_index(select)?;
    let table = tables
        .get(table_index)
        .ok_or(MdsqlError::TableNotFound(table_index))?;

    // Get selected columns
    let selected_columns = get_selected_columns(select, table)?;

    // Filter rows based on WHERE clause
    let filtered_rows = filter_rows(table, select.selection.as_ref())?;

    // Project selected columns
    let projected_rows = project_columns(table, &filtered_rows, &selected_columns);

    // Apply ORDER BY
    let ordered_rows = apply_order_by(
        &selected_columns,
        projected_rows,
        query.order_by.as_ref(),
    )?;

    // Apply LIMIT
    let limited_rows = apply_limit(ordered_rows, &query.limit);

    Ok(QueryResult::new(selected_columns, limited_rows))
}

fn get_table_index(select: &Select) -> Result<usize> {
    if select.from.is_empty() {
        return Err(MdsqlError::Query("FROM clause is required".to_string()));
    }

    let from = &select.from[0];
    match &from.relation {
        TableFactor::Table { name, .. } => {
            // Strip quotes from table name (e.g., "1" -> 1)
            let table_name = name.to_string();
            let table_name = table_name.trim_matches('"');
            table_name
                .parse::<usize>()
                .map_err(|_| MdsqlError::Query(format!(
                    "Table identifier must be a number (1, 2, 3...), got: {}",
                    table_name
                )))
        }
        _ => Err(MdsqlError::Query("Unsupported FROM clause".to_string())),
    }
}

fn get_selected_columns(select: &Select, table: &Table) -> Result<Vec<String>> {
    let mut columns = Vec::new();

    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                columns.extend(table.columns.clone());
            }
            SelectItem::UnnamedExpr(expr) => {
                if let Expr::Identifier(ident) = expr {
                    let col_name = ident.value.clone();
                    if !table.columns.contains(&col_name) {
                        return Err(MdsqlError::ColumnNotFound(col_name));
                    }
                    columns.push(col_name);
                } else {
                    return Err(MdsqlError::Query(format!(
                        "Unsupported expression in SELECT: {:?}",
                        expr
                    )));
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Expr::Identifier(_) = expr {
                    columns.push(alias.value.clone());
                } else {
                    return Err(MdsqlError::Query(
                        "Unsupported aliased expression".to_string(),
                    ));
                }
            }
            _ => {
                return Err(MdsqlError::Query(format!(
                    "Unsupported SELECT item: {:?}",
                    item
                )));
            }
        }
    }

    Ok(columns)
}

fn filter_rows<'a>(table: &'a Table, where_clause: Option<&Expr>) -> Result<Vec<&'a Vec<String>>> {
    let Some(expr) = where_clause else {
        return Ok(table.rows.iter().collect());
    };

    let mut result = Vec::new();

    for row in &table.rows {
        if evaluate_where(expr, &table.columns, row)? {
            result.push(row);
        }
    }

    Ok(result)
}

fn evaluate_where(expr: &Expr, columns: &[String], row: &[String]) -> Result<bool> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            use sqlparser::ast::BinaryOperator;

            let left_val = get_expr_value(left, columns, row)?;
            let right_val = get_expr_value(right, columns, row)?;

            let result = match op {
                BinaryOperator::Eq => left_val == right_val,
                BinaryOperator::NotEq => left_val != right_val,
                BinaryOperator::Lt => compare_values(&left_val, &right_val) == std::cmp::Ordering::Less,
                BinaryOperator::LtEq => {
                    matches!(compare_values(&left_val, &right_val), std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                }
                BinaryOperator::Gt => compare_values(&left_val, &right_val) == std::cmp::Ordering::Greater,
                BinaryOperator::GtEq => {
                    matches!(compare_values(&left_val, &right_val), std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                }
                BinaryOperator::And => {
                    evaluate_where(left, columns, row)? && evaluate_where(right, columns, row)?
                }
                BinaryOperator::Or => {
                    evaluate_where(left, columns, row)? || evaluate_where(right, columns, row)?
                }
                _ => return Err(MdsqlError::Query(format!("Unsupported operator: {:?}", op))),
            };

            Ok(result)
        }
        _ => Err(MdsqlError::Query(format!(
            "Unsupported WHERE expression: {:?}",
            expr
        ))),
    }
}

fn get_expr_value(expr: &Expr, columns: &[String], row: &[String]) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => {
            let col_idx = columns
                .iter()
                .position(|c| c == &ident.value)
                .ok_or_else(|| MdsqlError::ColumnNotFound(ident.value.clone()))?;
            Ok(row[col_idx].clone())
        }
        Expr::Value(v) => Ok(v.to_string().trim_matches('\'').to_string()),
        _ => Err(MdsqlError::Query(format!(
            "Unsupported expression: {:?}",
            expr
        ))),
    }
}

fn compare_values(a: &str, b: &str) -> std::cmp::Ordering {
    // Try numeric comparison first
    if let (Ok(a_num), Ok(b_num)) = (a.parse::<f64>(), b.parse::<f64>()) {
        a_num.partial_cmp(&b_num).unwrap_or(std::cmp::Ordering::Equal)
    } else {
        a.cmp(b)
    }
}

fn project_columns(
    table: &Table,
    rows: &[&Vec<String>],
    selected_columns: &[String],
) -> Vec<Vec<String>> {
    let col_indices: Vec<usize> = selected_columns
        .iter()
        .filter_map(|col| table.columns.iter().position(|c| c == col))
        .collect();

    rows.iter()
        .map(|row| col_indices.iter().map(|&i| row[i].clone()).collect())
        .collect()
}

fn apply_order_by(
    columns: &[String],
    mut rows: Vec<Vec<String>>,
    order_by: Option<&OrderBy>,
) -> Result<Vec<Vec<String>>> {
    let Some(order_by) = order_by else {
        return Ok(rows);
    };

    if order_by.exprs.is_empty() {
        return Ok(rows);
    }

    let order_expr = &order_by.exprs[0];
    let Expr::Identifier(ident) = &order_expr.expr else {
        return Err(MdsqlError::Query(
            "ORDER BY only supports column names".to_string(),
        ));
    };

    let col_idx = columns
        .iter()
        .position(|c| c == &ident.value)
        .ok_or_else(|| MdsqlError::ColumnNotFound(ident.value.clone()))?;

    let asc = order_expr.asc.unwrap_or(true);

    rows.sort_by(|a, b| {
        let cmp = compare_values(&a[col_idx], &b[col_idx]);
        if asc { cmp } else { cmp.reverse() }
    });

    Ok(rows)
}

fn apply_limit(rows: Vec<Vec<String>>, limit: &Option<Expr>) -> Vec<Vec<String>> {
    let Some(limit_expr) = limit else {
        return rows;
    };

    if let Expr::Value(sqlparser::ast::Value::Number(n, _)) = limit_expr {
        if let Ok(limit) = n.parse::<usize>() {
            return rows.into_iter().take(limit).collect();
        }
    }

    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_table() -> Table {
        Table::new(
            vec!["name".to_string(), "age".to_string(), "city".to_string()],
            vec![
                vec!["Alice".to_string(), "30".to_string(), "NYC".to_string()],
                vec!["Bob".to_string(), "25".to_string(), "LA".to_string()],
                vec!["Charlie".to_string(), "35".to_string(), "NYC".to_string()],
            ],
        )
    }

    #[test]
    fn test_select_all() {
        let tables = vec![sample_table()];
        let result = execute("SELECT * FROM 0", &tables).unwrap();
        assert_eq!(result.columns, vec!["name", "age", "city"]);
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn test_select_columns() {
        let tables = vec![sample_table()];
        let result = execute("SELECT name, age FROM 0", &tables).unwrap();
        assert_eq!(result.columns, vec!["name", "age"]);
        assert_eq!(result.rows[0], vec!["Alice", "30"]);
    }

    #[test]
    fn test_where_clause() {
        let tables = vec![sample_table()];
        let result = execute("SELECT * FROM 0 WHERE city = 'NYC'", &tables).unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_order_by() {
        let tables = vec![sample_table()];
        let result = execute("SELECT * FROM 0 ORDER BY age DESC", &tables).unwrap();
        assert_eq!(result.rows[0][0], "Charlie");
        assert_eq!(result.rows[1][0], "Alice");
        assert_eq!(result.rows[2][0], "Bob");
    }

    #[test]
    fn test_limit() {
        let tables = vec![sample_table()];
        let result = execute("SELECT * FROM 0 LIMIT 2", &tables).unwrap();
        assert_eq!(result.rows.len(), 2);
    }
}
