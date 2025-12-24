use serde::Serialize;

use crate::error::Result;

#[derive(Debug, Clone, Default)]
pub struct Table {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl Table {
    pub fn new(columns: Vec<String>, rows: Vec<Vec<String>>) -> Self {
        Self { columns, rows }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl QueryResult {
    pub fn new(columns: Vec<String>, rows: Vec<Vec<String>>) -> Self {
        Self { columns, rows }
    }

    pub fn to_markdown(&self) -> String {
        if self.columns.is_empty() {
            return String::new();
        }

        let mut output = String::new();

        // Header row
        output.push('|');
        for col in &self.columns {
            output.push_str(&format!(" {} |", col));
        }
        output.push('\n');

        // Separator row
        output.push('|');
        for _ in &self.columns {
            output.push_str(" --- |");
        }
        output.push('\n');

        // Data rows
        for row in &self.rows {
            output.push('|');
            for cell in row {
                output.push_str(&format!(" {} |", cell));
            }
            output.push('\n');
        }

        output
    }

    pub fn to_csv(&self) -> String {
        let mut output = String::new();

        // Header
        output.push_str(&self.columns.join(","));
        output.push('\n');

        // Rows
        for row in &self.rows {
            output.push_str(&row.join(","));
            output.push('\n');
        }

        output
    }

    pub fn to_json(&self) -> Result<String> {
        let records: Vec<_> = self
            .rows
            .iter()
            .map(|row| {
                self.columns
                    .iter()
                    .zip(row.iter())
                    .collect::<std::collections::HashMap<_, _>>()
            })
            .collect();

        Ok(serde_json::to_string_pretty(&records)?)
    }

    /// Column-aligned output like df -h (default, unix-friendly)
    pub fn to_columns(&self) -> String {
        if self.columns.is_empty() {
            return String::new();
        }

        // Calculate column widths
        let mut widths: Vec<usize> = self.columns.iter().map(|c| c.len()).collect();
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                if i < widths.len() {
                    widths[i] = widths[i].max(cell.len());
                }
            }
        }

        let mut output = String::new();

        // Header
        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                output.push_str("  ");
            }
            output.push_str(&format!("{:width$}", col, width = widths[i]));
        }
        output.push('\n');

        // Rows
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                if i > 0 {
                    output.push_str("  ");
                }
                if i < widths.len() {
                    output.push_str(&format!("{:width$}", cell, width = widths[i]));
                }
            }
            output.push('\n');
        }

        output
    }

    pub fn to_tsv(&self) -> String {
        let mut output = String::new();

        // Header
        output.push_str(&self.columns.join("\t"));
        output.push('\n');

        // Rows
        for row in &self.rows {
            output.push_str(&row.join("\t"));
            output.push('\n');
        }

        output
    }
}
