use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod error;
mod markdown;
mod query;
mod table;

use error::Result;

#[derive(Parser)]
#[command(name = "mdsql")]
#[command(about = "SQL queries for markdown tables")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// List tables in a markdown file
    Tables {
        /// Path to the markdown file
        file: PathBuf,
    },
    /// Execute a SQL query on markdown tables
    Query {
        /// SQL query (e.g., "SELECT * FROM 0")
        sql: String,
        /// Path to the markdown file
        file: PathBuf,
        /// Output format
        #[arg(short, long, default_value = "columns")]
        format: OutputFormat,
    },
    /// Insert a row into a table
    Insert {
        /// SQL INSERT statement (e.g., "INSERT INTO 0 (col1, col2) VALUES ('a', 'b')")
        sql: String,
        /// Path to the markdown file
        file: PathBuf,
    },
    /// Delete rows from a table
    Delete {
        /// SQL DELETE statement (e.g., "DELETE FROM 0 WHERE name = 'Alice'")
        sql: String,
        /// Path to the markdown file
        file: PathBuf,
    },
    /// Update rows in a table
    Update {
        /// SQL UPDATE statement (e.g., "UPDATE 0 SET name = 'Alice' WHERE id = '1'")
        sql: String,
        /// Path to the markdown file
        file: PathBuf,
    },
}

#[derive(Clone, Default, clap::ValueEnum)]
enum OutputFormat {
    /// Column-aligned like df -h (default)
    #[default]
    Columns,
    /// Markdown table
    Markdown,
    /// Comma-separated values
    Csv,
    /// JSON array of objects
    Json,
    /// Tab-separated values
    Tsv,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Tables { file } => {
            let content = std::fs::read_to_string(&file)?;
            let tables = markdown::extract_tables(&content);

            for (i, table) in tables.iter().enumerate() {
                println!(
                    "Table {}: {} columns, {} rows",
                    i,
                    table.columns.len(),
                    table.rows.len()
                );
                if !table.columns.is_empty() {
                    println!("  Columns: {}", table.columns.join(", "));
                }
            }
        }
        Commands::Query { sql, file, format } => {
            let content = std::fs::read_to_string(&file)?;
            let tables = markdown::extract_tables(&content);
            let result = query::execute(&sql, &tables)?;

            match format {
                OutputFormat::Columns => print!("{}", result.to_columns()),
                OutputFormat::Markdown => print!("{}", result.to_markdown()),
                OutputFormat::Csv => print!("{}", result.to_csv()),
                OutputFormat::Json => println!("{}", result.to_json()?),
                OutputFormat::Tsv => print!("{}", result.to_tsv()),
            }
        }
        Commands::Insert { sql, file } => {
            let content = std::fs::read_to_string(&file)?;
            let tables = markdown::extract_tables(&content);

            let insert_result = query::execute_insert(&sql, &tables)?;

            // Create modified table with new row
            let mut modified_table = tables[insert_result.table_index].clone();
            modified_table.rows.push(insert_result.new_row);

            // Rewrite the file
            let new_content = markdown::rewrite_table(&content, insert_result.table_index, &modified_table)
                .ok_or_else(|| error::MdsqlError::Query("Failed to rewrite table".to_string()))?;

            std::fs::write(&file, new_content)?;
            eprintln!("Inserted 1 row into table {}", insert_result.table_index);
        }
        Commands::Delete { sql, file } => {
            let content = std::fs::read_to_string(&file)?;
            let tables = markdown::extract_tables(&content);

            let delete_result = query::execute_delete(&sql, &tables)?;

            // Create modified table with remaining rows
            let mut modified_table = tables[delete_result.table_index].clone();
            modified_table.rows = delete_result.remaining_rows;

            // Rewrite the file
            let new_content = markdown::rewrite_table(&content, delete_result.table_index, &modified_table)
                .ok_or_else(|| error::MdsqlError::Query("Failed to rewrite table".to_string()))?;

            std::fs::write(&file, new_content)?;
            eprintln!("Deleted {} row(s) from table {}", delete_result.rows_deleted, delete_result.table_index);
        }
        Commands::Update { sql, file } => {
            let content = std::fs::read_to_string(&file)?;
            let tables = markdown::extract_tables(&content);

            let update_result = query::execute_update(&sql, &tables)?;

            // Create modified table with updated rows
            let mut modified_table = tables[update_result.table_index].clone();
            modified_table.rows = update_result.updated_rows;

            // Rewrite the file
            let new_content = markdown::rewrite_table(&content, update_result.table_index, &modified_table)
                .ok_or_else(|| error::MdsqlError::Query("Failed to rewrite table".to_string()))?;

            std::fs::write(&file, new_content)?;
            eprintln!("Updated {} row(s) in table {}", update_result.rows_updated, update_result.table_index);
        }
    }

    Ok(())
}
