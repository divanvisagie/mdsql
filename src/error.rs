use thiserror::Error;

pub type Result<T> = std::result::Result<T, MdsqlError>;

#[derive(Error, Debug)]
pub enum MdsqlError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("SQL parse error: {0}")]
    SqlParse(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Table not found: {0}")]
    TableNotFound(usize),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
}
