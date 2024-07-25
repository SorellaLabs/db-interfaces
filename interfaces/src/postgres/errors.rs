use std::fmt::Debug;

use thiserror::Error;

use crate::errors::DatabaseError;

#[derive(Error, Debug, Clone)]
pub enum PostgresError {
    #[error("database connection error: {0}")]
    ConnectionError(String),
    #[error("error building the query: {0}")]
    QueryBuildingError(String),
    #[error("error inserting into the database: {0}")]
    InsertError(String),
    #[error("error querying from the database: {0}")]
    QueryError(String),
    #[error("error reading sql file: {0}")]
    SqlFileReadError(String)
}

impl From<std::io::Error> for PostgresError {
    fn from(value: std::io::Error) -> Self {
        PostgresError::SqlFileReadError(value.to_string())
    }
}

impl From<PostgresError> for DatabaseError {
    fn from(value: PostgresError) -> DatabaseError {
        DatabaseError { error: value.to_string() }
    }
}
