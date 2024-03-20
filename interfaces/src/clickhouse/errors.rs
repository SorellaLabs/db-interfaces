use std::fmt::Debug;

use thiserror::Error;

use crate::errors::DatabaseError;

#[derive(Error, Debug, Clone)]
pub enum ClickhouseError {
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

impl DatabaseError for ClickhouseError where Self: Sized {}

impl From<std::io::Error> for ClickhouseError {
    fn from(value: std::io::Error) -> Self {
        ClickhouseError::SqlFileReadError(value.to_string())
    }
}
