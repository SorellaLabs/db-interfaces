use std::fmt::Debug;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClickhouseError {
    #[error("clickhouse error: {0}")]
    ClickhouseNative(clickhouse::error::Error),
    #[error("error reading clickhouse sql file: {0}")]
    SqlFileReadError(String)
}

impl From<std::io::Error> for ClickhouseError {
    fn from(value: std::io::Error) -> Self {
        ClickhouseError::SqlFileReadError(value.to_string())
    }
}

impl From<clickhouse::error::Error> for ClickhouseError {
    fn from(value: clickhouse::error::Error) -> ClickhouseError {
        ClickhouseError::ClickhouseNative(value)
    }
}
