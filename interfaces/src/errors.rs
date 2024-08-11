use std::fmt::{Debug, Display};

use crate::clickhouse::errors::ClickhouseError;

#[derive(Debug)]
pub enum DatabaseError {
    ClickhouseError(ClickhouseError)
}

impl Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{:?}", self)
    }
}

impl std::error::Error for DatabaseError {}

impl From<clickhouse::error::Error> for DatabaseError {
    fn from(value: clickhouse::error::Error) -> Self {
        Self::ClickhouseError(value.into())
    }
}

impl From<ClickhouseError> for DatabaseError {
    fn from(value: ClickhouseError) -> Self {
        Self::ClickhouseError(value)
    }
}
