use std::fmt::{Debug, Display};

#[derive(Debug)]
pub struct DatabaseError {
    pub error: Box<dyn std::error::Error>,
}

impl Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.error)
    }
}

impl std::error::Error for DatabaseError {}

pub trait MapError: Into<DatabaseError> + Clone {
    fn to_db_err(&self) -> DatabaseError {
        self.clone().into()
    }
}
