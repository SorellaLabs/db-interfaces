use std::fmt::{Debug, Display};

#[derive(Debug)]
pub struct DatabaseError {
    pub error: String
}

impl Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.error)
    }
}

impl std::error::Error for DatabaseError {}
