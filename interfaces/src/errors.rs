use std::{error::Error, fmt::Debug};

pub trait DatabaseError: Error + Debug + Sized + Send + Sync {}
