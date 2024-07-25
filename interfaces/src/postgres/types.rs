use ::postgres::{DbRow, InsertRow};
use tokio_postgres::row::Row;
use dyn_clone::DynClone;
use serde::{Deserialize, Serialize};

use crate::DatabaseQuery;

#[derive(Default, Debug, Clone, Serialize, Deserialize, Row)]
pub struct NoneType();

pub trait PostgresInsert: Serialize + InsertRow + Send + Sync + 'static + DynClone + Sized {}
impl<T> PostgresInsert for T where T: Serialize + InsertRow + Send + Sync + 'static + DynClone + Sized {}

pub trait PostgresQuery: for<'a> Deserialize<'a> + DbRow + Send + Sync {}
impl<T> PostgresQuery for T where T: for<'a> Deserialize<'a> + DbRow + Send + Sync {}
impl<T> DatabaseQuery for T where T: PostgresQuery {}
