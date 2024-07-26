use tokio_postgres::{types::FromSql, ToStatement};
use dyn_clone::DynClone;
use serde::{Deserialize, Serialize};

use crate::DatabaseQuery;

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct NoneType();

pub trait PostgresInsert: Send + Sync + 'static + DynClone + Sized {}
//impl<T> PostgresInsert for T where T: Serialize + InsertRow + Send + Sync + 'static + DynClone + Sized {}

pub trait PostgresQuery: ToStatement {}
impl<T> PostgresQuery for T where T: ToStatement {}

pub trait PostgresResult: for<'a> FromSql<'a> + Send + Sync {}
impl<T> PostgresResult for T where T: for<'a> FromSql<'a> + Send + Sync + ?Sized {}
//impl<T> DatabaseQuery for T where T: PostgresQuery 
