use dyn_clone::DynClone;
use serde::{Deserialize, Serialize};
use sqlx::{Database, Encode, Type};

use crate::DatabaseQuery;

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct NoneType();

pub trait PostgresInsert: Send + Sync + 'static + DynClone + Sized {}
impl<T> PostgresInsert for T where T: Serialize + InsertRow + Send + Sync + 'static + DynClone + Sized {}

pub trait PostgresQuery {
    const QUERY: &'static str;
    type ResultType: PostgresResult;
    type ParamType;
}

pub trait PostgresParam<D> {}
impl<T, D> PostgresParam<D> for T where T: for<'args> Encode<'args, D> + Type<D>, D: Database {}

pub trait PostgresResult: Send + Sync {}
impl<T> PostgresResult for T where T: Send + Sync + ?Sized {}
//impl<T> DatabaseQuery for T where T: PostgresQuery 
