use ::clickhouse::{DbRow, InsertRow};
use clickhouse::Row;
use dyn_clone::DynClone;
use serde::{Deserialize, Serialize};

use crate::DatabaseQuery;

#[derive(Default, Debug, Clone, Serialize, Deserialize, Row)]
pub struct NoneType();

pub trait ClickhouseInsert: Serialize + InsertRow + Send + Sync + 'static + DynClone + Sized {}
impl<T> ClickhouseInsert for T where T: Serialize + InsertRow + Send + Sync + 'static + DynClone + Sized {}

pub trait ClickhouseQuery: for<'a> Deserialize<'a> + DbRow + Send + Sync {}
impl<T> ClickhouseQuery for T where T: for<'a> Deserialize<'a> + DbRow + Send + Sync {}
impl<T> DatabaseQuery for T where T: ClickhouseQuery {}
