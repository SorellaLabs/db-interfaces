#![feature(associated_type_defaults)]
#![feature(const_trait_impl)]

pub mod clickhouse;
pub mod errors;
pub mod params;
pub mod tables;

#[cfg(feature = "alloy-types")]
pub mod alloy_types;

//#[cfg(feature = "test-utils")]
pub mod test_utils;

use clickhouse::{dbms::NullDBMS, types::ClickhouseQuery};
pub use db_interfaces_macros::{remote_clickhouse_table, remote_clickhouse_table_value};
use errors::DatabaseError;
use params::BindParameters;
use tables::*;

pub trait Database<D = NullDBMS>: Sync + Send {
    type DBMS;

    fn insert_one<T: DatabaseTable<DBMS = D>>(&self, value: &T::DataType) -> impl std::future::Future<Output = Result<(), DatabaseError>> + Send;

    fn insert_many<T: DatabaseTable<DBMS = D>>(&self, values: &[T::DataType]) -> impl std::future::Future<Output = Result<(), DatabaseError>> + Send;

    fn query_one<Q: DatabaseQuery, P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P
    ) -> impl std::future::Future<Output = Result<Q, DatabaseError>> + Send;

    fn query_one_optional<Q: DatabaseQuery, P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P
    ) -> impl std::future::Future<Output = Result<Option<Q>, DatabaseError>> + Send;

    fn query_many<Q: DatabaseQuery, P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P
    ) -> impl std::future::Future<Output = Result<Vec<Q>, DatabaseError>> + Send;

    fn query_raw<Q: DatabaseQuery, P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P
    ) -> impl std::future::Future<Output = Result<Vec<u8>, DatabaseError>> + Send;

    fn execute_remote<P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P
    ) -> impl std::future::Future<Output = Result<(), DatabaseError>> + Send;
}

pub trait DatabaseQuery: ClickhouseQuery {}
