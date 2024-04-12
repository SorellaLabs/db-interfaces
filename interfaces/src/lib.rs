#![feature(associated_type_defaults)]

pub mod clickhouse;
pub mod errors;
pub mod params;

pub mod tables;

#[cfg(feature = "test-utils")]
pub mod test_utils;

use clickhouse::types::ClickhouseQuery;
pub use db_interfaces_macros::remote_clickhouse_table;
use errors::MapError;
use params::BindParameters;
use tables::*;

#[async_trait::async_trait]
pub trait Database: Sync + Send {
    type Error: MapError;
    type DBMS;

    async fn insert_one<T: DatabaseTable>(&self, value: &T::DataType) -> Result<(), Self::Error>;

    async fn insert_many<T: DatabaseTable>(
        &self,
        values: &[T::DataType],
    ) -> Result<(), Self::Error>;

    async fn query_one<Q: DatabaseQuery, P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P,
    ) -> Result<Q, Self::Error>;

    async fn query_one_optional<Q: DatabaseQuery, P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P,
    ) -> Result<Option<Q>, Self::Error>;

    async fn query_many<Q: DatabaseQuery, P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P,
    ) -> Result<Vec<Q>, Self::Error>;

    async fn query_raw<Q: DatabaseQuery, P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P,
    ) -> Result<Vec<u8>, Self::Error>;

    async fn execute_remote<P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P,
    ) -> Result<(), Self::Error>;
}

pub trait DatabaseQuery: ClickhouseQuery {}
