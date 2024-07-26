use std::marker::PhantomData;

use tokio_postgres::*;
use eyre::Result;
use types::{FromSql, ToSql};

use super::{dbms::PostgresDBMS, errors::PostgresError, types::{PostgresQuery, PostgresResult}};
use crate::{errors::DatabaseError, params::BindParameters, Database, DatabaseTable, PostgresDatabaseTable};

pub struct PostgresClient<D> {
    pub client:   Client,
    pub _phantom: PhantomData<D>,
}

impl<D> PostgresClient<D>
where
    D: PostgresDBMS + 'static
{
    // pub fn credentials(&self) -> Credentials {
    //     self.client.credentials()
    // }

    // pub fn query_bind(&self, query: &str) -> Query {
    //     self.client.query(query)
    // }

    // pub fn blank_query(&self, query: &str) -> Query {
    //     self.client.query(query)
    // }
}

//#[async_trait::async_trait]
impl<D> PostgresClient<D>
where
    D: PostgresDBMS
{    
    async fn insert_one<T: PostgresDatabaseTable>(&self, value: &T::DataType) -> Result<(), DatabaseError> {
        Ok(())
    }

    async fn insert_many<T: PostgresDatabaseTable>(&self, values: &[T::DataType]) -> Result<(), DatabaseError> {
        Ok(())
    }

    async fn query_one<R: PostgresResult>(&self, query: &impl PostgresQuery, params: &[&(dyn ToSql + Sync)]) -> Result<R, DatabaseError> {
        let row = self
            .client
            .query_one(query, params).await
            .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string())))?;

        let res = row
            .try_get::<usize, R>(0)
            .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string())))?;

        Ok(res)
    }

    async fn query_one_optional<R: PostgresResult>(
        &self,
        query: &impl PostgresQuery,
        params: &[&(dyn ToSql + Sync)]
    ) -> Result<Option<R>, DatabaseError> {
        let row = self
            .client
            .query_opt(query, params)
            .await
            .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string())))?;

        if let Some(row) = row {
            let res = row
                .try_get::<usize, R>(0)
                .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string())))?;
            Ok(Some(res))
        } else {
            Ok(None)
        }
    }

    async fn query_many<R: PostgresResult>(&self, query: &impl PostgresQuery, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<R>, DatabaseError> {
        let rows = self
            .client
            .query(query, params)
            .await
            .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string())))?;

        rows
            .iter()
            .map(|r| r
                .try_get::<usize, R>(0)
                .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string()))))
            .collect()
    }

    async fn query_raw<Q: PostgresResult>(&self, query: &impl PostgresQuery, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<u8>, DatabaseError> {
        let rows = self
            .client
            .query_raw(query, params.iter())
            .await
            .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string())))?;

        rows
            .fetch_raw::<Q>()
            .await
            .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string())))
    }

    async fn execute_remote<P: BindParameters>(&self, query: impl AsRef<str> + Send, params: &P) -> Result<(), DatabaseError> {
        Ok(())
    }
}
