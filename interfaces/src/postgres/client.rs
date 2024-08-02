use std::marker::PhantomData;

use alloy_primitives::bytes::BytesMut;
use eyre::Result;
use sqlx::{query, query_as_unchecked, query_builder, query_with, Encode, Pool, Type};

use super::{dbms::PostgresDBMS, errors::PostgresError, types::{PostgresParam, PostgresQuery, PostgresResult}};
use crate::{errors::DatabaseError, params::BindParameters, Database, DatabaseTable};

use futures::TryStreamExt;

#[derive(Clone)]
pub struct PostgresClient<D: PostgresDBMS> {
    pub pool:     Pool<D>,
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
    async fn insert_one<T: DatabaseTable>(&self, value: &T::DataType) -> Result<(), DatabaseError> {
        Ok(())
    }

    async fn insert_many<T: DatabaseTable>(&self, values: &[T::DataType]) -> Result<(), DatabaseError> {
        Ok(())
    }

    async fn query_one<Q: PostgresQuery>(&self, params: &Q::ParamType) -> Result<(), DatabaseError> {
        // let mut query = sqlx::query(Q::QUERY);
        // query.bind(value)
        // for param in params {
        //     query = query.bind(param);
        // }
        
        // let res = query.execute(self.pool).await?;

        Ok(())
    }

    // async fn query_one_optional<R: PostgresResult>(
    //     &self,
    //     query: &impl PostgresQuery,
    //     params: &[&(dyn ToSql + Sync)]
    // ) -> Result<Option<R>, DatabaseError> {
    //     let row = self
    //         .client
    //         .query_opt(query, params)
    //         .await
    //         .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string())))?;

    //     row.map(|r| r.try_get::<usize, R>(0))
    //         .transpose()
    //         .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string())))
    // }

    // async fn query_many<R: PostgresResult>(&self, query: &impl PostgresQuery, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<R>, DatabaseError> {
    //     let rows = self
    //         .client
    //         .query(query, params)
    //         .await
    //         .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string())))?;

    //     rows
    //         .iter()
    //         .map(|r| r
    //             .try_get::<usize, R>(0)
    //             .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string()))))
    //         .collect()
    // }

    // async fn query_raw<Q: PostgresResult>(&self, query: &impl PostgresQuery, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<u8>, DatabaseError> {
    //     let row_stream = self
    //         .client
    //         .query_raw(query, params.iter())
    //         .await
    //         .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string())))?;
    
    //     let mut buffer = BytesMut::new();
    //     row_stream
    //         .try_for_each(|row| {
    //             buffer.extend_from_slice(&row);
    //             futures::future::ready(Ok(()))
    //         })
    //         .await
    //         .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string())))?;
    
    //     Ok(buffer.freeze().to_vec())
    // }
    // async fn execute_remote<P: BindParameters>(&self, query: impl AsRef<str> + Send, params: &P) -> Result<(), DatabaseError> {
    //     Ok(())
    // }

    pub async fn execute_remote<P: BindParameters>(&self, query: impl AsRef<str> + Send, params: &P) -> Result<(), DatabaseError> {
        // let query = params.bind_query(self.client.query(query.as_ref()));

        // query
        //     .execute()
        //     .await
        //     .map_err(|e| DatabaseError::from(ClickhouseError::QueryError(e.to_string())))?;

        Ok(())
    }

}
