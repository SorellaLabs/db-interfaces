use std::marker::PhantomData;

use tokio_postgres::{row::SimpleQueryRow, *};
use eyre::Result;

use super::{dbms::PostgresDBMS, errors::PostgresError, types::PostgresQuery};
use crate::{errors::DatabaseError, params::BindParameters, Database, DatabaseTable};

#[derive(Clone)]
pub struct PostgresClient<D> {
    pub client:   Client,
    pub _phantom: PhantomData<D>
}

impl<D> PostgresClient<D>
where
    D: PostgresDBMS + 'static
{
    pub fn credentials(&self) -> Credentials {
        self.client.credentials()
    }

    pub fn query_bind(&self, query: &str) -> Query {
        self.client.query(query)
    }

    pub fn blank_query(&self, query: &str) -> Query {
        self.client.query(query)
    }
}

//#[async_trait::async_trait]
impl<D> Database for PostgresClient<D>
where
    D: PostgresDBMS
{
    type DBMS = D;

    async fn insert_one<T: DatabaseTable>(&self, value: &T::DataType) -> Result<(), DatabaseError> {
        let mut insert = self
            .client
            .insert(Self::DBMS::from_database_table_str(T::NAME).full_name())
            .map_err(|e| DatabaseError::from(PostgresError::InsertError(e.to_string())))?;

        insert
            .write(value)
            .await
            .map_err(|e| DatabaseError::from(PostgresError::InsertError(e.to_string())))?;

        insert
            .end()
            .await
            .map_err(|e| DatabaseError::from(PostgresError::InsertError(e.to_string())))?;

        Ok(())
    }

    async fn insert_many<T: DatabaseTable>(&self, values: &[T::DataType]) -> Result<(), DatabaseError> {
        let mut insert = self
            .client
            .insert(Self::DBMS::from_database_table_str(T::NAME).full_name())
            .map_err(|e| DatabaseError::from(PostgresError::InsertError(e.to_string())))?;

        for value in values {
            insert
                .write(value)
                .await
                .map_err(|e| DatabaseError::from(PostgresError::InsertError(e.to_string())))?;
        }

        insert
            .end()
            .await
            .map_err(|e| DatabaseError::from(PostgresError::InsertError(e.to_string())))?;

        Ok(())
    }

    async fn query_one<Q: PostgresQuery, P: BindParameters>(&self, query: impl AsRef<str> + Send, params: &P) -> Result<Q, DatabaseError> {
        let row = self.client.query_one(query, params).await?;

        let res = row
            .fetch_one::<Q>()
            .await
            .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string())))?;

        Ok(res)
    }

    async fn query_one_optional<Q: PostgresQuery, P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P
    ) -> Result<Option<Q>, DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        let res = query
            .fetch_optional::<Q>()
            .await
            .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string())))?;

        Ok(res)
    }

    async fn query_many<Q: PostgresQuery, P: BindParameters>(&self, query: impl AsRef<str> + Send, params: &P) -> Result<Vec<Q>, DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        let res = query
            .fetch_all::<Q>()
            .await
            .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string())))?;

        Ok(res)
    }

    async fn query_raw<Q: PostgresQuery, P: BindParameters>(&self, query: impl AsRef<str> + Send, params: &P) -> Result<Vec<u8>, DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));
        query
            .fetch_raw::<Q>()
            .await
            .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string())))
    }

    async fn execute_remote<P: BindParameters>(&self, query: impl AsRef<str> + Send, params: &P) -> Result<(), DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        query
            .execute()
            .await
            .map_err(|e| DatabaseError::from(PostgresError::QueryError(e.to_string())))?;

        Ok(())
    }
}
