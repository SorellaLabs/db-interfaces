use std::marker::PhantomData;

use clickhouse::{query::Query, *};
use eyre::Result;

use super::{dbms::ClickhouseDBMS, types::ClickhouseQuery};
use crate::{errors::DatabaseError, params::BindParameters, Database, DatabaseTable};

#[derive(Clone)]
pub struct ClickhouseClient<D> {
    pub client:   Client,
    pub _phantom: PhantomData<D>
}

impl<D> ClickhouseClient<D>
where
    D: ClickhouseDBMS + 'static
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
impl<D> Database for ClickhouseClient<D>
where
    D: ClickhouseDBMS
{
    type DBMS = D;

    async fn insert_one<T: DatabaseTable>(&self, value: &T::DataType) -> Result<(), DatabaseError> {
        let mut insert = self
            .client
            .insert(Self::DBMS::from_database_table_str(T::NAME).full_name())?;

        insert.write(value).await?;

        insert.end().await?;

        Ok(())
    }

    async fn insert_many<T: DatabaseTable>(&self, values: &[T::DataType]) -> Result<(), DatabaseError> {
        let mut insert = self
            .client
            .insert(Self::DBMS::from_database_table_str(T::NAME).full_name())?;

        for value in values {
            insert.write(value).await?;
        }

        insert.end().await?;

        Ok(())
    }

    async fn query_one<Q: ClickhouseQuery, P: BindParameters>(&self, query: impl AsRef<str> + Send, params: &P) -> Result<Q, DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        let res = query.fetch_one::<Q>().await?;

        Ok(res)
    }

    async fn query_one_optional<Q: ClickhouseQuery, P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P
    ) -> Result<Option<Q>, DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        let res = query.fetch_optional::<Q>().await?;

        Ok(res)
    }

    async fn query_many<Q: ClickhouseQuery, P: BindParameters>(&self, query: impl AsRef<str> + Send, params: &P) -> Result<Vec<Q>, DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        let res = query.fetch_all::<Q>().await?;

        Ok(res)
    }

    async fn query_raw<Q: ClickhouseQuery, P: BindParameters>(&self, query: impl AsRef<str> + Send, params: &P) -> Result<Vec<u8>, DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));
        Ok(query.fetch_raw::<Q>().await?)
    }

    async fn execute_remote<P: BindParameters>(&self, query: impl AsRef<str> + Send, params: &P) -> Result<(), DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        query.execute().await?;

        Ok(())
    }
}
