use std::{env, marker::PhantomData};

use clickhouse::{query::Query, *};
use eyre::Result;
use hyper_tls::HttpsConnector;

use super::{
    config::ClickhouseConfig, dbms::ClickhouseDBMS, errors::ClickhouseError, types::ClickhouseQuery,
};
use crate::{params::BindParameters, Database, DatabaseTable};

#[derive(Clone)]
pub struct ClickhouseClient<D> {
    pub client: Client,
    _phantom: PhantomData<D>,
}

impl<D> Default for ClickhouseClient<D>
where
    D: ClickhouseDBMS,
{
    fn default() -> Self {
        dotenv::dotenv().ok();

        // clickhouse path
        let clickhouse_path = format!(
            "{}:{}",
            &env::var("CLICKHOUSE_URL").expect("CLICKHOUSE_URL not found in .env"),
            &env::var("CLICKHOUSE_PORT").expect("CLICKHOUSE_PORT not found in .env")
        );

        let https = HttpsConnector::new();
        let https_client = hyper::Client::builder().build::<_, hyper::Body>(https);

        // builds the clickhouse client
        let client = Client::with_http_client(https_client)
            .with_url(clickhouse_path)
            .with_user(env::var("CLICKHOUSE_USER").expect("CLICKHOUSE_USER not found in .env"))
            .with_password(env::var("CLICKHOUSE_PASS").expect("CLICKHOUSE_PASS not found in .env"));

        Self {
            client,
            _phantom: PhantomData,
        }
    }
}

impl<D> ClickhouseClient<D>
where
    D: ClickhouseDBMS,
{
    pub fn new(config: ClickhouseConfig) -> Self {
        // builds the clickhouse client
        let client = Client::default()
            .with_url(config.url)
            .with_user(config.user)
            .with_password(config.password);

        if let Some(db) = config.database {
            let client = client.clone().with_database(db);
            return Self {
                client,
                ..Default::default()
            };
        }

        Self {
            client,
            ..Default::default()
        }
    }

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

#[async_trait::async_trait]
impl<D> Database for ClickhouseClient<D>
where
    D: ClickhouseDBMS,
{
    type DBMS = D;
    type Error = ClickhouseError;

    async fn insert_one<T: DatabaseTable>(
        &self,
        value: &T::DataType,
    ) -> Result<(), ClickhouseError> {
        let mut insert = self
            .client
            .insert(Self::DBMS::from_database_table_str(T::NAME).full_name())
            .map_err(|e| ClickhouseError::InsertError(e.to_string()))?;

        insert
            .write(value)
            .await
            .map_err(|e| ClickhouseError::InsertError(e.to_string()))?;

        insert
            .end()
            .await
            .map_err(|e| ClickhouseError::InsertError(e.to_string()))?;

        Ok(())
    }

    async fn insert_many<T: DatabaseTable>(
        &self,
        values: &[T::DataType],
    ) -> Result<(), ClickhouseError> {
        let mut insert = self
            .client
            .insert(Self::DBMS::from_database_table_str(T::NAME).full_name())
            .map_err(|e| ClickhouseError::InsertError(e.to_string()))?;

        for value in values {
            insert
                .write(value)
                .await
                .map_err(|e| ClickhouseError::InsertError(e.to_string()))?;
        }

        insert
            .end()
            .await
            .map_err(|e| ClickhouseError::InsertError(e.to_string()))?;

        Ok(())
    }

    async fn query_one<Q: ClickhouseQuery, P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P,
    ) -> Result<Q, ClickhouseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        let res = query
            .fetch_one::<Q>()
            .await
            .map_err(|e| ClickhouseError::QueryError(e.to_string()))?;

        Ok(res)
    }

    async fn query_one_optional<Q: ClickhouseQuery, P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P,
    ) -> Result<Option<Q>, ClickhouseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        let res = query
            .fetch_optional::<Q>()
            .await
            .map_err(|e| ClickhouseError::QueryError(e.to_string()))?;

        Ok(res)
    }

    async fn query_many<Q: ClickhouseQuery, P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P,
    ) -> Result<Vec<Q>, ClickhouseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        let res = query
            .fetch_all::<Q>()
            .await
            .map_err(|e| ClickhouseError::QueryError(e.to_string()))?;

        Ok(res)
    }

    async fn query_raw<Q: ClickhouseQuery, P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P,
    ) -> Result<Vec<u8>, ClickhouseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));
        query
            .fetch_raw::<Q>()
            .await
            .map_err(|e| ClickhouseError::QueryError(e.to_string()))
    }

    async fn execute_remote<P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P,
    ) -> Result<(), ClickhouseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        query
            .execute()
            .await
            .map_err(|e| ClickhouseError::QueryError(e.to_string()))?;

        Ok(())
    }
}
