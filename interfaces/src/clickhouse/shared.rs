use std::collections::HashMap;

use clickhouse::Client;

use super::{config::ClickhouseConfig, dbms::ClickhouseDBMS, errors::ClickhouseError, types::ClickhouseQuery};
use crate::{errors::DatabaseError, BindParameters, Database, DatabaseTable};

pub struct BufferedClickhouseClientTx<'db, D: ClickhouseDBMS> {
    pub client: &'db Client,
    pub tx:     tokio::sync::mpsc::UnboundedSender<D::DataEnum>
}

impl<D: ClickhouseDBMS> BufferedClickhouseClientTx<'_, D> {
    pub fn send_to_buffer<T: Into<D::DataEnum>>(&self, value: T) -> Result<(), DatabaseError> {
        self.tx
            .send(value.into())
            .map_err(|e| DatabaseError::from(ClickhouseError::SharedSendError(e.to_string())))
    }
}

#[async_trait::async_trait]
impl<D> Database for BufferedClickhouseClientTx<'_, D>
where
    D: ClickhouseDBMS
{
    type DBMS = D;

    async fn insert_one<T: DatabaseTable>(&self, _value: &T::DataType) -> Result<(), DatabaseError> {
        unreachable!("cannot insert on shared buffered clickhouse client");
    }

    async fn insert_many<T: DatabaseTable>(&self, _values: &[T::DataType]) -> Result<(), DatabaseError> {
        unreachable!("cannot insert on shared buffered clickhouse client");
    }

    async fn query_one<Q: ClickhouseQuery, P: BindParameters>(&self, query: impl AsRef<str> + Send, params: P) -> Result<Q, DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        let res = query
            .fetch_one::<Q>()
            .await
            .map_err(|e| DatabaseError::from(ClickhouseError::QueryError(e.to_string())))?;

        Ok(res)
    }

    async fn query_one_optional<Q: ClickhouseQuery, P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: P
    ) -> Result<Option<Q>, DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        let res = query
            .fetch_optional::<Q>()
            .await
            .map_err(|e| DatabaseError::from(ClickhouseError::QueryError(e.to_string())))?;

        Ok(res)
    }

    async fn query_many<Q: ClickhouseQuery, P: BindParameters>(&self, query: impl AsRef<str> + Send, params: P) -> Result<Vec<Q>, DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        let res = query
            .fetch_all::<Q>()
            .await
            .map_err(|e| DatabaseError::from(ClickhouseError::QueryError(e.to_string())))?;

        Ok(res)
    }

    async fn query_raw<Q: ClickhouseQuery, P: BindParameters>(&self, query: impl AsRef<str> + Send, params: P) -> Result<Vec<u8>, DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));
        query
            .fetch_raw::<Q>()
            .await
            .map_err(|e| DatabaseError::from(ClickhouseError::QueryError(e.to_string())))
    }

    async fn execute_remote<P: BindParameters>(&self, query: impl AsRef<str> + Send, params: P) -> Result<(), DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        query
            .execute()
            .await
            .map_err(|e| DatabaseError::from(ClickhouseError::QueryError(e.to_string())))?;

        Ok(())
    }
}

pub struct BufferedClickhouseClientRx<'db, D: ClickhouseDBMS> {
    pub client:    &'db Client,
    pub rx:        tokio::sync::mpsc::UnboundedReceiver<D::DataEnum>,
    pub value_map: HashMap<D, Vec<D::DataEnum>>
}

impl<D: ClickhouseDBMS> BufferedClickhouseClientRx<'_, D> {
    // pub fn add_to_values(&self, value: D::DataEnum) -> Result<(), DatabaseError>
    // {     self.rx
    //         .send(value.into())
    //         .map_err(|e|
    // DatabaseError::from(ClickhouseError::SharedSendError(e.to_string()))) }
}

#[async_trait::async_trait]
impl<D> Database for BufferedClickhouseClientRx<'_, D>
where
    D: ClickhouseDBMS
{
    type DBMS = D;

    async fn insert_one<T: DatabaseTable>(&self, value: &T::DataType) -> Result<(), DatabaseError> {
        let mut insert = self
            .client
            .insert(Self::DBMS::from_database_table_str(T::NAME).full_name())
            .map_err(|e| DatabaseError::from(ClickhouseError::InsertError(e.to_string())))?;

        insert
            .write(value)
            .await
            .map_err(|e| DatabaseError::from(ClickhouseError::InsertError(e.to_string())))?;

        insert
            .end()
            .await
            .map_err(|e| DatabaseError::from(ClickhouseError::InsertError(e.to_string())))?;

        Ok(())
    }

    async fn insert_many<T: DatabaseTable>(&self, values: &[T::DataType]) -> Result<(), DatabaseError> {
        let mut insert = self
            .client
            .insert(Self::DBMS::from_database_table_str(T::NAME).full_name())
            .map_err(|e| DatabaseError::from(ClickhouseError::InsertError(e.to_string())))?;

        for value in values {
            insert
                .write(value)
                .await
                .map_err(|e| DatabaseError::from(ClickhouseError::InsertError(e.to_string())))?;
        }

        insert
            .end()
            .await
            .map_err(|e| DatabaseError::from(ClickhouseError::InsertError(e.to_string())))?;

        Ok(())
    }

    async fn query_one<Q: ClickhouseQuery, P: BindParameters>(&self, query: impl AsRef<str> + Send, params: P) -> Result<Q, DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        let res = query
            .fetch_one::<Q>()
            .await
            .map_err(|e| DatabaseError::from(ClickhouseError::QueryError(e.to_string())))?;

        Ok(res)
    }

    async fn query_one_optional<Q: ClickhouseQuery, P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: P
    ) -> Result<Option<Q>, DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        let res = query
            .fetch_optional::<Q>()
            .await
            .map_err(|e| DatabaseError::from(ClickhouseError::QueryError(e.to_string())))?;

        Ok(res)
    }

    async fn query_many<Q: ClickhouseQuery, P: BindParameters>(&self, query: impl AsRef<str> + Send, params: P) -> Result<Vec<Q>, DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        let res = query
            .fetch_all::<Q>()
            .await
            .map_err(|e| DatabaseError::from(ClickhouseError::QueryError(e.to_string())))?;

        Ok(res)
    }

    async fn query_raw<Q: ClickhouseQuery, P: BindParameters>(&self, query: impl AsRef<str> + Send, params: P) -> Result<Vec<u8>, DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));
        query
            .fetch_raw::<Q>()
            .await
            .map_err(|e| DatabaseError::from(ClickhouseError::QueryError(e.to_string())))
    }

    async fn execute_remote<P: BindParameters>(&self, query: impl AsRef<str> + Send, params: P) -> Result<(), DatabaseError> {
        let query = params.bind_query(self.client.query(query.as_ref()));

        query
            .execute()
            .await
            .map_err(|e| DatabaseError::from(ClickhouseError::QueryError(e.to_string())))?;

        Ok(())
    }
}
