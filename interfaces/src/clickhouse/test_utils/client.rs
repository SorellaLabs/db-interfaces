use std::{collections::HashSet, pin::Pin};

use eyre::Result;
use futures::{future::join_all, Future};
use rand::Rng;

use super::ClickhouseTestDBMS;
use crate::{
    clickhouse::{client::ClickhouseClient, types::ClickhouseQuery},
    errors::DatabaseError,
    params::BindParameters,
    test_utils::TestDatabase,
    Database, DatabaseTable
};

#[derive(Clone)]
pub struct ClickhouseTestClient<D> {
    pub client: ClickhouseClient<D>
}

impl<D> ClickhouseTestClient<D>
where
    D: ClickhouseTestDBMS + 'static
{
    pub fn new_from_db(client: ClickhouseClient<D>) -> Self {
        Self { client }
    }

    pub async fn setup(&self, tables: Option<&[D]>) -> Result<(), DatabaseError> {
        self.setup_cleanup(tables, false).await?; // drops all dbs if necessary
        self.setup_cleanup(tables, true).await?; // drops all dbs

        join_all(tables.unwrap_or_default().iter().map(|table| {
            let mut rng = rand::thread_rng();
            let random_seed: u32 = rng.gen();
            table.create_test_table(self, random_seed)
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

        Ok(())
    }

    pub async fn setup_cleanup(&self, tables: Option<&[D]>, create: bool) -> Result<(), DatabaseError> {
        let cmd = if create { "CREATE DATABASE IF NOT EXISTS" } else { "DROP DATABASE IF EXISTS" };

        let drop_on_cluster = &D::CLUSTER.map(|s: &str| format!("ON CLUSTER {s}"));

        let dbs = tables
            .unwrap_or_default()
            .iter()
            .map(|table| table.test_db_name())
            .collect::<HashSet<_>>();

        join_all(dbs.iter().map(|db| {
            let mut query = format!("{cmd} {db} ");

            if let Some(dc) = drop_on_cluster {
                query.push_str(dc)
            }
            self.client.execute_remote(query, &())
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

        Ok(())
    }
}

impl<D> Database for ClickhouseTestClient<D>
where
    D: ClickhouseTestDBMS + 'static
{
    type DBMS = D;

    async fn insert_one<T: DatabaseTable>(&self, value: &T::DataType) -> Result<(), DatabaseError> {
        let table = format!("test_{}", Self::DBMS::from_database_table_str(T::NAME).full_name());
        let mut insert = self.client.client.insert(table)?;

        insert.write(value).await?;

        insert.end().await?;

        Ok(())
    }

    async fn insert_many<T: DatabaseTable>(&self, values: &[T::DataType]) -> Result<(), DatabaseError> {
        let table = format!("test_{}", Self::DBMS::from_database_table_str(T::NAME).full_name());
        let mut insert = self.client.client.insert(table)?;

        for value in values {
            insert.write(value).await?;
        }

        insert.end().await?;

        Ok(())
    }

    async fn query_one<Q: ClickhouseQuery, P: BindParameters>(&self, query: impl AsRef<str> + Send, params: &P) -> Result<Q, DatabaseError> {
        let query: String = <Self as TestDatabase<D>>::modify_query_str(query.as_ref());

        self.client.query_one(&query, params).await
    }

    async fn query_one_optional<Q: ClickhouseQuery, P: BindParameters>(
        &self,
        query: impl AsRef<str> + Send,
        params: &P
    ) -> Result<Option<Q>, DatabaseError> {
        let query = <Self as TestDatabase<D>>::modify_query_str(query.as_ref());

        self.client.query_one_optional(&query, params).await
    }

    async fn query_many<Q: ClickhouseQuery, P: BindParameters>(&self, query: impl AsRef<str> + Send, params: &P) -> Result<Vec<Q>, DatabaseError> {
        let query = <Self as TestDatabase<D>>::modify_query_str(query.as_ref());

        self.client.query_many(&query, params).await
    }

    async fn query_raw<Q: ClickhouseQuery, P: BindParameters>(&self, query: impl AsRef<str> + Send, params: &P) -> Result<Vec<u8>, DatabaseError> {
        let query = <Self as TestDatabase<D>>::modify_query_str(query.as_ref());
        self.client.query_raw::<Q, P>(&query, params).await
    }

    async fn execute_remote<P: BindParameters>(&self, query: impl AsRef<str> + Send, params: &P) -> Result<(), DatabaseError> {
        let query = <Self as TestDatabase<D>>::modify_query_str(query.as_ref());

        self.client.execute_remote(&query, params).await
    }
}

impl<D> TestDatabase<D> for ClickhouseTestClient<D>
where
    D: ClickhouseTestDBMS + 'static
{
    async fn run_test_with_test_db<'t, F>(&'t self, tables: &'t [D], f: F)
    where
        F: FnOnce(&'t Self) -> Pin<Box<dyn Future<Output = ()> + 't + Send>> + Send
    {
        self.setup(Some(tables)).await.unwrap();

        let fut = f(self);
        fut.await;

        self.setup_cleanup(Some(tables), false).await.unwrap();
    }

    fn modify_query_str(query: &str) -> String {
        let mut query = query.to_string();

        let db_names_w_test = D::all_tables()
            .iter()
            .map(|t| (t.db_name(), t.test_db_name()))
            .collect::<HashSet<_>>();

        db_names_w_test.iter().for_each(|(db, test_db)| {
            let test_db_replace0 = format!("{}.", test_db);
            let db_replace0 = format!("{}.", db);
            query = query.replace(&db_replace0, &test_db_replace0);

            let test_db_replace1 = format!("'{}'", test_db);
            let db_replace1 = format!("'{}'", db);
            query = query.replace(&db_replace1, &test_db_replace1);
        });

        query
    }
}
