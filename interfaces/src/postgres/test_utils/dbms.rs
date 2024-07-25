use std::pin::Pin;

use super::PostgresTestClient;
use crate::{postgres::dbms::PostgresDBMS, errors::DatabaseError};

pub trait PostgresTestDBMS: PostgresDBMS {
    fn create_test_table<'a>(
        &'a self,
        database: &'a PostgresTestClient<Self>,
        random_seed: u32
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), DatabaseError>> + Send + 'a>>;

    fn drop_test_db(&self, database: &PostgresTestClient<Self>) -> impl std::future::Future<Output = Result<(), DatabaseError>> + Send;

    fn test_db_name(&self) -> String;
}
