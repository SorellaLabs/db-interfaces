use std::pin::Pin;

use super::ClickhouseTestClient;
use crate::{clickhouse::dbms::ClickhouseDBMS, errors::DatabaseError};

pub trait ClickhouseTestDBMS: ClickhouseDBMS {
    fn create_test_table<'a>(
        &'a self,
        database: &'a ClickhouseTestClient<Self>,
        random_seed: u32
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), DatabaseError>> + Send + 'a>>;

    fn drop_test_db(&self, database: &ClickhouseTestClient<Self>) -> impl std::future::Future<Output = Result<(), DatabaseError>> + Send;

    fn test_db_name(&self) -> String;
}
