use futures::Future;

use crate::{
    clickhouse::{dbms::ClickhouseDBMS, test_utils::ClickhouseTestingClient},
    DatabaseError
};

pub trait ClickhouseTestingDBMS: ClickhouseDBMS {
    fn create_test_table(&self, database: &ClickhouseTestingClient<Self>, random_seed: u32)
        -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn drop_test_db(&self, database: &ClickhouseTestingClient<Self>) -> impl Future<Output = Result<(), DatabaseError>> + Send;

    fn test_db_name(&self) -> String;
}
