use std::pin::Pin;

use futures::Future;

use crate::Database;

#[async_trait::async_trait]
pub trait TestDatabase<T>: Database + Sized {
    async fn run_test_with_test_db<'t, F>(&'t self, tables: &'t [T], f: F)
    where
        F: FnOnce(&'t Self) -> Pin<Box<dyn Future<Output = ()> + 't + Send>> + Send;

    fn modify_query_str(query: &str) -> String;
}
