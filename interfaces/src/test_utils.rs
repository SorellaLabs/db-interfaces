use std::pin::Pin;

use futures::Future;

use crate::Database;

pub trait TestDatabase<T>: Database + Sized {
    fn run_test_with_test_db<'t, F>(&'t self, tables: &'t [T], f: F) -> impl std::future::Future<Output = ()>
    where
        F: FnOnce(&'t Self) -> Pin<Box<dyn Future<Output = ()> + 't + Send>> + Send;

    fn modify_query_str(query: &str) -> String;
}
