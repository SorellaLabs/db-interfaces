use std::pin::Pin;

use super::client::PostgresClient;
use crate::errors::DatabaseError;

pub trait PostgresDBMS: Sized + Sync + Send {
    fn dependant_tables(&self) -> &[Self];

    fn create_table<'a>(
        &'a self,
        database: &'a PostgresClient<Self>
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), DatabaseError>> + Send + 'a>>;

    fn all_tables() -> Vec<Self>;

    /// <DB NAME>.<TABLE NAME>
    fn full_name(&self) -> String;

    fn db_name(&self) -> String;

    fn from_database_table_str(val: &str) -> Self;
}

impl PostgresDBMS for sqlx::Postgres {
    fn dependant_tables(&self) -> &[Self] {
        todo!()
    }

    fn create_table<'a>(
        &'a self,
        database: &'a PostgresClient<Self>
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), DatabaseError>> + Send + 'a>> {
        todo!()
    }

    fn all_tables() -> Vec<Self> {
        todo!()
    }

    fn full_name(&self) -> String {
        todo!()
    }

    fn db_name(&self) -> String {
        todo!()
    }

    fn from_database_table_str(val: &str) -> Self {
        todo!()
    }
}

#[cfg(not(feature = "test-utils"))]
/// There is one possible input
///
/// 1. enum name for the DBMS
/// 2. set of tables
///
/// Example:
/// ```
/// db_interfaces::postgres_dbms!(ExampleDBMS1, [Table0, Table1])
/// ```
#[macro_export]
macro_rules! postgres_dbms {
    ($dbms:ident, [$($table:ident),*]) => {
        #[allow(non_camel_case_types)]
        #[derive(Debug, PartialEq, Eq, Clone, Hash)]
        pub enum $dbms {
            $(
                #[allow(non_camel_case_types)]
                $table
            ),*
        }

        impl ::db_interfaces::postgres::dbms::PostgresDBMS for $dbms {
             fn dependant_tables(&self) -> &[Self] {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::postgres::tables::PostgresTable<Self>>::CHILD_TABLES
                    })*
                }
            }

            fn create_table<'a>(&'a self, database: &'a ::db_interfaces::postgres::client::PostgresClient<Self>)
                 -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ::db_interfaces::errors::DatabaseError>> + Send + 'a>> {
                Box::pin(async move {
                    match self {
                        $($dbms::$table => {
                            <$table as ::db_interfaces::postgres::tables::PostgresTable<Self>>::create_table(database).await
                        })*
                    }
                })

            }

            fn db_name(&self) -> String {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::postgres::tables::PostgresTable<Self>>::database_name()
                    })*
                }
            }

            fn full_name(&self) -> String {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::postgres::tables::PostgresTable<Self>>::full_name()
                    })*
                }
            }

            fn all_tables() -> Vec<Self> {
                vec![$($dbms::$table,)*]
            }

            fn from_database_table_str(value: &str) -> Self {
                match value {
                    $(<$table as ::db_interfaces::tables::DatabaseTable>::NAME => {
                        $dbms::$table
                    })*
                    _ => panic!("From str: {value} is not part of PostgresTables")
                }
            }
        }

    }
}

#[cfg(feature = "test-utils")]
/// There is 1 possible input
///
/// 1. enum name for the DBMS
/// 2. set of tables
///
/// Example:
/// ```
/// db_interfaces::postgres_dbms!(ExampleDBMS1, [Table0, Table1])
/// ```
#[macro_export]
macro_rules! postgres_dbms {
    ($dbms:ident, [$($table:ident),*]) => {
        postgres_dbms!(INTERNAL, $dbms, [$($table,)*]);
    };

    (INTERNAL, $dbms:ident, [$($table:ident,)*]) => {
        #[allow(non_camel_case_types)]
        #[derive(Debug, PartialEq, Eq, Clone, Hash)]
        pub enum $dbms {
            $(
                #[allow(non_camel_case_types)]
                $table
            ),*
        }

        impl ::db_interfaces::postgres::dbms::PostgresDBMS for $dbms {

             fn dependant_tables(&self) -> &[Self] {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::postgres::tables::PostgresTable<Self>>::CHILD_TABLES
                    })*
                }
            }

            fn create_table<'a>(&'a self, database: &'a ::db_interfaces::postgres::client::PostgresClient<Self>)
                 -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ::db_interfaces::errors::DatabaseError>> + Send + 'a>> {
                Box::pin(async move {
                    match self {
                        $($dbms::$table => {
                            <$table as ::db_interfaces::postgres::tables::PostgresTable<Self>>::create_table(database).await
                        })*
                    }
                })

            }

            fn db_name(&self) -> String {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::postgres::tables::PostgresTable<Self>>::database_name()
                    })*
                }
            }

            fn full_name(&self) -> String {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::postgres::tables::PostgresTable<Self>>::full_name()
                    })*
                }
            }

            fn all_tables() -> Vec<Self> {
                vec![$($dbms::$table,)*]
            }

            fn from_database_table_str(value: &str) -> Self {
                match value {
                    $(<$table as ::db_interfaces::tables::DatabaseTable>::NAME => {
                        $dbms::$table
                    })*
                    _ => panic!("From str: {value} is not part of PostgresTables")
                }
            }
        }

        impl ::db_interfaces::postgres::test_utils::PostgresTestDBMS for $dbms {
            fn create_test_table<'a>(&'a self, database: &'a ::db_interfaces::postgres::test_utils::PostgresTestClient<Self>, random_seed: u32)
                 -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ::db_interfaces::errors::DatabaseError>> + Send + 'a>> {
                Box::pin(async move {
                    match self {
                        $($dbms::$table => {
                            <$table as ::db_interfaces::postgres::test_utils::PostgresTestTable<Self>>::create_test_table(database, random_seed)
                                .await
                        })*
                    }
                })
            }

            #[allow(clippy::manual_async_fn)]
            fn drop_test_db(&self, database: &::db_interfaces::postgres::test_utils::PostgresTestClient<Self>)
                 -> impl std::future::Future<Output = Result<(), ::db_interfaces::errors::DatabaseError>> + Send {
                async move {
                    match self {
                        $($dbms::$table => {
                            <$table as ::db_interfaces::postgres::test_utils::PostgresTestTable<Self>>::drop_test_db(database).await
                        })*
                    }
                }
            }

            fn test_db_name(&self) -> String {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::postgres::test_utils::PostgresTestTable<Self>>::test_database_name()
                    })*
                }
            }
        }

    }
}

#[derive(Debug, Default, Clone)]
pub struct NullDBMS;

impl PostgresDBMS for NullDBMS {

    fn dependant_tables(&self) -> &[Self] {
        &[]
    }

    fn create_table(&self, _database: &PostgresClient<Self>) -> Pin<Box<dyn std::future::Future<Output = Result<(), DatabaseError>> + Send>> {
        Box::pin(async { Ok(()) })
    }

    fn all_tables() -> Vec<Self> {
        Vec::new()
    }

    /// <DB NAME>.<TABLE NAME>
    fn full_name(&self) -> String {
        String::new()
    }

    fn db_name(&self) -> String {
        String::new()
    }

    fn from_database_table_str(_val: &str) -> Self {
        Self
    }
}

#[cfg(feature = "test-utils")]
impl crate::postgres::test_utils::PostgresTestDBMS for NullDBMS {
    fn create_test_table<'a>(
        &'a self,
        _database: &'a crate::postgres::test_utils::PostgresTestClient<Self>,
        _random_seed: u32
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), DatabaseError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    async fn drop_test_db(&self, _database: &crate::postgres::test_utils::PostgresTestClient<Self>) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn test_db_name(&self) -> String {
        String::new()
    }
}
