use std::pin::Pin;

use super::client::ClickhouseClient;
use crate::errors::DatabaseError;

pub trait ClickhouseDBMS: Sized + Sync + Send {
    const CLUSTER: Option<&'static str>;

    fn dependant_tables(&self) -> &[Self];

    fn create_table<'a>(
        &'a self,
        database: &'a ClickhouseClient<Self>
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), DatabaseError>> + Send + 'a>>;

    fn all_tables() -> Vec<Self>;

    /// <DB NAME>.<TABLE NAME>
    fn full_name(&self) -> String;

    fn db_name(&self) -> String;

    fn from_database_table_str(val: &str) -> Self;
}

#[cfg(not(feature = "test-utils"))]
/// There are 2 possible inputs, for tables (not) in a distributed setup
///
/// With distributed:
/// 1. enum name for the DBMS
/// 2. cluster name
/// 3. set of tables
///
/// Example:
/// ```
/// db_interfaces::clickhouse_dbms!(ExampleDBMS0, "cluster1", [Table0, Table1])
/// ```
///
/// Without distributed:
/// 1. enum name for the DBMS
/// 3. set of tables
///
/// Example:
/// ```
/// db_interfaces::clickhouse_dbms!(ExampleDBMS1, [Table0, Table1])
/// ```
#[macro_export]
macro_rules! clickhouse_dbms {
    ($dbms:ident, [$($table:ident),*]) => {
        clickhouse_dbms!(INTERNAL, $dbms, None, [$($table,)*]);
    };

    ($dbms:ident, $cluster:expr, [$($table:ident),*]) => {
        clickhouse_dbms!(INTERNAL, $dbms, Some($cluster), [$($table,)*]);
    };

    (INTERNAL, $dbms:ident, $cluster:expr, [$($table:ident,)*]) => {
        #[allow(non_camel_case_types)]
        #[derive(Debug, PartialEq, Eq, Clone, Hash)]
        pub enum $dbms {
            $(
                #[allow(non_camel_case_types)]
                $table
            ),*
        }

        impl ::db_interfaces::clickhouse::dbms::ClickhouseDBMS for $dbms {
            const CLUSTER: Option<&'static str> = $cluster;

             fn dependant_tables(&self) -> &[Self] {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::clickhouse::tables::ClickhouseTable<Self>>::CHILD_TABLES
                    })*
                }
            }

            fn create_table<'a>(&'a self, database: &'a ::db_interfaces::clickhouse::client::ClickhouseClient<Self>)
                 -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ::db_interfaces::errors::DatabaseError>> + Send + 'a>> {
                Box::pin(async move {
                    match self {
                        $($dbms::$table => {
                            <$table as ::db_interfaces::clickhouse::tables::ClickhouseTable<Self>>::create_table(database).await
                        })*
                    }
                })

            }

            fn db_name(&self) -> String {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::clickhouse::tables::ClickhouseTable<Self>>::database_name()
                    })*
                }
            }

            fn full_name(&self) -> String {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::clickhouse::tables::ClickhouseTable<Self>>::full_name()
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
                    _ => panic!("From str: {value} is not part of ClickhouseTables")
                }
            }
        }

    }
}

#[cfg(feature = "test-utils")]
/// There are 2 possible inputs, for tables (not) in a distributed setup
///
/// With distributed:
/// 1. enum name for the DBMS
/// 2. cluster name
/// 3. set of tables
///
/// Example:
/// ```
/// db_interfaces::clickhouse_dbms!(ExampleDBMS0, "cluster1", [Table0, Table1])
/// ```
///
/// Without distributed:
/// 1. enum name for the DBMS
/// 3. set of tables
///
/// Example:
/// ```
/// db_interfaces::clickhouse_dbms!(ExampleDBMS1, [Table0, Table1])
/// ```
#[macro_export]
macro_rules! clickhouse_dbms {
    ($dbms:ident, [$($table:ident),*]) => {
        clickhouse_dbms!(INTERNAL, $dbms, None, [$($table,)*]);
    };

    ($dbms:ident, $cluster:expr, [$($table:ident),*]) => {
        clickhouse_dbms!(INTERNAL, $dbms, Some($cluster), [$($table,)*]);
    };

    (INTERNAL, $dbms:ident, $cluster:expr, [$($table:ident,)*]) => {
        #[allow(non_camel_case_types)]
        #[derive(Debug, PartialEq, Eq, Clone, Hash)]
        pub enum $dbms {
            $(
                #[allow(non_camel_case_types)]
                $table
            ),*
        }

        impl ::db_interfaces::clickhouse::dbms::ClickhouseDBMS for $dbms {
            const CLUSTER: Option<&'static str> = $cluster;

             fn dependant_tables(&self) -> &[Self] {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::clickhouse::tables::ClickhouseTable<Self>>::CHILD_TABLES
                    })*
                }
            }

            fn create_table<'a>(&'a self, database: &'a ::db_interfaces::clickhouse::client::ClickhouseClient<Self>)
                 -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ::db_interfaces::errors::DatabaseError>> + Send + 'a>> {
                Box::pin(async move {
                    match self {
                        $($dbms::$table => {
                            <$table as ::db_interfaces::clickhouse::tables::ClickhouseTable<Self>>::create_table(database).await
                        })*
                    }
                })

            }

            fn db_name(&self) -> String {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::clickhouse::tables::ClickhouseTable<Self>>::database_name()
                    })*
                }
            }

            fn full_name(&self) -> String {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::clickhouse::tables::ClickhouseTable<Self>>::full_name()
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
                    _ => panic!("From str: {value} is not part of ClickhouseTables")
                }
            }
        }

        impl ::db_interfaces::clickhouse::test_utils::ClickhouseTestDBMS for $dbms {
            fn create_test_table<'a>(&'a self, database: &'a ::db_interfaces::clickhouse::test_utils::ClickhouseTestClient<Self>, random_seed: u32)
                 -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ::db_interfaces::errors::DatabaseError>> + Send + 'a>> {
                Box::pin(async move {
                    match self {
                        $($dbms::$table => {
                            <$table as ::db_interfaces::clickhouse::test_utils::ClickhouseTestTable<Self>>::create_test_table(database, random_seed)
                                .await
                        })*
                    }
                })
            }

            #[allow(clippy::manual_async_fn)]
            fn drop_test_db(&self, database: &::db_interfaces::clickhouse::test_utils::ClickhouseTestClient<Self>)
                 -> impl std::future::Future<Output = Result<(), ::db_interfaces::errors::DatabaseError>> + Send {
                async move {
                    match self {
                        $($dbms::$table => {
                            <$table as ::db_interfaces::clickhouse::test_utils::ClickhouseTestTable<Self>>::drop_test_db(database).await
                        })*
                    }
                }
            }

            fn test_db_name(&self) -> String {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::clickhouse::test_utils::ClickhouseTestTable<Self>>::test_database_name()
                    })*
                }
            }
        }

    }
}

#[derive(Debug, Default, Clone)]
pub struct NullDBMS;

impl ClickhouseDBMS for NullDBMS {
    const CLUSTER: Option<&'static str> = None;

    fn dependant_tables(&self) -> &[Self] {
        &[]
    }

    fn create_table(&self, _database: &ClickhouseClient<Self>) -> Pin<Box<dyn std::future::Future<Output = Result<(), DatabaseError>> + Send>> {
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
impl crate::clickhouse::test_utils::ClickhouseTestDBMS for NullDBMS {
    fn create_test_table<'a>(
        &'a self,
        _database: &'a crate::clickhouse::test_utils::ClickhouseTestClient<Self>,
        _random_seed: u32
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), DatabaseError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    async fn drop_test_db(&self, _database: &crate::clickhouse::test_utils::ClickhouseTestClient<Self>) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn test_db_name(&self) -> String {
        String::new()
    }
}
