use super::client::ClickhouseClient;
use crate::errors::DatabaseError;

#[async_trait::async_trait]
pub trait ClickhouseDBMS: Sized + Sync + Send {
    const CLUSTER: Option<&'static str>;

    fn dependant_tables(&self) -> &[Self];

    async fn create_table(&self, database: &ClickhouseClient<Self>) -> Result<(), DatabaseError>;

    async fn create_test_table(
        &self,
        database: &ClickhouseClient<Self>,
        random_seed: u32,
    ) -> Result<(), DatabaseError>;

    async fn drop_test_db(&self, database: &ClickhouseClient<Self>) -> Result<(), DatabaseError>;

    fn all_tables() -> Vec<Self>;

    /// <DB NAME>.<TABLE NAME>
    fn full_name(&self) -> String;

    fn db_name(&self) -> String;

    fn test_db_name(&self) -> String;

    fn from_database_table_str(val: &str) -> Self;
}

/// There are 2 possible inputs, for tables (not) in a distributed setup
///
/// With distributed:
/// 1. enum name for the DBMS
/// 2. cluster name
/// 3. set of tables
///
/// Example:
/// ```
/// clickhouse_dbms!(ExampleDBMS0, "cluster1", [Table0, Table1])
/// ```
///
/// Without distributed:
/// 1. enum name for the DBMS
/// 3. set of tables
///
/// Example:
/// ```
/// clickhouse_dbms!(ExampleDBMS1, [Table0, Table1])
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

        #[derive(Debug, PartialEq, Eq, Clone, Hash)]
        pub enum $dbms {
            $($table),*
        }

        #[async_trait::async_trait]
        impl ::db_interfaces::clickhouse::dbms::ClickhouseDBMS for $dbms {
            const CLUSTER: Option<&'static str> = $cluster;

             fn dependant_tables(&self) -> &[Self] {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::clickhouse::tables::ClickhouseTable<Self>>::CHILD_TABLES
                    })*
                }
            }

             async fn create_table(&self, database: &::db_interfaces::clickhouse::client::ClickhouseClient<Self>) -> Result<(), ::db_interfaces::errors::DatabaseError> {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::clickhouse::tables::ClickhouseTable<Self>>::create_table(database).await?
                    })*
                }

                Ok(())
            }


             async fn create_test_table(&self, database: &::db_interfaces::clickhouse::client::ClickhouseClient<Self>, random_seed: u32) -> Result<(), ::db_interfaces::errors::DatabaseError> {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::clickhouse::tables::ClickhouseTable<Self>>::create_test_table(database, random_seed).await?
                    })*
                }

                Ok(())
            }

            async fn drop_test_db(&self, database: &::db_interfaces::clickhouse::client::ClickhouseClient<Self>) -> Result<(), ::db_interfaces::errors::DatabaseError> {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::clickhouse::tables::ClickhouseTable<Self>>::drop_test_db(database).await?
                    })*
                }

                Ok(())
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

            fn test_db_name(&self) -> String {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::clickhouse::tables::ClickhouseTable<Self>>::test_database_name()
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




#[derive(Debug, Default, Clone)]
pub struct NullDBMS;


#[async_trait::async_trait]
impl ClickhouseDBMS for NullDBMS {
    const CLUSTER: Option<&'static str> = None;

    fn dependant_tables(&self) -> &[Self] {
        &[]
    }

    async fn create_table(&self, database: &ClickhouseClient<Self>) -> Result<(), DatabaseError> {
        Ok(())
    }

    async fn create_test_table(
        &self,
        database: &ClickhouseClient<Self>,
        random_seed: u32,
    ) -> Result<(), DatabaseError>{
        Ok(())
    }

    async fn drop_test_db(&self, database: &ClickhouseClient<Self>) -> Result<(), DatabaseError>{
        Ok(())
    }

    fn all_tables() -> Vec<Self>
    {
        Vec::new()
    }

    /// <DB NAME>.<TABLE NAME>
    fn full_name(&self) -> String {
        String::new()
    }

    fn db_name(&self) -> String{
        String::new()
    }

    fn test_db_name(&self) -> String{
        String::new()
    }

    fn from_database_table_str(val: &str) -> Self{
        Self::default()
    }
}