use super::db::ClickhouseClient;
use crate::{
    clickhouse::{errors::ClickhouseError, tables::*},
    tables::*,
};

#[async_trait::async_trait]
pub trait ClickhouseDBMS: Sized + Sync + Send {
    const CLUSTER: Option<&'static str>;

    fn dependant_tables(&self) -> &[Self];

    async fn create_table(&self, database: &ClickhouseClient<Self>) -> Result<(), ClickhouseError>;

    async fn create_test_table(
        &self,
        database: &ClickhouseClient<Self>,
        random_seed: u32,
    ) -> Result<(), ClickhouseError>;

    async fn drop_test_db(&self, database: &ClickhouseClient<Self>) -> Result<(), ClickhouseError>;

    fn all_tables() -> Vec<Self>;

    /// <DB NAME>.<TABLE NAME>
    fn full_name(&self) -> String;

    fn db_name(&self) -> String;

    fn test_db_name(&self) -> String;

    fn from_database_table_str(val: &str) -> Self;
}

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
            const CLUSTER: Option<&'static str> = Some("eth_cluster0");

             fn dependant_tables(&self) -> &[Self] {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::clickhouse::tables::ClickhouseTable<Self>>::CHILD_TABLES
                    })*
                }
            }

             async fn create_table(&self, database: &::db_interfaces::clickhouse::db::ClickhouseClient<Self>) -> Result<(), ::db_interfaces::clickhouse::errors::ClickhouseError> {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::clickhouse::tables::ClickhouseTable<Self>>::create_table(database).await?
                    })*
                }

                Ok(())
            }


             async fn create_test_table(&self, database: &::db_interfaces::clickhouse::db::ClickhouseClient<Self>, random_seed: u32) -> Result<(), ::db_interfaces::clickhouse::errors::ClickhouseError> {
                match self {
                    $($dbms::$table => {
                        <$table as ::db_interfaces::clickhouse::tables::ClickhouseTable<Self>>::create_test_table(database, random_seed).await?
                    })*
                }

                Ok(())
            }

            async fn drop_test_db(&self, database: &::db_interfaces::clickhouse::db::ClickhouseClient<Self>) -> Result<(), ::db_interfaces::clickhouse::errors::ClickhouseError> {
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

// clickhouse_dbms!(
//     ClickhouseTables,
//     "eth_cluster0",
//     [
//         Relays,
//         Blocks,
//         BlockObservations,
//         Transactions,
//         PrivateTxs,
//         Mempool,
//         UniqueMempool,
//         Pools,
//         Contracts,
//         Addresses,
//         DexTokens,
//         PoolReserves,
//         NormalizedQuotes,
//         NormalizedL2,
//         NormalizedTrades,
//         Symbols,
//         ChainboundBlockBodies,
//         ChainboundBlockHeaders,
//         ChainboundBlockObservations,
//         ChainboundMempool,
//         ChainboundTransactions,
//         LocalRelays,
//         LocalMempool,
//         LocalUniqueMempool,
//         LocalTransactions,
//         LocalPoolReserves,
//         LocalNormalizedL2,
//         LocalNormalizedQuotes,
//         LocalNormalizedTrades,
//         ArkhamAddressMeta,
//         EtherscanAddressMeta,
//         RawAddressMeta
//     ]
// );
