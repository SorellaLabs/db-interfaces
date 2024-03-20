use super::{
    db::ClickhouseClient, dbms::ClickhouseDBMS, errors::ClickhouseError, types::ClickhouseInsert,
    utils::*,
};
use crate::{database_table, tables::*, Database};

#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub enum ClickhouseTableKind {
    Distributed,
    Remote,
    ReplicatedMergeTree,
    ReplicatedAggregatingMergeTree,
    ReplicatedReplacingMergeTree,
    MaterializedView,
    Null,
    #[default]
    None,
}

#[async_trait::async_trait]
pub trait ClickhouseTable<D>: Send + Sync
where
    D: ClickhouseDBMS + Send + Sync + 'static,
{
    const DATABASE_NAME: &'static str;
    const TABLE_NAME: &'static str;
    const FILE_PATH: &'static str;
    const CHILD_TABLES: &'static [D];
    const TABLE_TYPE: ClickhouseTableKind;
    const TABLE_ENUM: D;
    type ClickhouseDataType: ClickhouseInsert;

    /// creates the table and associated tables
    async fn create_table(database: &ClickhouseClient<D>) -> Result<(), ClickhouseError> {
        let table_sql_path = Self::FILE_PATH;
        let create_sql = std::fs::read_to_string(table_sql_path)?;
        database.execute_remote(&create_sql, &()).await?;

        for table in Self::CHILD_TABLES {
            table.create_table(database).await?;
        }

        Ok(())
    }

    /// FOR TESTING: creates the test table and associated test tables
    async fn create_test_table(
        database: &ClickhouseClient<D>,
        random_seed: u32,
    ) -> Result<(), ClickhouseError> {
        let table_sql_path = Self::FILE_PATH;
        let mut create_sql = std::fs::read_to_string(table_sql_path)?;
        create_sql = replace_test_str(create_sql);

        let table_type = Self::TABLE_TYPE;
        if matches!(table_type, ClickhouseTableKind::Distributed) {
            database.execute_remote(&create_sql, &()).await?;
        } else {
            create_sql = create_sql.replace(
                &format!("/{}", Self::TABLE_NAME),
                &format!("/test{}/{}", random_seed, Self::TABLE_NAME),
            );

            database.execute_remote(&create_sql, &()).await?;
        }

        for table in Self::CHILD_TABLES {
            table.create_test_table(database, random_seed).await?;
        }

        Ok(())
    }

    /// FOR TESTING: truncates the test table and associated test tables
    async fn drop_test_db(database: &ClickhouseClient<D>) -> Result<(), ClickhouseError> {
        let drop_on_cluster = D::CLUSTER
            .map(|s| format!("ON CLUSTER {s}"))
            .unwrap_or_default();

        let drop_query = format!(
            "DROP DATABASE IF EXISTS {} {drop_on_cluster}",
            Self::test_database_name()
        );
        database.execute_remote(&drop_query, &()).await?;

        Ok(())
    }

    fn database_name() -> String {
        Self::DATABASE_NAME.to_string()
    }

    fn test_database_name() -> String {
        format!("test_{}", Self::DATABASE_NAME)
    }

    fn full_name() -> String {
        format!("{}.{}", Self::DATABASE_NAME, Self::TABLE_NAME)
    }

    fn full_test_name() -> String {
        format!("test_{}.{}", Self::DATABASE_NAME, Self::TABLE_NAME)
    }
}

// clickhouse_table!(
//     Ethereum: [
//         (Relays, BidDB, (LocalRelays))
//         | (Blocks, BlockDB)
//         | (BlockObservations)
//         | (ChainboundBlockBodies)
//         | (ChainboundBlockHeaders)
//         | (ChainboundBlockObservations)
//         | (Mempool)
//         | (UniqueMempool)
//         | (ChainboundMempool)
//         | (Transactions, TransactionDB)
//         | (ChainboundTransactions)
//         | (Contracts, ContractDB)
//         | (Pools, PoolDB)
//         | (Addresses, (ArkhamAddressMeta, EtherscanAddressMeta, RawAddressMeta))
//         | (DexTokens, DexTokensDB)
//         | (PoolReserves, PoolReservesDB, (LocalPoolReserves))
//     ]
//     -
//     EthAnalytics: [
//     (PrivateTxs, PrivateTransaction)
//     ]
//     -
//     LocalTables: [
//         (LocalRelays, BidDB)
//         | (LocalMempool)
//         | (LocalUniqueMempool)
//         | (LocalTransactions, TransactionDB)
//         | (LocalPoolReserves, PoolReservesDB)
//         | (LocalNormalizedTrades, Trades)
//         | (LocalNormalizedQuotes, Quotes)
//         | (LocalNormalizedL2, L2)
//     ]
//     -
//     Cex: [
//         (NormalizedTrades, Trades, (LocalNormalizedTrades))
//         | (NormalizedQuotes, Quotes, (LocalNormalizedQuotes))
//         | (NormalizedL2, L2, (LocalNormalizedL2))
//         | (Symbols, CexSymbol)
//     ]
//     -
//     UtilTables: [
//      (ArkhamAddressMeta, ArkhamIntelDB)
//      | (EtherscanAddressMeta, EtherscanAddressDB)
//      | (RawAddressMeta, AddressDB)
//     ]
// );

// database_table!(UniqueMempool);
// database_table!(LocalRelays, BidDB);
// database_table!(LocalMempool);
// database_table!(LocalUniqueMempool);
// database_table!(LocalTransactions, TransactionDB);
// database_table!(LocalPoolReserves, PoolReservesDB);
// database_table!(LocalNormalizedTrades, Trades);
// database_table!(LocalNormalizedQuotes, Quotes);
// database_table!(LocalNormalizedL2, L2);
// database_table!(ChainboundBlockBodies);
// database_table!(ChainboundBlockHeaders);
// database_table!(ChainboundBlockObservations);
// database_table!(ChainboundMempool);
// database_table!(ChainboundTransactions);
// database_table!(ArkhamAddressMeta, ArkhamIntelDB);
// database_table!(EtherscanAddressMeta, EtherscanAddressDB);
// database_table!(RawAddressMeta, AddressDB);

// #[cfg(test)]
// mod table_tests {
//     use std::collections::HashSet;

//     use super::*;

//     macro_rules! clickhouse_table_test {
//         (
//         ( $struct_name:ident ),
//         (DATABASE_NAME | $database_name:expr),
//         (TABLE_NAME | $table_name:expr),
//         (FILE_PATH | $file_path:expr),
//         (CHILD_TABLES | [$($child_tables:ident),*]),
//         (TABLE_TYPE | $table_type:ident),
//         (TABLE_ENUM | $table_enum:ident)
//         ) => {
//             paste::paste! {
//                 #[test]
//                 fn [<test_ $database_name _ $table_name>]() {
//                     dotenv::dotenv().ok();
//                     let root_path = std::env::var("ROOT_FOLDER_PATH").expect("No ROOT_FOLDER_PATH in .env");
//                     let clickhouse_table_dir = format!("{root_path}/clickhouse/eth_cluster0{}", $file_path);

//                     assert_eq!($struct_name::DATABASE_NAME, $database_name);
//                     assert_eq!($struct_name::TABLE_NAME, $table_name);
//                     assert_eq!($struct_name::FILE_PATH, clickhouse_table_dir);
//                     assert_eq!($struct_name::CHILD_TABLES, [$(ClickhouseTables::$child_tables),*]);
//                     assert_eq!($struct_name::TABLE_TYPE, ClickhouseTableKind::$table_type);
//                     assert_eq!($struct_name::TABLE_ENUM, ClickhouseTables::$table_enum);
//                 }
//             }
//         };
//     }

//     clickhouse_table_test!(
//         (Relays),
//         (DATABASE_NAME | "ethereum"),
//         (TABLE_NAME | "relays"),
//         (FILE_PATH | "/ethereum/distributed/relays.sql"),
//         (CHILD_TABLES | [LocalRelays]),
//         (TABLE_TYPE | Distributed),
//         (TABLE_ENUM | Relays)
//     );

//     clickhouse_table_test!(
//         (LocalRelays),
//         (DATABASE_NAME | "local_tables"),
//         (TABLE_NAME | "relays"),
//         (FILE_PATH | "/ethereum/local/relays.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | ReplicatedMergeTree),
//         (TABLE_ENUM | LocalRelays)
//     );

//     clickhouse_table_test!(
//         (Blocks),
//         (DATABASE_NAME | "ethereum"),
//         (TABLE_NAME | "blocks"),
//         (FILE_PATH | "/ethereum/duplicated/blocks.sql"),
//         (CHILD_TABLES | [ChainboundBlockBodies, ChainboundBlockHeaders]),
//         (TABLE_TYPE | ReplicatedReplacingMergeTree),
//         (TABLE_ENUM | Blocks)
//     );

//     clickhouse_table_test!(
//         (BlockObservations),
//         (DATABASE_NAME | "ethereum"),
//         (TABLE_NAME | "block_observations"),
//         (FILE_PATH | "/ethereum/duplicated/block_observations.sql"),
//         (CHILD_TABLES | [ChainboundBlockObservations]),
//         (TABLE_TYPE | ReplicatedMergeTree),
//         (TABLE_ENUM | BlockObservations)
//     );

//     clickhouse_table_test!(
//         (ChainboundBlockBodies),
//         (DATABASE_NAME | "ethereum"),
//         (TABLE_NAME | "chainbound_block_bodies_remote"),
//         (FILE_PATH | "/ethereum/remote/chainbound_block_bodies_remote.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | Remote),
//         (TABLE_ENUM | ChainboundBlockBodies)
//     );

//     clickhouse_table_test!(
//         (ChainboundBlockHeaders),
//         (DATABASE_NAME | "ethereum"),
//         (TABLE_NAME | "chainbound_block_headers_remote"),
//         (FILE_PATH | "/ethereum/remote/chainbound_block_headers_remote.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | Remote),
//         (TABLE_ENUM | ChainboundBlockHeaders)
//     );

//     clickhouse_table_test!(
//         (ChainboundBlockObservations),
//         (DATABASE_NAME | "ethereum"),
//         (TABLE_NAME | "chainbound_block_observations_remote"),
//         (FILE_PATH | "/ethereum/remote/chainbound_block_observations_remote.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | Remote),
//         (TABLE_ENUM | ChainboundBlockObservations)
//     );

//     clickhouse_table_test!(
//         (Mempool),
//         (DATABASE_NAME | "ethereum"),
//         (TABLE_NAME | "mempool"),
//         (FILE_PATH | "/ethereum/distributed/mempool.sql"),
//         (CHILD_TABLES | [UniqueMempool, LocalMempool, ChainboundMempool]),
//         (TABLE_TYPE | Distributed),
//         (TABLE_ENUM | Mempool)
//     );

//     clickhouse_table_test!(
//         (LocalMempool),
//         (DATABASE_NAME | "local_tables"),
//         (TABLE_NAME | "mempool"),
//         (FILE_PATH | "/ethereum/local/mempool.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | ReplicatedMergeTree),
//         (TABLE_ENUM | LocalMempool)
//     );

//     clickhouse_table_test!(
//         (UniqueMempool),
//         (DATABASE_NAME | "ethereum"),
//         (TABLE_NAME | "unique_mempool"),
//         (FILE_PATH | "/ethereum/distributed/unique_mempool.sql"),
//         (CHILD_TABLES | [LocalUniqueMempool]),
//         (TABLE_TYPE | Distributed),
//         (TABLE_ENUM | UniqueMempool)
//     );

//     clickhouse_table_test!(
//         (LocalUniqueMempool),
//         (DATABASE_NAME | "local_tables"),
//         (TABLE_NAME | "unique_mempool"),
//         (FILE_PATH | "/ethereum/local/unique_mempool.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | ReplicatedReplacingMergeTree),
//         (TABLE_ENUM | LocalUniqueMempool)
//     );

//     clickhouse_table_test!(
//         (ChainboundMempool),
//         (DATABASE_NAME | "ethereum"),
//         (TABLE_NAME | "chainbound_mempool_remote"),
//         (FILE_PATH | "/ethereum/remote/chainbound_mempool_remote.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | Remote),
//         (TABLE_ENUM | ChainboundMempool)
//     );

//     clickhouse_table_test!(
//         (Transactions),
//         (DATABASE_NAME | "ethereum"),
//         (TABLE_NAME | "transactions"),
//         (FILE_PATH | "/ethereum/distributed/transactions.sql"),
//         (CHILD_TABLES | [ChainboundTransactions, LocalTransactions, PrivateTxs]),
//         (TABLE_TYPE | Distributed),
//         (TABLE_ENUM | Transactions)
//     );

//     clickhouse_table_test!(
//         (LocalTransactions),
//         (DATABASE_NAME | "local_tables"),
//         (TABLE_NAME | "transactions"),
//         (FILE_PATH | "/ethereum/local/transactions.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | ReplicatedMergeTree),
//         (TABLE_ENUM | LocalTransactions)
//     );

//     clickhouse_table_test!(
//         (ChainboundTransactions),
//         (DATABASE_NAME | "ethereum"),
//         (TABLE_NAME | "chainbound_transactions_remote"),
//         (FILE_PATH | "/ethereum/remote/chainbound_transactions_remote.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | Remote),
//         (TABLE_ENUM | ChainboundTransactions)
//     );

//     clickhouse_table_test!(
//         (Contracts),
//         (DATABASE_NAME | "ethereum"),
//         (TABLE_NAME | "contracts"),
//         (FILE_PATH | "/ethereum/duplicated/contracts.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | ReplicatedReplacingMergeTree),
//         (TABLE_ENUM | Contracts)
//     );

//     clickhouse_table_test!(
//         (Pools),
//         (DATABASE_NAME | "ethereum"),
//         (TABLE_NAME | "pools"),
//         (FILE_PATH | "/ethereum/duplicated/pools.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | ReplicatedReplacingMergeTree),
//         (TABLE_ENUM | Pools)
//     );

//     clickhouse_table_test!(
//         (Addresses),
//         (DATABASE_NAME | "ethereum"),
//         (TABLE_NAME | "addresses"),
//         (FILE_PATH | "/ethereum/duplicated/addresses.sql"),
//         (CHILD_TABLES | [ArkhamAddressMeta, EtherscanAddressMeta]),
//         (TABLE_TYPE | ReplicatedReplacingMergeTree),
//         (TABLE_ENUM | Addresses)
//     );

//     clickhouse_table_test!(
//         (DexTokens),
//         (DATABASE_NAME | "ethereum"),
//         (TABLE_NAME | "dex_tokens"),
//         (FILE_PATH | "/ethereum/duplicated/dex_tokens.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | ReplicatedMergeTree),
//         (TABLE_ENUM | DexTokens)
//     );

//     clickhouse_table_test!(
//         (PoolReserves),
//         (DATABASE_NAME | "ethereum"),
//         (TABLE_NAME | "pool_reserves"),
//         (FILE_PATH | "/ethereum/distributed/pool_reserves.sql"),
//         (CHILD_TABLES | [LocalPoolReserves]),
//         (TABLE_TYPE | Distributed),
//         (TABLE_ENUM | PoolReserves)
//     );

//     clickhouse_table_test!(
//         (LocalPoolReserves),
//         (DATABASE_NAME | "local_tables"),
//         (TABLE_NAME | "pool_reserves"),
//         (FILE_PATH | "/ethereum/local/pool_reserves.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | ReplicatedMergeTree),
//         (TABLE_ENUM | LocalPoolReserves)
//     );

//     clickhouse_table_test!(
//         (Symbols),
//         (DATABASE_NAME | "cex"),
//         (TABLE_NAME | "symbols"),
//         (FILE_PATH | "/cex/duplicated_tables/symbols.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | ReplicatedMergeTree),
//         (TABLE_ENUM | Symbols)
//     );

//     clickhouse_table_test!(
//         (NormalizedL2),
//         (DATABASE_NAME | "cex"),
//         (TABLE_NAME | "normalized_l2"),
//         (FILE_PATH | "/cex/distributed_tables/normalized_l2.sql"),
//         (CHILD_TABLES | [LocalNormalizedL2]),
//         (TABLE_TYPE | Distributed),
//         (TABLE_ENUM | NormalizedL2)
//     );

//     clickhouse_table_test!(
//         (LocalNormalizedL2),
//         (DATABASE_NAME | "local_tables"),
//         (TABLE_NAME | "normalized_l2"),
//         (FILE_PATH | "/cex/local_tables/normalized_l2.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | ReplicatedMergeTree),
//         (TABLE_ENUM | LocalNormalizedL2)
//     );

//     clickhouse_table_test!(
//         (NormalizedQuotes),
//         (DATABASE_NAME | "cex"),
//         (TABLE_NAME | "normalized_quotes"),
//         (FILE_PATH | "/cex/distributed_tables/normalized_quotes.sql"),
//         (CHILD_TABLES | [LocalNormalizedQuotes]),
//         (TABLE_TYPE | Distributed),
//         (TABLE_ENUM | NormalizedQuotes)
//     );

//     clickhouse_table_test!(
//         (LocalNormalizedQuotes),
//         (DATABASE_NAME | "local_tables"),
//         (TABLE_NAME | "normalized_quotes"),
//         (FILE_PATH | "/cex/local_tables/normalized_quotes.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | ReplicatedMergeTree),
//         (TABLE_ENUM | LocalNormalizedQuotes)
//     );

//     clickhouse_table_test!(
//         (NormalizedTrades),
//         (DATABASE_NAME | "cex"),
//         (TABLE_NAME | "normalized_trades"),
//         (FILE_PATH | "/cex/distributed_tables/normalized_trades.sql"),
//         (CHILD_TABLES | [LocalNormalizedTrades]),
//         (TABLE_TYPE | Distributed),
//         (TABLE_ENUM | NormalizedTrades)
//     );

//     clickhouse_table_test!(
//         (LocalNormalizedTrades),
//         (DATABASE_NAME | "local_tables"),
//         (TABLE_NAME | "normalized_trades"),
//         (FILE_PATH | "/cex/local_tables/normalized_trades.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | ReplicatedMergeTree),
//         (TABLE_ENUM | LocalNormalizedTrades)
//     );

//     clickhouse_table_test!(
//         (ArkhamAddressMeta),
//         (DATABASE_NAME | "util_tables"),
//         (TABLE_NAME | "arkham_address_meta"),
//         (FILE_PATH | "/ethereum/util_tables/arkham_addresses.sql"),
//         (CHILD_TABLES | []),
//         (TABLE_TYPE | ReplicatedMergeTree),
//         (TABLE_ENUM | ArkhamAddressMeta)
//     );

//     #[test]
//     fn check_num_tables_with_deps() {
//         let num_clickhouse_tables = ClickhouseTables::all_tables().len();

//         let all_database_variants = DatabaseTables::all_vec()
//             .into_iter()
//             .flat_map(|table| {
//                 let this = ClickhouseTables::from_database_table_str(&table.to_string());
//                 let mut tables = this.dependant_tables().to_vec();
//                 tables.push(this);
//                 tables
//             })
//             .collect::<HashSet<_>>()
//             .len();

//         assert_eq!(num_clickhouse_tables, all_database_variants);
//     }
// }
