use std::fmt::Display;

use strum::IntoEnumIterator;
use strum_macros::EnumIter;

use crate::clickhouse::types::ClickhouseInsert;

pub trait DatabaseTable: Default + Send + Sync {
    const NAME: &'static str;
    type DataType: ClickhouseInsert;
}

// #[derive(Debug, Clone, Copy, EnumIter)]
// pub enum DatabaseTables {
//     Relays,
//     Blocks,
//     Transactions,
//     PrivateTxs,
//     Mempool,
//     UniqueMempool,
//     BlockObservations,
//     Pools,
//     Contracts,
//     Addresses,
//     DexTokens,
//     PoolReserves,
//     NormalizedQuotes,
//     NormalizedL2,
//     NormalizedTrades,
//     Symbols,
// }

// impl Display for DatabaseTables {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{:?}", self)
//     }
// }

// impl DatabaseTables {
//     pub fn all_vec() -> Vec<Self> {
//         DatabaseTables::iter().collect::<Vec<_>>()
//     }
// }

#[macro_export]
macro_rules! database_table {
    ($table_name:ident, $data_type:ident) => {
        #[derive(Debug, Clone, Default)]
        pub struct $table_name;

        impl DatabaseTable for $table_name {
            type DataType = $data_type;

            const NAME: &'static str = stringify!($table_name);
        }
    };

    ($table_name:ident) => {
        #[derive(Debug, Clone, Default)]
        pub struct $table_name;

        impl DatabaseTable for $table_name {
            type DataType = usize;

            const NAME: &'static str = stringify!($table_name);
        }
    };
}

// database_table!(Relays, BidDB);
// database_table!(Blocks, BlockDB);
// database_table!(BlockObservations);
// database_table!(Mempool);
// database_table!(Transactions, TransactionDB);
// database_table!(PrivateTxs, PrivateTransaction);
// database_table!(Contracts, ContractDB);
// database_table!(Pools, PoolDB);
// database_table!(Addresses);
// database_table!(DexTokens, DexTokensDB);
// database_table!(PoolReserves, PoolReservesDB);
// database_table!(NormalizedTrades, Trades);
// database_table!(NormalizedQuotes, Quotes);
// database_table!(NormalizedL2, L2);
// database_table!(Symbols, CexSymbol);
