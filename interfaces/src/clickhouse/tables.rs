use super::{client::ClickhouseClient, dbms::ClickhouseDBMS, errors::ClickhouseError, types::ClickhouseInsert};
use crate::{clickhouse::test_utils::ClickhouseTestingDBMS, errors::DatabaseError, Database};

#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub enum ClickhouseTableKind {
    Distributed,
    Remote,
    RemoteSecure,
    ReplicatedMergeTree,
    ReplicatedAggregatingMergeTree,
    ReplicatedReplacingMergeTree,
    MaterializedView,
    ReplacingMergeTree,
    MergeTree,
    AggregatingMergeTree,
    Null,
    #[default]
    None
}

/// trait for different implementations of clickhouse tables
pub trait ClickhouseTable<D: ClickhouseDBMS + 'static>: Send + Sync {
    const DATABASE_NAME: &'static str;
    const TABLE_NAME: &'static str;
    const FILE_PATH: &'static str;
    /// tables which need to be made in order to support this table
    /// i.e. local tables for a distributed table
    const CHILD_TABLES: &'static [D];
    const TABLE_TYPE: ClickhouseTableKind;
    type ClickhouseDataType: ClickhouseInsert;

    /// name of the database
    fn database_name() -> String {
        Self::DATABASE_NAME.to_string()
    }

    /// full name <DATABASE NAME>.<TABLE NAME>
    fn full_name() -> String {
        format!("{}.{}", Self::DATABASE_NAME, Self::TABLE_NAME)
    }
}
