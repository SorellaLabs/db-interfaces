#![allow(async_fn_in_trait)]

use super::{client::PostgresClient, dbms::PostgresDBMS, errors::PostgresError, types::PostgresInsert};
use crate::{errors::DatabaseError, Database};

#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub enum PostgresTableKind {
    Distributed,
    Remote,
    RemoteSecure,
    ReplicatedMergeTree,
    ReplicatedAggregatingMergeTree,
    ReplicatedReplacingMergeTree,
    MergeTree,
    AggregatingMergeTree,
    ReplacingMergeTree,
    MaterializedView,
    Null,
    #[default]
    None
}

/// trait for different implementations of postgres tables
//#[async_trait::async_trait]
pub trait PostgresTable<D>: Send + Sync
where
    D: PostgresDBMS + Send + Sync + 'static
{
    const DATABASE_NAME: &'static str;
    const TABLE_NAME: &'static str;
    const FILE_PATH: &'static str;
    const CHILD_TABLES: &'static [D];
    const TABLE_TYPE: PostgresTableKind;
    const TABLE_ENUM: D;
    type PostgresDataType: PostgresInsert;

    /// creates the table and associated tables
    fn create_table(database: &PostgresClient<D>) -> impl std::future::Future<Output = Result<(), DatabaseError>> + Send {
        async {
            let table_sql_path = Self::FILE_PATH;
            let create_sql = std::fs::read_to_string(table_sql_path).map_err(|e| PostgresError::SqlFileReadError(e.to_string()))?;
            database.execute_remote(&create_sql, &()).await?;

            for table in Self::CHILD_TABLES {
                table.create_table(database).await?;
            }

            Ok(())
        }
    }

    /// name of the database
    fn database_name() -> String {
        Self::DATABASE_NAME.to_string()
    }

    /// full name <DATABASE NAME>.<TABLE NAME>
    fn full_name() -> String {
        format!("{}.{}", Self::DATABASE_NAME, Self::TABLE_NAME)
    }
}
