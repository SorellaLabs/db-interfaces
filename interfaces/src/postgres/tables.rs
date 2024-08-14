#![allow(async_fn_in_trait)]

use super::{client::PostgresClient, dbms::PostgresDBMS, errors::PostgresError, types::PostgresInsert};
use crate::{errors::DatabaseError, Database};

/// trait for different implementations of postgres tables
//#[async_trait::async_trait]
pub trait PostgresTable<D>: Send + Sync
where
    D: PostgresDBMS + Send + Sync + 'static
{
    const SCHEMA_NAME: &'static str;
    const TABLE_NAME: &'static str;
    const FILE_PATH: &'static str;
    const CHILD_TABLES: &'static [D];
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
    fn schema_name() -> String {
        Self::SCHEMA_NAME.to_string()
    }

    /// full name <DATABASE NAME>.<TABLE NAME>
    fn full_name() -> String {
        format!("{}.{}", Self::SCHEMA_NAME, Self::TABLE_NAME)
    }
}
