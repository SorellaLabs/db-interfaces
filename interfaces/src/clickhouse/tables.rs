use super::{
    client::ClickhouseClient, dbms::ClickhouseDBMS, errors::ClickhouseError,
    types::ClickhouseInsert,
};
use crate::Database;

#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub enum ClickhouseTableKind {
    Distributed,
    Remote,
    RemoteSecure,
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
        create_sql = Self::replace_test_str(create_sql);

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

    fn replace_test_str(str: String) -> String {
        let db_name = Self::database_name();
        let test_db_name = Self::test_database_name();

        let from0 = format!("{db_name}.");
        let to0 = format!("{test_db_name}.");

        let from1 = format!("'{db_name}'");
        let to1 = format!("'{test_db_name}'");

        let mut str = str.replace(&from0, &to0);
        str = str.replace(&from1, &to1);

        str
    }
}
