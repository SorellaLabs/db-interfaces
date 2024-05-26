use super::{ClickhouseTestClient, ClickhouseTestDBMS};
use crate::{
    clickhouse::{
        errors::ClickhouseError,
        tables::{ClickhouseTable, ClickhouseTableKind}
    },
    errors::DatabaseError,
    Database
};

pub trait ClickhouseTestTable<D>: ClickhouseTable<D>
where
    D: ClickhouseTestDBMS + 'static
{
    /// FOR TESTING: creates the test table and associated test tables
    fn create_test_table(
        database: &ClickhouseTestClient<D>,
        random_seed: u32
    ) -> impl std::future::Future<Output = Result<(), DatabaseError>> + Send {
        async move {
            let table_sql_path = Self::FILE_PATH;
            let mut create_sql = std::fs::read_to_string(table_sql_path).map_err(|e| ClickhouseError::SqlFileReadError(e.to_string()))?;
            create_sql = Self::replace_test_str(create_sql);

            let table_type = Self::TABLE_TYPE;
            if matches!(table_type, ClickhouseTableKind::Distributed) {
                database.client.execute_remote(&create_sql, &()).await?;
            } else {
                create_sql = create_sql.replace(&format!("/{}", Self::TABLE_NAME), &format!("/test{}/{}", random_seed, Self::TABLE_NAME));

                database.client.execute_remote(&create_sql, &()).await?;
            }

            for table in Self::CHILD_TABLES {
                table.create_test_table(database, random_seed).await?;
            }

            Ok(())
        }
    }

    /// FOR TESTING: truncates the test table and associated test tables
    fn drop_test_db(database: &ClickhouseTestClient<D>) -> impl std::future::Future<Output = Result<(), DatabaseError>> + Send {
        async {
            let drop_on_cluster = D::CLUSTER
                .map(|s| format!("ON CLUSTER {s}"))
                .unwrap_or_default();

            let drop_query = format!("DROP DATABASE IF EXISTS {} {drop_on_cluster}", Self::test_database_name());
            database.client.execute_remote(&drop_query, &()).await?;

            Ok(())
        }
    }

    /// name of the test database
    fn test_database_name() -> String {
        format!("test_{}", Self::DATABASE_NAME)
    }

    /// full name <TEST DATABASE NAME>.<TABLE NAME>
    fn full_test_name() -> String {
        format!("{}.{}", Self::test_database_name(), Self::TABLE_NAME)
    }

    /// replaces the database/table names from a string
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
