use crate::{
    clickhouse::{
        client::ClickhouseClient,
        dbms::ClickhouseDBMS,
        errors::ClickhouseError,
        tables::{ClickhouseTable, ClickhouseTableKind},
        test_utils::{ClickhouseTestingClient, ClickhouseTestingDBMS},
        types::ClickhouseInsert
    },
    errors::DatabaseError,
    Database
};

pub trait ClickhouseTestingTable<D: ClickhouseTestingDBMS + 'static>: ClickhouseTable<D> {
    /// name of the test database
    fn test_database_name() -> String {
        format!("test_{}", Self::DATABASE_NAME)
    }

    /// full name <TEST DATABASE NAME>.<TABLE NAME>
    fn full_test_name() -> String {
        format!("{}.{}", Self::test_database_name(), Self::TABLE_NAME)
    }

    fn replace_test_str(str: String) -> String {
        println!("QUERY: {}", str);

        let db_name = Self::database_name();
        let test_db_name = Self::test_database_name();

        let from0 = format!("{db_name}.");
        let to0 = format!("{test_db_name}.");

        let from1 = format!("'{db_name}'");
        let to1 = format!("'{test_db_name}'");

        let mut str = str.replace(&from0, &to0);
        str = str.replace(&from1, &to1);

        println!("\n\nQUERY: {}\n\n", str);

        str
    }
}

impl<T, D> ClickhouseTestingTable<D> for T
where
    T: ClickhouseTable<D>,
    D: ClickhouseTestingDBMS + 'static
{
}
