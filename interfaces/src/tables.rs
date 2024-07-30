use tokio_postgres::ToStatement;

use crate::clickhouse::types::ClickhouseInsert;
use crate::postgres::types::PostgresInsert;

pub trait DatabaseTable: Default + Send + Sync {
    const NAME: &'static str;
    type DataType: ClickhouseInsert + PostgresInsert;
}

#[macro_export]
macro_rules! database_table {
    ($table_name:ident, $data_type:ty) => {
        #[allow(non_camel_case_types)]
        #[derive(Debug, Clone, Default)]
        pub struct $table_name;

        impl db_interfaces::tables::DatabaseTable for $table_name {
            type DataType = $data_type;

            const NAME: &'static str = stringify!($table_name);
        }
    };

    ($table_name:ident) => {
        #[allow(non_camel_case_types)]
        #[derive(Debug, Clone, Default)]
        pub struct $table_name;

        impl db_interfaces::tables::DatabaseTable for $table_name {
            type DataType = usize;

            const NAME: &'static str = stringify!($table_name);
        }
    };
}
