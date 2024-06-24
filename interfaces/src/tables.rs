use crate::clickhouse::types::ClickhouseInsert;

pub trait DatabaseTable: Default + Send + Sync {
    const NAME: &'static str;
    type DataType: ClickhouseInsert;
    type DBMS;

    fn make_dbms(val: &Self::DataType) -> Self::DBMS;
}

#[macro_export]
macro_rules! database_table {
    ($table_name:ident, $data_type:ty) => {
        #[allow(non_camel_case_types)]
        #[derive(Debug, Clone, Default)]
        pub struct $table_name;

        impl db_interfaces::tables::DatabaseTable for $table_name {
            type DBMS = db_interfaces::clickhouse::dbms::NullDBMS;
            type DataType = $data_type;

            const NAME: &'static str = stringify!($table_name);

            fn make_dbms(val: &Self::DataType) -> Self::DBMS {
                Default::default()
            }
        }
    };

    ($table_name:ident) => {
        #[allow(non_camel_case_types)]
        #[derive(Debug, Clone, Default)]
        pub struct $table_name;

        impl db_interfaces::tables::DatabaseTable for $table_name {
            type DBMS = db_interfaces::clickhouse::dbms::NullDBMS;
            type DataType = usize;

            const NAME: &'static str = stringify!($table_name);

            fn make_dbms(val: &Self::DataType) -> Self::DBMS {
                Default::default()
            }
        }
    };

    (VALUE_ENUM | $table_name:ident, $data_type:ty, $dbms:ident) => {
        #[allow(non_camel_case_types)]
        #[derive(Debug, Clone, Default)]
        pub struct $table_name;

        impl db_interfaces::tables::DatabaseTable for $table_name {
            type DBMS = $dbms;
            type DataType = $data_type;

            const NAME: &'static str = stringify!($table_name);

            fn make_dbms(val: &Self::DataType) -> Self::DBMS {
                $dbms::$table_name(val.clone())
            }
        }
    };

    (SIMPLE_ENUM | $table_name:ident, $data_type:ty, $dbms:ident) => {
        #[allow(non_camel_case_types)]
        #[derive(Debug, Clone, Default)]
        pub struct $table_name;

        impl db_interfaces::tables::DatabaseTable for $table_name {
            type DBMS = $dbms;
            type DataType = $data_type;

            const NAME: &'static str = stringify!($table_name);

            fn make_dbms(val: &Self::DataType) -> Self::DBMS {
                $dbms::$table_name
            }
        }
    };
}
