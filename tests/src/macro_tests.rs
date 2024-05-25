use clickhouse::Row;
use db_interfaces::{
    clickhouse::{
        tables::{ClickhouseTable, ClickhouseTableKind},
        types::NoneType
    },
    clickhouse_dbms, remote_clickhouse_table
};
use serde::{Deserialize, Serialize};

// macro_rules! clickhouse_table_test {
//     (
//     ( $struct_name:ident ),
//     ( $dbms:ident ),
//     (DATABASE_NAME | $database_name:expr),
//     (TABLE_NAME | $table_name:expr),
//     (FILE_PATH | $file_path:expr),
//     (CHILD_TABLES | [$($child_tables:ident),*]),
//     (TABLE_TYPE | $table_type:ident)
//     ) => {
//         paste::paste! {
//             #[test]
//             fn [<test_ $database_name _ $table_name>]() {
//                 dotenv::dotenv().ok();
//                 let root_path = std::env::var("ROOT_FOLDER_PATH").expect("No
// ROOT_FOLDER_PATH in .env");                 let clickhouse_table_dir =
// format!("{root_path}/clickhouse/eth_cluster0{}", $file_path);

//                 assert_eq!($struct_name::DATABASE_NAME, $database_name);
//                 assert_eq!($struct_name::TABLE_NAME, $table_name);
//                 assert_eq!($struct_name::FILE_PATH, clickhouse_table_dir);
//                 assert_eq!($struct_name::CHILD_TABLES,
// [$($dbms::$child_tables),*]);
// assert_eq!($struct_name::TABLE_TYPE, ClickhouseTableKind::$table_type);
//             }
//         }
//     };
// }

#[derive(Clone, Deserialize, Serialize, Row)]
struct Type0 {
    type0: String,
    type1: u64,
    type2: f64
}

clickhouse_dbms!(Dbms0, "cluster0", [Table0_0, Table0_1, Table0_2]);

remote_clickhouse_table!(Dbms0, "database0", Table0_0, String, "tests/sql/tables/");
remote_clickhouse_table!(Dbms0, "database1", Table0_1, Type0, (Table0_2), "tests/sql/tables/");
remote_clickhouse_table!(Dbms0, "database1", Table0_2, "tests/sql/tables/");

// clickhouse_table_test!(
//     (Table0_0),
//     (Dbms0),
//     (DATABASE_NAME | "database0"),
//     (TABLE_NAME | "table0_0"),
//     (FILE_PATH | "/tests/sql/tables/tables0_0.sql"),
//     (CHILD_TABLES | []),
//     (TABLE_TYPE | AggregatingMergeTree)
// );

// clickhouse_table_test!(
//     (Table0_1),
//     (Dbms0),
//     (DATABASE_NAME | "database1"),
//     (TABLE_NAME | "table0_1"),
//     (FILE_PATH | "/tests/sql/tables/tables0_1.sql"),
//     (CHILD_TABLES | [Table0_2]),
//     (TABLE_TYPE | Distributed)
// );

// clickhouse_table_test!(
//     (Table0_2),
//     (Dbms0),
//     (DATABASE_NAME | "database1"),
//     (TABLE_NAME | "table0_2"),
//     (FILE_PATH | "/tests/sql/tables/tables0_2.sql"),
//     (CHILD_TABLES | []),
//     (TABLE_TYPE | ReplicatedReplacingMergeTree)
// );

#[test]
fn test_table0_0() {
    dotenv::dotenv().ok();
    let root_path = std::env::var("ROOT_FOLDER_PATH").expect("No ROOT_FOLDER_PATH in .env");
    let clickhouse_table_dir = format!("{root_path}/tests/sql/tables/tables0_0.sql");

    assert_eq!(Table0_0::DATABASE_NAME, "database0");
    assert_eq!(Table0_0::TABLE_NAME, "table0_0");
    assert_eq!(Table0_0::FILE_PATH, clickhouse_table_dir);
    assert_eq!(Table0_0::CHILD_TABLES, []);
    assert_eq!(Table0_0::TABLE_TYPE, ClickhouseTableKind::AggregatingMergeTree);
}
