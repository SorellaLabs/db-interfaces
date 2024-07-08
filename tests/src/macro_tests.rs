use clickhouse::{DbRow, Row};
use db_interfaces::{
    clickhouse::tables::{ClickhouseTable, ClickhouseTableKind},
    clickhouse_dbms, remote_clickhouse_table
};
use serde::{Deserialize, Serialize};

fn workspace_dir() -> String {
    let output = std::process::Command::new(env!("CARGO"))
        .arg("locate-project")
        .arg("--workspace")
        .arg("--message-format=plain")
        .output()
        .unwrap()
        .stdout;
    let cargo_path = std::path::Path::new(std::str::from_utf8(&output).unwrap().trim());
    cargo_path
        .parent()
        .unwrap()
        .to_path_buf()
        .to_str()
        .unwrap()
        .to_string()
}

macro_rules! clickhouse_table_test {
    (
    ( $struct_name:ident ),
    (DATABASE_NAME | $database_name:expr),
    (TABLE_NAME | $table_name:expr),
    (FILE_PATH | $file_path:expr),
    (CHILD_TABLES | [$($child_tables:ident),*]),
    (TABLE_TYPE | $table_type:ident),
    (TABLE_ENUM | $table_enum:ident)
    ) => {
        paste::paste! {
            #[allow(non_snake_case)]
            #[test]
            fn [<test_ $database_name _ $struct_name>]() {
                let root_path = workspace_dir();
                let clickhouse_table_dir = format!("{root_path}{}", $file_path);

                assert_eq!($struct_name::DATABASE_NAME, $database_name);
                assert_eq!($struct_name::TABLE_NAME, $table_name);
                assert_eq!($struct_name::FILE_PATH, clickhouse_table_dir);
                assert_eq!($struct_name::CHILD_TABLES, [$(Dbms0::$child_tables),*]);
                assert_eq!($struct_name::TABLE_TYPE, ClickhouseTableKind::$table_type);
                assert_eq!($struct_name::TABLE_ENUM, Dbms0::$table_enum);
            }
        }
    };
}

#[derive(Clone, Deserialize, Serialize, Row)]
pub struct Type0 {
    type0: String,
    type1: u64,
    type2: f64
}

#[derive(Clone, Serialize, Row)]
pub struct TypeGeneric<T: Clone + Serialize + DbRow> {
    #[serde(flatten)]
    inner: T,
    id:    u64
}

// Table0_1, Table0_2, Table0_3
clickhouse_dbms!(Dbms0, "cluster0", [Database0Table0_0, Database1Table0_1, Database1Table0_2, Database1Sub_Db0Table0_3, Database1Sub_Db0Table0_4]);

remote_clickhouse_table!(Dbms0, [Database0, Table0_0], String, "tests/sql/tables/");
remote_clickhouse_table!(Dbms0, [Database1, Table0_1], Type0, (Database0Table0_0), "tests/sql/tables/");
remote_clickhouse_table!(Dbms0, [Database1, Table0_2], "tests/sql/tables/");
remote_clickhouse_table!(Dbms0, [Database1, Sub_Db0, Table0_3], Type0, "tests/sql/tables/");
remote_clickhouse_table!(Dbms0, [Database1, Sub_Db0, Table0_4], TypeGeneric<Type0>, "tests/sql/tables/");

clickhouse_table_test!(
    (Database0Table0_0),
    (DATABASE_NAME | "database0"),
    (TABLE_NAME | "table0_0"),
    (FILE_PATH | "/tests/sql/tables/table0_0.sql"),
    (CHILD_TABLES | []),
    (TABLE_TYPE | AggregatingMergeTree),
    (TABLE_ENUM | Database0Table0_0)
);

clickhouse_table_test!(
    (Database1Table0_1),
    (DATABASE_NAME | "database1"),
    (TABLE_NAME | "table0_1"),
    (FILE_PATH | "/tests/sql/tables/table0_1.sql"),
    (CHILD_TABLES | [Database0Table0_0]),
    (TABLE_TYPE | Distributed),
    (TABLE_ENUM | Database1Table0_1)
);

clickhouse_table_test!(
    (Database1Table0_2),
    (DATABASE_NAME | "database1"),
    (TABLE_NAME | "table0_2"),
    (FILE_PATH | "/tests/sql/tables/table0_2.sql"),
    (CHILD_TABLES | []),
    (TABLE_TYPE | ReplicatedReplacingMergeTree),
    (TABLE_ENUM | Database1Table0_2)
);

clickhouse_table_test!(
    (Database1Sub_Db0Table0_3),
    (DATABASE_NAME | "database1"),
    (TABLE_NAME | "`sub_db0.table0_3`"),
    (FILE_PATH | "/tests/sql/tables/sub_db0.table0_3.sql"),
    (CHILD_TABLES | []),
    (TABLE_TYPE | ReplicatedReplacingMergeTree),
    (TABLE_ENUM | Database1Sub_Db0Table0_3)
);
