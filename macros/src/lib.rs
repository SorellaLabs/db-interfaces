#![warn(unreachable_pub)]
#![deny(unused_must_use, rust_2018_idioms)]
#![doc(test(no_crate_inject, attr(deny(warnings, rust_2018_idioms), allow(dead_code, unused_variables))))]
#![allow(clippy::wrong_self_convention)]

use proc_macro::TokenStream;

mod clickhouse;

#[allow(unused_extern_crates)]
extern crate proc_macro;

#[proc_macro]
/// macro creates a struct that initializes all functionality
/// for interacting with clickhouse tables (must have created a `ClickhouseDBMS`
/// with the `clickhouse_dbms!` macro) There are 3 requied and 3 optional inputs
///
/// 1 - enum name of the DBMS (ident)
/// 2 - name of the database (string)
/// 3 - name of the table struct (ident)
/// 4 (optional) - the type that is used when inserting into the table
/// 5 (optional) - a tuple of 'child tables' (i.e. tables that will be
/// created/dropped along with the parent table) 6 (optional) the path to the
/// directory where the sql table is defined (relative to the workspace/crate
/// root), if not provided the testing module is disabled for the table
///
/// Examples:
/// ```
/// remote_clickhouse_table!(DMBS, "db", Table, TableInsertType);
/// remote_clickhouse_table!(DMBS, "db", Table, TableInsertType, (LocalRelays));
/// remote_clickhouse_table!(DMBS, "db", Table, TableInsertType, (LocalRelays), "path/to/table/dir");
/// remote_clickhouse_table!(DMBS, "db", Table, "path/to/table/dir");
/// ```
pub fn remote_clickhouse_table(input: TokenStream) -> TokenStream {
    clickhouse::remote_table::remote_clickhouse_table(input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
