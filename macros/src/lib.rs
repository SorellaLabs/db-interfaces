#![warn(unreachable_pub)]
#![deny(unused_must_use, rust_2018_idioms)]
#![doc(test(no_crate_inject, attr(deny(warnings, rust_2018_idioms), allow(dead_code, unused_variables))))]
#![allow(clippy::wrong_self_convention)]

use proc_macro::TokenStream;
use syn::{Data, DeriveInput, Fields};

use quote::quote;

mod clickhouse;
mod postgres;

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

#[proc_macro]
/// macro creates a struct that initializes all functionality
/// for interacting with postgres tables (must have created a `PostgresDBMS`
/// with the `postgres_dbms!` macro) There are 3 requied and 3 optional inputs
///
/// 1 - enum name of the DBMS (ident)
/// 2 - name of the schema (string)
/// 3 - name of the table struct (ident)
/// 4 (optional) - the type that is used when inserting into the table
/// 5 (optional) - a tuple of 'child tables' (i.e. tables that will be
/// created/dropped along with the parent table) 
/// 6 (optional) the path to the directory where the sql table is defined
/// (relative to the workspace/crate root), if not provided the testing module
/// is disabled for the table
///
/// Examples:
/// ```
/// postgres_table!(DMBS, "db", Table, TableInsertType);
/// postgres_table!(DMBS, "db", Table, TableInsertType, (LocalRelays));
/// postgres_table!(DMBS, "db", Table, TableInsertType, (LocalRelays), "path/to/table/dir");
/// postgres_table!(DMBS, "db", Table, "path/to/table/dir");
/// ```
pub fn postgres_table(input: TokenStream) -> TokenStream {
    postgres::table::postgres_table(input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[proc_macro_derive(Insertable)]
pub fn insert_formatter(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a DeriveInput
    let input = syn::parse_macro_input!(input as DeriveInput);

    // Get the name of the struct
    let name = input.ident;

    // Match on the data of the struct
    let fields = match input.data {
        Data::Struct(data) => data.fields,
        _ => panic!("Insertable can only be used with structs"),
    };

    // Collect field names
    let field_names: Vec<_> = match fields {
        Fields::Named(fields) => fields.named.into_iter().map(|f| f.ident.clone()).collect(),
        _ => panic!("Insertable only supports named fields"),
    };

    // Generate code
    let expanded = quote! {
        impl #name  {
            pub fn print_fields() {
                #(println!("{}", stringify!(#field_names));)*
            }
        }
    };

    // Convert the generated code into a TokenStream and return it
    TokenStream::from(expanded)
}
