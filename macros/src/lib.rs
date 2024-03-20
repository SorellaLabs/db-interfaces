#![warn(unreachable_pub)]
#![deny(unused_must_use, rust_2018_idioms)]
#![doc(test(
    no_crate_inject,
    attr(deny(warnings, rust_2018_idioms), allow(dead_code, unused_variables))
))]
#![allow(clippy::wrong_self_convention)]

use proc_macro::TokenStream;

mod clickhouse;

#[allow(unused_extern_crates)]
extern crate proc_macro;

#[proc_macro]
/// clickhouse table macro creates a struct that initializes all functionality
/// for interacting with clickhouse tables
pub fn remote_clickhouse_table(input: TokenStream) -> TokenStream {
    clickhouse::remote_table::remote_clickhouse_table(input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
