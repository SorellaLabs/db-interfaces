use itertools::Itertools;
use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use syn::{parenthesized, parse::Parse, token, Expr, Ident, LitStr, Token};

use super::{table::ClickhouseTableParse, types::TableMeta};

pub(crate) fn remote_clickhouse_table(token_stream: TokenStream) -> syn::Result<TokenStream> {
    let parsed: RemoteClickhouseTableParse = syn::parse2(token_stream)?;
    let token_stream = parsed.to_token_stream()?;

    Ok(token_stream)
}

#[derive(Clone)]
pub(crate) struct RemoteClickhouseTableParse {
    pub(crate) table_path:          Option<LitStr>,
    pub(crate) dbms:                Ident,
    pub(crate) table_name:          Ident,
    pub(crate) database_name:       LitStr,
    pub(crate) data_type:           TokenStream,
    pub(crate) other_tables_needed: Vec<Expr>
}

impl RemoteClickhouseTableParse {
    fn to_token_stream(self) -> syn::Result<TokenStream> {
        let this = self.clone();
        let RemoteClickhouseTableParse { table_path, dbms, table_name, data_type, database_name, other_tables_needed } = self;
        let other_tables_needed = other_tables_needed
            .into_iter()
            .map(|table| table.into_token_stream())
            .collect_vec();

        let TableMeta { table_name_lowercase, enum_name, table_type, file_path } = TableMeta::new(this.into(), table_path.as_ref())?;

        let (table_name_lowercase, enum_name, table_type, file_path, other_tables_needed) =
            (table_name_lowercase, enum_name, table_type, file_path.into_token_stream(), quote!(&[#(#dbms::#other_tables_needed),*]));

        // let no_file_impls = if table_path.is_none() {
        //     quote! {
        //             async fn create_table(_database:
        // &::db_interfaces::clickhouse::client::ClickhouseClient<#dbms>) -> Result<(),
        // ::db_interfaces::errors::DatabaseError> {
        // unreachable!("Not Enabled - No File Path Given In Macro")
        // }

        //             async fn create_test_table(_database:
        // &::db_interfaces::clickhouse::client::ClickhouseClient<#dbms>, _random_seed:
        // u32) -> Result<(), ::db_interfaces::errors::DatabaseError> {
        //                 unreachable!("Not Enabled - No File Path Given In Macro")
        //             }
        //     }
        // } else {
        //     quote! {
        //             async fn create_table(database:
        // &::db_interfaces::clickhouse::client::ClickhouseClient<#dbms>) -> Result<(),
        // ::db_interfaces::errors::DatabaseError> {                 let
        // table_sql_path = <Self as
        // ::db_interfaces::clickhouse::tables::ClickhouseTable<#dbms>>::FILE_PATH;
        //                 let create_sql =
        // std::fs::read_to_string(table_sql_path).map_err(|e|
        // ::db_interfaces::clickhouse::errors::ClickhouseError::SqlFileReadError(e.
        // to_string()))?;
        // ::db_interfaces::Database::execute_remote(database, &create_sql, &()).await?;

        //                 Ok(())
        //             }

        //             async fn create_test_table(database:
        // &::db_interfaces::clickhouse::client::ClickhouseClient<#dbms>, random_seed:
        // u32) -> Result<(), ::db_interfaces::errors::DatabaseError> {
        //                 let table_sql_path = <Self as
        // ::db_interfaces::clickhouse::tables::ClickhouseTable<#dbms>>::FILE_PATH;
        //                 let mut create_sql =
        // std::fs::read_to_string(table_sql_path).map_err(|e|
        // ::db_interfaces::clickhouse::errors::ClickhouseError::SqlFileReadError(e.
        // to_string()))?;

        //                 let db = <Self as
        // ::db_interfaces::clickhouse::tables::ClickhouseTable<#dbms>>::DATABASE_NAME;
        //                 create_sql = create_sql.replace(&format!("{db}."),
        // &format!("test_{db}."));                 create_sql =
        // create_sql.replace(&format!("'{db}'"), &format!("'test_{db}'"));

        //                 let table_type = <Self as
        // ::db_interfaces::clickhouse::tables::ClickhouseTable<#dbms>>::TABLE_TYPE;
        //                 match table_type {
        //
        // db_interfaces::clickhouse::tables::ClickhouseTableKind::Distributed =>
        // ::db_interfaces::Database::execute_remote(database, &create_sql, &()).await?,
        //                     _ => {
        //                         create_sql = create_sql.replace(&format!("/{}", <Self
        // as ::db_interfaces::clickhouse::tables::ClickhouseTable<#dbms>>::TABLE_NAME),
        // &format!("/test{}/{}", random_seed, <Self as
        // ::db_interfaces::clickhouse::tables::ClickhouseTable<#dbms>>::TABLE_NAME));

        //                         ::db_interfaces::Database::execute_remote(database,
        // &create_sql, &()).await?;                     }
        //                 }

        //                 Ok(())
        //             }

        //     }
        // };

        let val = quote! {
            impl ::db_interfaces::clickhouse::tables::ClickhouseTable<#dbms> for #table_name {
                const DATABASE_NAME: &'static str = #database_name;
                const TABLE_NAME: &'static str = #table_name_lowercase;
                const FILE_PATH: &'static str = #file_path;
                const TABLE_TYPE: db_interfaces::clickhouse::tables::ClickhouseTableKind = #table_type;
                const CHILD_TABLES: &'static [#dbms] = #other_tables_needed;
                type ClickhouseDataType = #data_type;
            }

            ::db_interfaces::database_table!(#table_name, #data_type);
        };

        Ok(val)
    }
}

impl Parse for RemoteClickhouseTableParse {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let dbms: Ident = input
            .parse()
            .map_err(|e| syn::Error::new(e.span(), "Failed to parse table's dbms"))?;
        input.parse::<Token![,]>()?;

        let database_name: LitStr = input
            .parse()
            .map_err(|e| syn::Error::new(e.span(), "Failed to parse database name"))?;
        input.parse::<Token![,]>()?;

        let table_name: Ident = input
            .parse()
            .map_err(|e| syn::Error::new(e.span(), "Failed to parse table name"))?;

        let data_type = if input.peek2(syn::Ident) {
            input.parse::<Token![,]>()?;
            let dt_ident: Ident = input
                .parse()
                .map_err(|e| syn::Error::new(e.span(), "Failed to parse data type"))?;
            quote!(#dt_ident)
        } else {
            quote!(NoneType)
        };

        let mut other_tables_needed = Vec::new();
        let mut table_path = None;
        while input.peek(Token![,]) {
            input.parse::<Token![,]>()?;

            if input.peek(token::Paren) {
                let content;
                parenthesized!(content in input);
                let other_fields = content.parse_terminated(Expr::parse, Token![,])?;

                other_fields
                    .into_iter()
                    .for_each(|expr| other_tables_needed.push(expr));
            } else {
                let tp: LitStr = input
                    .parse()
                    .map_err(|e| syn::Error::new(e.span(), "Failed to parse table path"))?;
                table_path = Some(tp);
            }
        }

        if !input.is_empty() {
            return Err(syn::Error::new(input.span(), "There should be no values after the call function"));
        }

        Ok(Self { table_path, dbms, table_name: table_name.clone(), data_type, database_name, other_tables_needed })
    }
}

impl From<RemoteClickhouseTableParse> for ClickhouseTableParse {
    fn from(val: RemoteClickhouseTableParse) -> Self {
        ClickhouseTableParse { table_name: val.table_name, database_name: val.database_name.value() }
    }
}
