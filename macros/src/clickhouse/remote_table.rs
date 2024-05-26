use itertools::Itertools;
use proc_macro2::{Span, TokenStream};
use quote::{quote, ToTokens};
use syn::{bracketed, parenthesized, parse::Parse, token, Expr, Ident, LitStr, Token};

use super::types::TableMeta;

pub(crate) fn remote_clickhouse_table(token_stream: TokenStream) -> syn::Result<TokenStream> {
    let parsed: RemoteClickhouseTableParse = syn::parse2(token_stream)?;
    let token_stream = parsed.to_token_stream()?;

    Ok(token_stream)
}

#[derive(Clone)]
pub(crate) struct RemoteClickhouseTableParse {
    pub(crate) table_path: Option<LitStr>,
    pub(crate) dbms: Ident,
    pub(crate) db_hierarchy: Vec<Ident>,
    pub(crate) data_type: TokenStream,
    pub(crate) other_tables_needed: Vec<Expr>,
}

impl RemoteClickhouseTableParse {
    pub(crate) fn db_table_type(&self) -> Ident {
        let concatted = self.db_hierarchy.iter().map(|d| d.to_string()).join("");
        Ident::new(&concatted, Span::call_site())
    }

    pub(crate) fn database_name(&self) -> Ident {
        self.db_hierarchy.first().unwrap().clone()
    }

    pub(crate) fn database_name_string(&self) -> String {
        self.database_name().to_string().to_lowercase()
    }

    pub(crate) fn table_name_string(&self) -> String {
        if self.db_hierarchy.len() == 2 {
            self.db_hierarchy.last().unwrap().to_string().to_lowercase()
        } else {
            let main_name = self.db_hierarchy.iter().skip(1).map(|d| d.to_string()).join(".").to_lowercase();
            format!("`{main_name}`")
        }

    }
}

impl RemoteClickhouseTableParse {
    fn to_token_stream(self) -> syn::Result<TokenStream> {
        let this = self.clone();
        let RemoteClickhouseTableParse {
            table_path,
            dbms,
            data_type,
            other_tables_needed,
            ..
        } = self;
        let other_tables_needed = other_tables_needed
            .into_iter()
            .map(|table| table.into_token_stream())
            .collect_vec();

        let TableMeta {
            table_name_str,
            db_table_type,
            database_name,
            table_type,
            file_path,
        } = TableMeta::new(this.into(), table_path.as_ref())?;

        let (table_name_str, db_table_type, table_type, file_path, other_tables_needed) = (
            table_name_str,
            db_table_type,
            table_type,
            file_path.into_token_stream(),
            quote!(&[#(#dbms::#other_tables_needed),*]),
        );

        let no_file_impls = if table_path.is_none() {
            quote! {
                fn create_table(_database: &::db_interfaces::clickhouse::client::ClickhouseClient<#dbms>) -> impl std::future::Future<Output = Result<(), ::db_interfaces::errors::DatabaseError>> {
                    unreachable!("Not Enabled - No File Path Given In Macro");
                    async {}
                }
            }
        } else {
            // quote! {
            //     async fn create_table(database: &::db_interfaces::clickhouse::client::ClickhouseClient<#dbms>) -> Result<(), ::db_interfaces::errors::DatabaseError> {
            //         let table_sql_path = <Self as ::db_interfaces::clickhouse::tables::ClickhouseTable<#dbms>>::FILE_PATH;
            //         let create_sql = std::fs::read_to_string(table_sql_path).map_err(|e| ::db_interfaces::clickhouse::errors::ClickhouseError::SqlFileReadError(e.to_string()))?;
            //         ::db_interfaces::Database::execute_remote(database, &create_sql, &()).await?;

            //         Ok(())
            //     }
            // }
            quote! ()
        };


        let no_file_impls_test = if table_path.is_none() {
            quote! {
                fn create_test_table(_database: &::db_interfaces::clickhouse::test_utils::ClickhouseTestClient<#dbms>, _random_seed: u32) -> impl std::future::Future<Output = Result<(), ::db_interfaces::errors::DatabaseError>> {
                    unreachable!("Not Enabled - No File Path Given In Macro");
                    async {}
                }
            }
        } else {
            // quote! {
            //     async fn create_test_table(database: &::db_interfaces::clickhouse::test_utils::ClickhouseTestClient<#dbms>, random_seed: u32) -> Result<(), ::db_interfaces::errors::DatabaseError> {
            //         let table_sql_path = <Self as ::db_interfaces::clickhouse::test_utils::ClickhouseTestTable<#dbms>>::FILE_PATH;
            //         let mut create_sql = std::fs::read_to_string(table_sql_path).map_err(|e| ::db_interfaces::clickhouse::errors::ClickhouseError::SqlFileReadError(e.to_string()))?;

            //         let db = <Self as ::db_interfaces::clickhouse::test_utils::ClickhouseTestTable<#dbms>>::DATABASE_NAME;
            //         create_sql = create_sql.replace(&format!("{db}."), &format!("test_{db}."));
            //         create_sql = create_sql.replace(&format!("'{db}'"), &format!("'test_{db}'"));

            //         let table_type = <Self as ::db_interfaces::clickhouse::test_utils::ClickhouseTestTable<#dbms>>::TABLE_TYPE;
            //         match table_type {
            //             db_interfaces::clickhouse::tables::ClickhouseTableKind::Distributed => ::db_interfaces::Database::execute_remote(database, &create_sql, &()).await?,
            //             _ => {
            //                 create_sql = create_sql.replace(&format!("/{}", <Self as ::db_interfaces::clickhouse::test_utils::ClickhouseTestTable<#dbms>>::TABLE_NAME), &format!("/test{}/{}", random_seed, <Self as ::db_interfaces::clickhouse::test_utils::ClickhouseTestTable<#dbms>>::TABLE_NAME));

            //                 ::db_interfaces::Database::execute_remote(&database.client, &create_sql, &()).await?;
            //             }
            //         }

            //         Ok(())
            //     }
            // }
            quote! ()
        };

        let val = quote! {
            impl ::db_interfaces::clickhouse::tables::ClickhouseTable<#dbms> for #db_table_type {
                const DATABASE_NAME: &'static str = #database_name;
                const TABLE_NAME: &'static str = #table_name_str;
                const FILE_PATH: &'static str = #file_path;
                const CHILD_TABLES: &'static [#dbms] = #other_tables_needed;
                const TABLE_TYPE: db_interfaces::clickhouse::tables::ClickhouseTableKind = #table_type;
                const TABLE_ENUM: #dbms = #dbms::#db_table_type;
                type ClickhouseDataType = #data_type;
            
                #no_file_impls
            }

            ::db_interfaces::database_table!(#db_table_type, #data_type);
        };


        let val_test = quote! {
            impl ::db_interfaces::clickhouse::test_utils::ClickhouseTestTable<#dbms> for #db_table_type {
                #no_file_impls_test
            }
        };

        Ok(quote!(#val  #val_test))
    }
}

impl Parse for RemoteClickhouseTableParse {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let dbms: Ident = input
            .parse()
            .map_err(|e| syn::Error::new(e.span(), "Failed to parse table's dbms"))?;
        input.parse::<Token![,]>()?;



        let db_table_path;
        bracketed!(db_table_path in input);

        let db_hierarchy = db_table_path.parse_terminated(Ident::parse, Token![,])?.into_iter().collect_vec();

        if db_hierarchy.len() < 2 {
            return Err(syn::Error::new(Span::call_site(), "database hierarchy must have at least 2 elements: [Database, Table]"));
        }

        let data_type = if input.peek2(syn::Ident) {
            input.parse::<Token![,]>()?;
            let dt_ident: Ident = input
                .parse()
                .map_err(|e| syn::Error::new(e.span(), "Failed to parse data type"))?;
            quote!(#dt_ident)
        } else {
            quote!(db_interfaces::clickhouse::types::NoneType)
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
            return Err(syn::Error::new(
                input.span(),
                "There should be no values after the call function",
            ));
        }

        Ok(Self {
            table_path,
            dbms,
            db_hierarchy,
            data_type,
            other_tables_needed,
        })
    }
}



