use itertools::Itertools;
use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use syn::{parenthesized, parse::Parse, token, Expr, Ident, LitStr, Token};

use super::{table::ClickhouseTableParse, types::TableMeta};
use crate::clickhouse::types::add_underscore_and_lower;

pub(crate) fn remote_clickhouse_table(token_stream: TokenStream) -> syn::Result<TokenStream> {
    let parsed: RemoteClickhouseTableParse = syn::parse2(token_stream)?;
    let token_stream = parsed.to_token_stream()?;

    Ok(token_stream)
}

#[derive(Clone)]
pub(crate) struct RemoteClickhouseTableParse {
    pub(crate) table_path: Option<LitStr>,
    pub(crate) dbms: Ident,
    pub(crate) table_name: Ident,
    pub(crate) database_name: LitStr,
    pub(crate) data_type: Ident,
    pub(crate) other_tables_needed: Vec<Expr>,
}

impl RemoteClickhouseTableParse {
    fn to_token_stream(self) -> syn::Result<TokenStream> {
        let this = self.clone();
        //panic!("THIS: {:?}", this);
        let RemoteClickhouseTableParse {
            table_path,
            dbms,
            table_name,
            data_type,
            database_name,
            other_tables_needed,
        } = self;
        let other_tables_needed = other_tables_needed
            .into_iter()
            .map(|table| table.into_token_stream())
            .collect_vec();

        let (table_name_lowercase, enum_name, table_type, file_path, other_tables_needed) =
            if table_path.is_none() {
                let table_name_str = table_name.clone().to_string().replace("Clickhouse", "");
                let table_name_lowercase =
                    add_underscore_and_lower(&table_name_str.replace("Local", "")).to_lowercase();

                (
                    table_name_lowercase,
                    table_name.clone(),
                    quote!(ClickhouseTableType::None),
                    quote!(""),
                    quote!(&[]),
                )
            } else {
                let TableMeta {
                    table_name_lowercase,
                    enum_name,
                    table_type,
                    file_path,
                } = TableMeta::new(this.into(), table_path.as_ref())?;
                (
                    table_name_lowercase,
                    enum_name,
                    table_type,
                    file_path.into_token_stream(),
                    quote!(&[#(#dbms::#other_tables_needed),*]),
                )
            };

        let no_file_impls = if table_path.is_none() {
            quote! {
                    async fn create_table(_database: &ClickhouseClient<#dbms>) -> Result<(), ClickhouseError> {
                        panic!("Not Enabled - No File Path Given In Macro")
                    }

                    async fn create_test_table(_database: &ClickhouseClient<#dbms>, _random_seed: u32) -> Result<(), ClickhouseError> {
                        panic!("Not Enabled - No File Path Given In Macro")
                    }

                    // async fn truncate_test_table(_database: &ClickhouseClient<#dbms>) -> Result<(), ClickhouseError> {
                    //     panic!("Not Enabled - No File Path Given In Macro")
                    // }
            }
        } else {
            quote! {
                    async fn create_table(database: &ClickhouseClient<#dbms>) -> Result<(), ClickhouseError> {
                        let table_sql_path = Self::FILE_PATH;
                        let create_sql = std::fs::read_to_string(table_sql_path)?;
                        database.execute_remote(&create_sql, &()).await?;

                        // for table in Self::CHILD_TABLES {
                        //     table.create_table(database).await?;
                        // }

                        Ok(())
                    }

                    async fn create_test_table(database: &ClickhouseClient<#dbms>, random_seed: u32) -> Result<(), ClickhouseError> {
                        let table_sql_path = Self::FILE_PATH;
                        let mut create_sql = std::fs::read_to_string(table_sql_path)?;

                        let db = Self::DATABASE_NAME;
                        create_sql = create_sql.replace(&format!("{db}."), &format!("test_{db}."));
                        create_sql = create_sql.replace(&format!("'{db}'"), &format!("'test_{db}'"));

                        let table_type = Self::TABLE_TYPE;
                        if matches!(table_type, ClickhouseTableType::Distributed) {
                            database.execute_remote(&create_sql, &()).await?;
                        } else {
                            create_sql = create_sql.replace(&format!("/{}", Self::TABLE_NAME), &format!("/test{}/{}", random_seed, Self::TABLE_NAME));

                            database.execute_remote(&create_sql, &()).await?;
                        }

                        Ok(())
                    }

            }
        };

        let val = quote! {

            #[async_trait::async_trait]
            impl ClickhouseTable<#dbms> for #table_name {
                const DATABASE_NAME: &'static str = #database_name;
                const TABLE_NAME: &'static str = #table_name_lowercase;
                const FILE_PATH: &'static str = #file_path;
                const CHILD_TABLES: &'static [#dbms] = #other_tables_needed;
                const TABLE_TYPE: ClickhouseTableType = #table_type;
                const TABLE_ENUM: #dbms = #dbms::#enum_name;
                type ClickhouseDataType = #data_type;

                #no_file_impls
            }

            db_interfaces::tables::database_table!(#table_name, #data_type);
        };

        // panic!("TABLE NAME: {:?} -- TABLE TYPE: {:?}", table_name, table_type);

        Ok(val)
    }
}

impl Parse for RemoteClickhouseTableParse {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let dbms: Ident = input
            .parse()
            .map_err(|e| syn::Error::new(e.span(), "Failed to parse table name"))?;
        input.parse::<Token![,]>()?;

        let database_name: LitStr = input
            .parse()
            .map_err(|e| syn::Error::new(e.span(), "Failed to parse table name"))?;
        input.parse::<Token![,]>()?;

        let table_name: Ident = input
            .parse()
            .map_err(|e| syn::Error::new(e.span(), "Failed to parse table name"))?;
        input.parse::<Token![,]>()?;

        let data_type = input
            .parse()
            .map_err(|e| syn::Error::new(e.span(), "Failed to parse data type"))?;

        let mut other_tables_needed = Vec::new();
        let mut table_path = None;
        if input.peek(Token![,]) {
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
            table_name: table_name.clone(),
            data_type,
            database_name,
            other_tables_needed,
        })
    }
}

impl From<RemoteClickhouseTableParse> for ClickhouseTableParse {
    fn from(val: RemoteClickhouseTableParse) -> Self {
        ClickhouseTableParse {
            table_name: val.table_name,
            database_name: val.database_name.value(),
        }
    }
}
