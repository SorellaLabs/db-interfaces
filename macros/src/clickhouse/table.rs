use syn::{parenthesized, parse::Parse, Expr, Ident, Token};

use crate::clickhouse::remote_table::RemoteClickhouseTableParse;

#[derive(Clone)]
pub(crate) struct ClickhouseTableParse {
    /// required for all
    pub(crate) table_name:        Ident,
    /// required for all
    pub(crate) database_name:     String,
    /// required for all
    pub(crate) sub_database_name: Option<String>
}

impl ClickhouseTableParse {
    pub(crate) fn make_table_name_lowercase(&self) -> String {
        let mut table_name_str = self
            .table_name
            .to_string()
            .replace("Clickhouse", "")
            .replace("Local", "");
        table_name_str = add_underscore_and_lower(&table_name_str);

        if let Some(sub_db_name) = self.sub_database_name.as_ref() {
            format!("{}.`{}.{}`", self.database_name, sub_db_name, table_name_str).to_lowercase()
        } else {
            format!("{}.{}", self.database_name, table_name_str).to_lowercase()
        }
    }
}

impl Parse for ClickhouseTableParse {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let inner_tuple;
        parenthesized!(inner_tuple in input);

        let table_name: Ident = inner_tuple
            .parse()
            .map_err(|e| syn::Error::new(e.span(), "Failed to parse table name"))?;

        let mut other_tables_needed = Vec::new();
        while inner_tuple.peek(Token![,]) {
            inner_tuple.parse::<Token![,]>()?;

            if !inner_tuple.peek(Ident) {
                let content;
                parenthesized!(content in inner_tuple);
                let other_fields = content.parse_terminated(Expr::parse, Token![,])?;

                other_fields
                    .into_iter()
                    .for_each(|expr| other_tables_needed.push(expr));
            }
        }

        if !inner_tuple.is_empty() {
            return Err(syn::Error::new(inner_tuple.span(), "There should be no values after the call function"));
        }

        Ok(Self { table_name: table_name.clone(), database_name: Default::default(), sub_database_name: None })
    }
}

impl From<RemoteClickhouseTableParse> for ClickhouseTableParse {
    fn from(val: RemoteClickhouseTableParse) -> Self {
        ClickhouseTableParse {
            table_name:        val.table_name,
            database_name:     val.database_name.value(),
            sub_database_name: val.sub_database_name
        }
    }
}

pub(crate) fn add_underscore_and_lower(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars().enumerate().peekable();

    while let Some((idx, c)) = chars.next() {
        if c.is_uppercase() && chars.peek().is_some() && idx != 0 {
            result.push('_');
        }
        result.push(c);
    }

    result
}
