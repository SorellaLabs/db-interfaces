use syn::{parenthesized, parse::Parse, Expr, Ident, Token};

#[derive(Clone)]
pub(crate) struct ClickhouseTableParse {
    /// required for all
    pub(crate) table_name: Ident,
    /// required for all
    pub(crate) database_name: String,
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
            return Err(syn::Error::new(
                inner_tuple.span(),
                "There should be no values after the call function",
            ));
        }

        Ok(Self {
            table_name: table_name.clone(),
            database_name: Default::default(),
        })
    }
}
