use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::{bracketed, parenthesized, parse::Parse, Path, Token, Type, TypePath};

pub(crate) fn derive_all(item: TokenStream, _attr: TokenStream) -> syn::Result<TokenStream> {
    let DeriveAttributes { kinds, rest } = syn::parse2(item)?;

    Ok(quote! {
        #[derive(#(#kinds),*)]
        #rest
    })
}

struct DeriveAttributes {
    kinds: Vec<Type>,
    rest:  TokenStream
}

impl Parse for DeriveAttributes {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let mut kinds: Vec<Type> = vec![quote!(db_interfaces::Row), quote!(db_interfaces::ToSql), quote!(db_interfaces::FromSql)]
            .into_iter()
            .map(|kind| syn::parse2(kind).map(|p: Path| Type::Path(TypePath { qself: None, path: p })))
            .collect::<Result<Vec<_>, _>>()?;

        if input.peek(Token![#]) {
            // #
            input.parse::<Token![#]>()?;

            // []
            let bracket;
            bracketed!(bracket in input);

            // derive
            bracket.parse::<Ident>()?;

            // ()
            let paren;
            parenthesized!(paren in bracket);

            kinds.extend(paren.parse_terminated(Type::parse, Token![,])?)
        };

        let rest = input.parse()?;

        Ok(Self { kinds, rest })
    }
}
