use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::LitStr;

use super::{table::ClickhouseTableParse, utils::find_file_path};

pub(crate) enum ClickhouseTableType {
    Distributed,
    Remote,
    ReplicatedMergeTree,
    ReplicatedAggregatingMergeTree,
    ReplicatedReplacingMergeTree,
    MaterializedView,
    Null
}

impl ClickhouseTableType {
    pub(crate) fn get_table_type(file_path: &str) -> Self {
        let file_str = std::fs::read_to_string(file_path).unwrap_or_else(|_| panic!("Failed to read {}", file_path));
        if file_str.contains(&ClickhouseTableType::Distributed.to_string()) {
            ClickhouseTableType::Distributed
        } else if file_str.contains(&ClickhouseTableType::Remote.to_string()) {
            ClickhouseTableType::Remote
        } else if file_str.contains(&ClickhouseTableType::ReplicatedMergeTree.to_string()) {
            ClickhouseTableType::ReplicatedMergeTree
        } else if file_str.contains(&ClickhouseTableType::ReplicatedAggregatingMergeTree.to_string()) {
            ClickhouseTableType::ReplicatedAggregatingMergeTree
        } else if file_str.contains(&ClickhouseTableType::ReplicatedReplacingMergeTree.to_string()) {
            ClickhouseTableType::ReplicatedReplacingMergeTree
        } else if file_str.contains(&ClickhouseTableType::MaterializedView.to_string()) {
            ClickhouseTableType::MaterializedView
        } else if file_str.contains(&ClickhouseTableType::Null.to_string()) {
            ClickhouseTableType::Null
        } else {
            panic!("None of the table engines match!")
        }
    }
}

impl ToString for ClickhouseTableType {
    fn to_string(&self) -> String {
        let val: &'static str = self.into();
        val.to_string()
    }
}

impl From<&ClickhouseTableType> for &'static str {
    fn from(val: &ClickhouseTableType) -> Self {
        match val {
            ClickhouseTableType::Distributed => "Distributed",
            ClickhouseTableType::Remote => "remoteSecure",
            ClickhouseTableType::ReplicatedMergeTree => "ReplicatedMergeTree",
            ClickhouseTableType::ReplicatedAggregatingMergeTree => "ReplicatedAggregatingMergeTree",
            ClickhouseTableType::ReplicatedReplacingMergeTree => "ReplicatedReplacingMergeTree",
            ClickhouseTableType::MaterializedView => "CREATE MATERIALIZED VIEW",
            ClickhouseTableType::Null => "Null"
        }
    }
}

impl From<ClickhouseTableType> for TokenStream {
    fn from(val: ClickhouseTableType) -> Self {
        match val {
            ClickhouseTableType::Distributed => quote! { ClickhouseTableType::Distributed },
            ClickhouseTableType::Remote => quote! { ClickhouseTableType::Remote },
            ClickhouseTableType::ReplicatedMergeTree => quote! { ClickhouseTableType::ReplicatedMergeTree },
            ClickhouseTableType::ReplicatedAggregatingMergeTree => {
                quote! { ClickhouseTableType::ReplicatedAggregatingMergeTree }
            }
            ClickhouseTableType::ReplicatedReplacingMergeTree => {
                quote! { ClickhouseTableType::ReplicatedReplacingMergeTree }
            }
            ClickhouseTableType::MaterializedView => quote! { ClickhouseTableType::MaterializedView },
            ClickhouseTableType::Null => quote! { ClickhouseTableType::Null }
        }
    }
}

pub(crate) struct TableMeta {
    pub(crate) table_name_lowercase: String,
    pub(crate) enum_name:            Ident,
    pub(crate) table_type:           TokenStream,
    pub(crate) file_path:            LitStr
}

impl TableMeta {
    pub(crate) fn new(parsed: ClickhouseTableParse, table_path: Option<&LitStr>) -> syn::Result<Self> {
        let mut table_name_str = parsed.table_name.to_string();
        let enum_name = Ident::new(&table_name_str, parsed.table_name.span());

        table_name_str = table_name_str.replace("Clickhouse", "");
        let mut sql_file_name = add_underscore_and_lower(&table_name_str.replace("Local", "")).to_lowercase();
        let file_path_str = find_file_path(&sql_file_name, &parsed.database_name, table_path);
        let file_path = LitStr::new(&file_path_str, parsed.table_name.span());

        let table_type = ClickhouseTableType::get_table_type(&file_path_str);
        if matches!(table_type, ClickhouseTableType::Remote) {
            sql_file_name.push_str("_remote");
        }

        let this = Self { table_name_lowercase: sql_file_name, enum_name, table_type: table_type.into(), file_path };

        Ok(this)
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
