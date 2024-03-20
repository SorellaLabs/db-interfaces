use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::LitStr;

use super::{table::ClickhouseTableParse, utils::find_file_path};

pub(crate) enum ClickhouseTableKind {
    Distributed,
    Remote,
    RemoteSecure,
    ReplicatedMergeTree,
    ReplicatedAggregatingMergeTree,
    ReplicatedReplacingMergeTree,
    MaterializedView,
    Null,
}

impl ClickhouseTableKind {
    pub(crate) fn get_table_type(file_path: &str) -> Self {
        let file_str = std::fs::read_to_string(file_path)
            .unwrap_or_else(|_| panic!("Failed to read {}", file_path));
        if file_str.contains(&ClickhouseTableKind::Distributed.to_string()) {
            ClickhouseTableKind::Distributed
        } else if file_str.contains(&ClickhouseTableKind::Remote.to_string()) {
            ClickhouseTableKind::Remote
        } else if file_str.contains(&ClickhouseTableKind::ReplicatedMergeTree.to_string()) {
            ClickhouseTableKind::ReplicatedMergeTree
        } else if file_str
            .contains(&ClickhouseTableKind::ReplicatedAggregatingMergeTree.to_string())
        {
            ClickhouseTableKind::ReplicatedAggregatingMergeTree
        } else if file_str.contains(&ClickhouseTableKind::ReplicatedReplacingMergeTree.to_string())
        {
            ClickhouseTableKind::ReplicatedReplacingMergeTree
        } else if file_str.contains(&ClickhouseTableKind::MaterializedView.to_string()) {
            ClickhouseTableKind::MaterializedView
        } else if file_str.contains(&ClickhouseTableKind::Null.to_string()) {
            ClickhouseTableKind::Null
        } else {
            panic!("None of the table engines match!")
        }
    }
}

impl ToString for ClickhouseTableKind {
    fn to_string(&self) -> String {
        let val: &'static str = self.into();
        val.to_string()
    }
}

impl From<&ClickhouseTableKind> for &'static str {
    fn from(val: &ClickhouseTableKind) -> Self {
        match val {
            ClickhouseTableKind::Distributed => "Distributed",
            ClickhouseTableKind::Remote => "remote",
            ClickhouseTableKind::RemoteSecure => "remoteSecure",
            ClickhouseTableKind::ReplicatedMergeTree => "ReplicatedMergeTree",
            ClickhouseTableKind::ReplicatedAggregatingMergeTree => "ReplicatedAggregatingMergeTree",
            ClickhouseTableKind::ReplicatedReplacingMergeTree => "ReplicatedReplacingMergeTree",
            ClickhouseTableKind::MaterializedView => "CREATE MATERIALIZED VIEW",
            ClickhouseTableKind::Null => "Null",
        }
    }
}

impl From<ClickhouseTableKind> for TokenStream {
    fn from(val: ClickhouseTableKind) -> Self {
        match val {
            ClickhouseTableKind::Distributed => {
                quote! { ::db_interfaces::clickhouse::tables::ClickhouseTableKind::Distributed }
            }
            ClickhouseTableKind::Remote => {
                quote! { ::db_interfaces::clickhouse::tables::ClickhouseTableKind::Remote }
            }
            ClickhouseTableKind::RemoteSecure => {
                quote! { ::db_interfaces::clickhouse::tables::ClickhouseTableKind::RemoteSecure }
            }
            ClickhouseTableKind::ReplicatedMergeTree => {
                quote! { ::db_interfaces::clickhouse::tables::ClickhouseTableKind::ReplicatedMergeTree }
            }
            ClickhouseTableKind::ReplicatedAggregatingMergeTree => {
                quote! { ::db_interfaces::clickhouse::tables::ClickhouseTableKind::ReplicatedAggregatingMergeTree }
            }
            ClickhouseTableKind::ReplicatedReplacingMergeTree => {
                quote! { ::db_interfaces::clickhouse::tables::ClickhouseTableKind::ReplicatedReplacingMergeTree }
            }
            ClickhouseTableKind::MaterializedView => {
                quote! { ::db_interfaces::clickhouse::tables::ClickhouseTableKind::MaterializedView }
            }
            ClickhouseTableKind::Null => {
                quote! { ::db_interfaces::clickhouse::tables::ClickhouseTableKind::Null }
            }
        }
    }
}

pub(crate) struct TableMeta {
    pub(crate) table_name_lowercase: String,
    pub(crate) enum_name: Ident,
    pub(crate) table_type: TokenStream,
    pub(crate) file_path: LitStr,
}

impl TableMeta {
    pub(crate) fn new(
        parsed: ClickhouseTableParse,
        table_path: Option<&LitStr>,
    ) -> syn::Result<Self> {
        let mut table_name_str = parsed.table_name.to_string();
        let enum_name = Ident::new(&table_name_str, parsed.table_name.span());

        table_name_str = table_name_str.replace("Clickhouse", "");
        let mut sql_file_name =
            add_underscore_and_lower(&table_name_str.replace("Local", "")).to_lowercase();
        let file_path_str = find_file_path(&sql_file_name, &parsed.database_name, table_path);
        let file_path = LitStr::new(&file_path_str, parsed.table_name.span());

        let table_type = ClickhouseTableKind::get_table_type(&file_path_str);
        if matches!(table_type, ClickhouseTableKind::Remote) {
            sql_file_name.push_str("_remote");
        }

        let this = Self {
            table_name_lowercase: sql_file_name,
            enum_name,
            table_type: table_type.into(),
            file_path,
        };

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
