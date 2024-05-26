use proc_macro2::TokenStream;
use quote::quote;

pub(crate) enum ClickhouseTableKind {
    Distributed,
    Remote,
    RemoteSecure,
    ReplicatedMergeTree,
    ReplicatedAggregatingMergeTree,
    ReplicatedReplacingMergeTree,
    MergeTree,
    AggregatingMergeTree,
    ReplacingMergeTree,
    MaterializedView,
    Null
}

impl ClickhouseTableKind {
    pub(crate) fn get_table_type(file_path: &str) -> Self {
        let file_str = std::fs::read_to_string(file_path).unwrap_or_else(|_| panic!("Failed to read {}", file_path));
        if file_str.contains(&ClickhouseTableKind::Distributed.to_string()) {
            ClickhouseTableKind::Distributed
        } else if file_str.contains(&ClickhouseTableKind::RemoteSecure.to_string()) {
            ClickhouseTableKind::RemoteSecure
        } else if file_str.contains(&ClickhouseTableKind::Remote.to_string()) {
            ClickhouseTableKind::Remote
        } else if file_str.contains(&ClickhouseTableKind::ReplicatedMergeTree.to_string()) {
            ClickhouseTableKind::ReplicatedMergeTree
        } else if file_str.contains(&ClickhouseTableKind::ReplicatedAggregatingMergeTree.to_string()) {
            ClickhouseTableKind::ReplicatedAggregatingMergeTree
        } else if file_str.contains(&ClickhouseTableKind::ReplicatedReplacingMergeTree.to_string()) {
            ClickhouseTableKind::ReplicatedReplacingMergeTree
        } else if file_str.contains(&ClickhouseTableKind::ReplacingMergeTree.to_string()) {
            ClickhouseTableKind::ReplacingMergeTree
        } else if file_str.contains(&ClickhouseTableKind::AggregatingMergeTree.to_string()) {
            ClickhouseTableKind::AggregatingMergeTree
        } else if file_str.contains(&ClickhouseTableKind::MergeTree.to_string()) {
            ClickhouseTableKind::MergeTree
        } else if file_str.contains(&ClickhouseTableKind::MaterializedView.to_string()) {
            ClickhouseTableKind::MaterializedView
        } else if file_str.contains(&ClickhouseTableKind::Null.to_string()) {
            ClickhouseTableKind::Null
        } else {
            panic!("None of the table engines match!")
        }
    }
}

#[allow(clippy::to_string_trait_impl)]
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
            ClickhouseTableKind::MergeTree => "MergeTree",
            ClickhouseTableKind::AggregatingMergeTree => "AggregatingMergeTree",
            ClickhouseTableKind::ReplacingMergeTree => "ReplacingMergeTree",
            ClickhouseTableKind::MaterializedView => "CREATE MATERIALIZED VIEW",
            ClickhouseTableKind::Null => "Null"
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
            ClickhouseTableKind::MergeTree => {
                quote! { ::db_interfaces::clickhouse::tables::ClickhouseTableKind::MergeTree }
            }
            ClickhouseTableKind::AggregatingMergeTree => {
                quote! { ::db_interfaces::clickhouse::tables::ClickhouseTableKind::AggregatingMergeTree }
            }
            ClickhouseTableKind::ReplacingMergeTree => {
                quote! { ::db_interfaces::clickhouse::tables::ClickhouseTableKind::ReplacingMergeTree }
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
