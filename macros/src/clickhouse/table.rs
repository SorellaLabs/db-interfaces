use proc_macro2::{Span, TokenStream};
use syn::{Ident, LitStr};

use super::types::ClickhouseTableKind;
use crate::clickhouse::{remote_table::RemoteClickhouseTableParse, utils::find_file_path};

pub(crate) struct TableMeta {
    pub(crate) table_name_str: String,
    pub(crate) db_table_type:  Ident,
    pub(crate) database_name:  String,
    pub(crate) table_type:     TokenStream,
    pub(crate) file_path:      LitStr
}

impl TableMeta {
    pub(crate) fn new(parsed: RemoteClickhouseTableParse, table_path: Option<&LitStr>) -> syn::Result<Self> {
        let db_table_type = parsed.db_table_type();

        let table_name_str = parsed.table_name_string();
        let database_name = parsed.database_name_string();
        let file_path_str = find_file_path(&table_name_str, &database_name, table_path);
        let file_path = LitStr::new(&file_path_str, Span::call_site());

        let table_type = ClickhouseTableKind::get_table_type(&file_path_str);

        let this = Self { database_name, table_name_str, db_table_type, table_type: table_type.into(), file_path };

        Ok(this)
    }
}
