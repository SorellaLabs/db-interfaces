use crate::DatabaseTable;

use super::{dbms::ClickhouseDBMS, tables::ClickhouseTable};

/// formats a vec into a ? operator in a sql query
pub fn format_query_array<T: ToString>(vals: &[T], query: &str) -> String {
    let strings = vals.iter().map(|v| v.to_string()).collect::<Vec<_>>();

    let mut fmt_vec_str = strings.join("', '");
    fmt_vec_str.push('\'');

    let mut final_str = "'".to_string();
    final_str.push_str(&fmt_vec_str);

    query.replace('?', &final_str)
}

pub fn replace_test_str(str: String) -> String {
    let mut str = str.replace("local_tables.", "test_local_tables.");
    str = str.replace("'local_tables'", "'test_local_tables'");

    str = str.replace("views.", "test_views.");
    str = str.replace("'views'", "'test_views'");

    str = str.replace("cex.", "test_cex.");
    str = str.replace("'cex'", "'test_cex'");

    str = str.replace("mev.", "test_mev.");
    str = str.replace("'mev'", "'test_mev'");

    str = str.replace("ethereum.", "test_ethereum.");
    str = str.replace("'ethereum'", "'test_ethereum'");

    str = str.replace("eth_analytics.", "test_eth_analytics.");
    str = str.replace("'eth_analytics'", "'test_eth_analytics'");

    str
}

pub trait ClickhouseUtils<T: ClickhouseTable<D> + ?Sized, D: ClickhouseDBMS + 'static> {
    fn database_name() -> String;

    fn test_database_name() -> String;

    fn full_name() -> String;

    fn full_test_name() -> String;
}
