/// formats a vec into a ? operator in a sql query
pub fn format_query_array<T: ToString>(vals: &[T], query: &str) -> String {
    let strings = vals.iter().map(|v| v.to_string()).collect::<Vec<_>>();

    let mut fmt_vec_str = strings.join("', '");
    fmt_vec_str.push('\'');

    let mut final_str = "'".to_string();
    final_str.push_str(&fmt_vec_str);

    query.replace('?', &final_str)
}
