use alloy_primitives::{Address, FixedBytes};

use crate::params::BindParameters;
use clickhouse::query::Query;

impl BindParameters for Address {
    fn bind_query(&self, query: Query) -> Query {
        format!("{:?}", self).bind_query(query)
    }
}

impl<const N: usize> BindParameters for FixedBytes<N> {
    fn bind_query(&self, query: Query) -> Query {
        format!("{:#x}", self).bind_query(query)
    }
}
