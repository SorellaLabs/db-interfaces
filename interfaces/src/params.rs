#![allow(non_snake_case)]
use std::fmt::Debug;

use clickhouse::{query::Query, sql::Bind};
use serde::Serialize;

pub trait BindParameters: Send + Sync {
    fn bind_query(&self, query: Query) -> Query;
}

impl<T: BindParameters + Serialize> BindParameters for &T {
    fn bind_query(&self, query: Query) -> Query {
        query.bind(self)
    }
}

#[macro_export]
/// exported bind params
macro_rules! impl_bind_parameters {
    ($($T:ty),*) => {
        $(
            impl BindParameters for $T
            where
                Self: Bind + Serialize,
            {
                fn bind_query(&self, query: Query) -> Query {
                    query.bind(self)
                }
            }
        )*
    };
}

/// simple bind params
macro_rules! impl_simple_bind_parameters {
    ($($T:ty),*) => {
        $(
            impl BindParameters for $T
            where
                Self: Serialize
            {
                fn bind_query(&self, query: Query) -> Query {
                    query.bind(self)
                }
            }
        )*
    };
}

impl_simple_bind_parameters!(u8, u16, u32, u64, u128);
impl_simple_bind_parameters!(i8, i16, i32, i64, i128);
impl_simple_bind_parameters!(f32, f64);
impl_simple_bind_parameters!(String, str);

impl BindParameters for &str {
    fn bind_query(&self, query: Query) -> Query {
        query.bind(self)
    }
}

/// single generic bind params
macro_rules! impl_generic_bind_parameters {
    ($($T:ty),*) => {
        $(
            impl<I> BindParameters for $T
            where
                $T: Bind + Serialize,
                I: Bind + Serialize + Debug + Send + Sync,
            {
                fn bind_query(&self, query: Query) -> Query {
                    query.bind(self)
                }
            }
        )*
    };
}

impl_generic_bind_parameters!(Vec<I>, [I]);

/// tuple bind params
macro_rules! impl_tuple_bind_parameters {
    ($($T:ident,)*) => {
        impl<$($T: Bind + Serialize + Debug + Send + Sync),*> BindParameters for ($($T,)*) {
            #[allow(unused_mut)]
            fn bind_query(&self, query: Query) -> Query {
                let mut query = query;
                #[allow(unused_variables)]
                let ($($T,)*) = self;
                $(
                    query = query.bind($T);
                )*
                query
            }
        }
    };
}

impl_tuple_bind_parameters!();
impl_tuple_bind_parameters!(T1,);
impl_tuple_bind_parameters!(T1, T2,);
impl_tuple_bind_parameters!(T1, T2, T3,);
impl_tuple_bind_parameters!(T1, T2, T3, T4,);
impl_tuple_bind_parameters!(T1, T2, T3, T4, T5,);
impl_tuple_bind_parameters!(T1, T2, T3, T4, T5, T6,);
impl_tuple_bind_parameters!(T1, T2, T3, T4, T5, T6, T7,);
impl_tuple_bind_parameters!(T1, T2, T3, T4, T5, T6, T7, T8,);
impl_tuple_bind_parameters!(T1, T2, T3, T4, T5, T6, T7, T8, T9,);
impl_tuple_bind_parameters!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10,);
impl_tuple_bind_parameters!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11,);
impl_tuple_bind_parameters!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12,);
impl_tuple_bind_parameters!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13,);
impl_tuple_bind_parameters!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14,);
impl_tuple_bind_parameters!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15,);
impl_tuple_bind_parameters!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,);
