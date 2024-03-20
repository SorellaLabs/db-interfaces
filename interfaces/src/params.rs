#![allow(non_snake_case)]
use std::fmt::Debug;

use clickhouse::{query::Query, sql::Bind};
use serde::Serialize;

pub trait BindParameters: Send + Sync + Debug {
    fn bind_query(&self, query: Query) -> Query;
}

macro_rules! impl_bind_parameters {
    ($($T:ty),*) => {
        $(
            impl BindParameters for $T
            where
                $T: Bind + Serialize,
            {
                fn bind_query(&self, query: Query) -> Query {
                    query.bind(self)
                }
            }
        )*
    };
}

impl_bind_parameters!(u8, u16, u32, u64, u128);
impl_bind_parameters!(i8, i16, i32, i64, i128);
impl_bind_parameters!(f32, f64);
impl_bind_parameters!(String);

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

impl_generic_bind_parameters!(Vec<I>);

impl<'a, I> BindParameters for &'a [I]
where
    I: Bind + Serialize + Debug + Send + Sync
{
    fn bind_query(&self, query: Query) -> Query {
        query.bind(self)
    }
}

impl<'a> BindParameters for &'a str {
    fn bind_query(&self, query: Query) -> Query {
        query.bind(self)
    }
}

/// For tuples
macro_rules! impl_bind_parameters_for_tuples {
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

impl_bind_parameters_for_tuples!();
impl_bind_parameters_for_tuples!(T1,);
impl_bind_parameters_for_tuples!(T1, T2,);
impl_bind_parameters_for_tuples!(T1, T2, T3,);
impl_bind_parameters_for_tuples!(T1, T2, T3, T4,);
impl_bind_parameters_for_tuples!(T1, T2, T3, T4, T5,);
impl_bind_parameters_for_tuples!(T1, T2, T3, T4, T5, T6,);
impl_bind_parameters_for_tuples!(T1, T2, T3, T4, T5, T6, T7,);
impl_bind_parameters_for_tuples!(T1, T2, T3, T4, T5, T6, T7, T8,);
impl_bind_parameters_for_tuples!(T1, T2, T3, T4, T5, T6, T7, T8, T9,);
impl_bind_parameters_for_tuples!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10,);
