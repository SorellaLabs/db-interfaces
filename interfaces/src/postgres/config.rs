use core::marker::PhantomData;

use hyper_tls::HttpsConnector;
use sqlx::{PgPool, Pool};

use super::{client::PostgresClient, dbms::PostgresDBMS};

#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub host:     String,
    pub port:     u16,
    pub user:     String,
    pub password: String,
    pub url:      String,
    pub https:    bool,
    pub database: Option<String>
}

impl PostgresConfig {
    pub fn new(host: String, port: u16, user: String, password: String, url: String, https: bool, database: Option<String>) -> Self {
        Self { host, port, user, password, url, https, database }
    }

    pub async fn build<D: PostgresDBMS>(self) -> PostgresClient<D> {
        let options = PgConnectOptions::new()
            .host(self.host)
            .port(self.port)
            .username(self.user)
            .password(self.password)
            .ssl_mode(PgSslMode::Require)
            .connect()
            .await?;
        let pool = PgPool::connect_with(&options).await?;

        PostgresClient { pool, _phantom: PhantomData }
    }

    // #[cfg(feature = "test-utils")]
    // pub fn build_testing_client<D: PostgresDBMS>(self) -> crate::postgres::test_utils::PostgresTestClient<D> {
    //     let mut client = if self.https {
    //         let https = HttpsConnector::new();
    //         let https_client = hyper::Client::builder().build::<_, hyper::Body>(https);
    //         Client::with_http_client(https_client)
    //             .with_url(self.url)
    //             .with_user(self.user)
    //             .with_password(self.password)
    //     } else {
    //         Client::default()
    //             .with_url(self.url)
    //             .with_user(self.user)
    //             .with_password(self.password)
    //     };

    //     if let Some(db) = self.database {
    //         client = client.clone().with_database(db);
    //     }

    //     crate::postgres::test_utils::PostgresTestClient { client: PostgresClient { client, _phantom: PhantomData } }
    // }
}
