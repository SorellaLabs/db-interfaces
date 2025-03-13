use core::marker::PhantomData;

use clickhouse::Client;
use hyper_tls::HttpsConnector;
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client as HyperClient},
    rt::TokioExecutor
};

use super::{client::ClickhouseClient, dbms::ClickhouseDBMS};

#[derive(Debug, Clone)]
pub struct ClickhouseConfig {
    pub user:     String,
    pub password: String,
    pub url:      String,
    pub https:    bool,
    pub database: Option<String>
}

impl ClickhouseConfig {
    pub fn new(user: String, password: String, url: String, https: bool, database: Option<String>) -> Self {
        Self { user, password, url, https, database }
    }

    pub fn build<D: ClickhouseDBMS>(self) -> ClickhouseClient<D> {
        let mut client = if self.https {
            let connector = HttpsConnector::new();
            let https_client = HyperClient::builder(TokioExecutor::default())
                .pool_idle_timeout(std::time::Duration::from_secs(2))
                .build(connector);
            Client::with_http_client(https_client)
                .with_url(self.url)
                .with_user(self.user)
                .with_password(self.password)
        } else {
            let mut connector = HttpConnector::new();
            connector.set_keepalive(Some(std::time::Duration::from_secs(290)));
            let http_client = HyperClient::builder(TokioExecutor::default())
                .pool_idle_timeout(std::time::Duration::from_secs(2))
                .build(connector);
            Client::with_http_client(http_client)
                .with_url(self.url)
                .with_user(self.user)
                .with_password(self.password)
        };

        if let Some(db) = self.database {
            client = client.clone().with_database(db);
        }

        ClickhouseClient { client, _phantom: PhantomData }
    }

    #[cfg(feature = "test-utils")]
    pub fn build_testing_client<D: ClickhouseDBMS>(self) -> crate::clickhouse::test_utils::ClickhouseTestClient<D> {
        let mut client = if self.https {
            let https = HttpsConnector::new();
            let https_client = HyperClient::builder(TokioExecutor::default()).build(https);
            Client::with_http_client(https_client)
                .with_url(self.url)
                .with_user(self.user)
                .with_password(self.password)
        } else {
            Client::default()
                .with_url(self.url)
                .with_user(self.user)
                .with_password(self.password)
        };

        if let Some(db) = self.database {
            client = client.clone().with_database(db);
        }

        crate::clickhouse::test_utils::ClickhouseTestClient { client: ClickhouseClient { client, _phantom: PhantomData } }
    }
}
