use core::marker::PhantomData;

use clickhouse::Client;
use hyper_tls::HttpsConnector;

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
            let https = HttpsConnector::new();
            let https_client = hyper::Client::builder().build::<_, hyper::Body>(https);
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

        ClickhouseClient { client, _phantom: PhantomData }
    }

    #[cfg(feature = "test-utils")]
    pub fn build_testing_client<D: ClickhouseDBMS>(self) -> crate::clickhouse::test_utils::ClickhouseTestClient<D> {
        let mut client = if self.https {
            let https = HttpsConnector::new();
            let https_client = hyper::Client::builder().build::<_, hyper::Body>(https);
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
