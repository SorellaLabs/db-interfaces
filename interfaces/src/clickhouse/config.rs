use std::env;

use hyper::client::HttpConnector;
use hyper_tls::HttpsConnector;

pub struct ClickhouseConfig {
    pub user:     String,
    pub password: String,
    pub url:      String,
    pub https:    Option<HttpsConnector<HttpConnector<HttpConnector>>>,
    pub database: Option<String>
}

impl Default for ClickhouseConfig {
    fn default() -> Self {
        let url = format!(
            "{}:{}",
            &env::var("CLICKHOUSE_URL").expect("CLICKHOUSE_URL not found in .env"),
            &env::var("CLICKHOUSE_PORT").expect("CLICKHOUSE_PORT not found in .env")
        );

        let user = env::var("CLICKHOUSE_USER").expect("CLICKHOUSE_USER not found in .env");
        let password = env::var("CLICKHOUSE_PASS").expect("CLICKHOUSE_PASS not found in .env");
        let database = env::var("CLICKHOUSE_DATABASE").ok();

        Self { user, password, url, https: None, database }
    }
}

impl ClickhouseConfig {
    #[allow(dead_code)]
    fn new(
        user: String,
        password: String,
        url: String,
        https: Option<HttpsConnector<HttpConnector<HttpConnector>>>,
        database: Option<String>
    ) -> Self {
        Self { user, password, url, https, database }
    }
}
