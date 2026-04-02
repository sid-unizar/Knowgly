use reqwest::blocking::Client;
use sophia::sparql_client::SparqlClient;
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::Duration;

pub struct SPARQLEndpoint;

static SPARQL_CLIENT: OnceLock<SparqlClient> = OnceLock::new();

impl SPARQLEndpoint {
    pub fn new(endpoint: &str) {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(600))
            .pool_max_idle_per_host(thread::available_parallelism().unwrap().get())
            .build()
            .expect("Failed to build HTTP client");

        let client = SparqlClient::new(endpoint)
            .with_client(http_client)
            .with_accept("application/n-triples");

        let _ = SPARQL_CLIENT.set(client);
    }

    pub fn access() -> &'static SparqlClient {
        SPARQL_CLIENT
            .get()
            .expect("SparqlClient not initialized. Call init(endpoint) first.")
    }
}
