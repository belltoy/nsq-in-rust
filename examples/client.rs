use std::net::SocketAddr;

use nsq_in_rust::{
    config::{
        Config,
        TlsConfig,
    },
    Connection,
    Error,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();
    let addr: SocketAddr = "127.0.0.1:4150".parse().unwrap();
    // let host = "localhost";

    let config: Config = Default::default();
    // config.tls_v1 = TlsConfig::Enabled {
    //     domain: host.to_string(),
    //     root_ca_file: "/tmp/root.ca".to_string(),
    //     cert_file: "/tmp/cert_file".to_string(),
    //     key_file: "/tmp/key_file".to_string(),
    //     insecure_skip_verify: true,
    // };
    // config.compress = Compress::Snappy;
    let _conn = Connection::connect(addr, &config).await?;
    // TODO more
    Ok(())
}
