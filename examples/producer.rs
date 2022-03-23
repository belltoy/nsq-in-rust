use std::net::SocketAddr;

use futures::prelude::*;
use nsq_in_rust::{
    config::{
        Config,
        Compress,
    },
    Connection,
    Error,
};
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();

    let config: Config = Config {
        // compress: Compress::Snappy,
        compress: Compress::Deflate{ level: 6 },
        ..Default::default()
    };
    let conn = Connection::connect("127.0.0.1:4150".parse::<SocketAddr>().unwrap(), &config).await?;
    let mut producer = conn.into_producer();
    let topic = "foo";

    info!("single publish");
    for i in 1..=10 {
        producer.publish(topic, format!("hello world with single publish: {}", i)).await?;
    }

    info!("multiple publish");
    let msgs = (1..=10).into_iter().map(|i| format!("hello world multiple publish: {}", i)).collect::<Vec<_>>();
    producer.multi_publish(topic, msgs).await?;

    info!("defereed publish");
    for i in 1..=10 {
        producer.deferred_publish(topic, 1000 + (i * 1000), format!("hello world with deferred publish: {}", i)).await?;
    }

    info!("use produer sink");
    let (sink, handler) = producer.into_sink(topic);
    let s = futures::stream::iter(1..=10).map(|i| Ok::<_, Error>(format!("hello world with sink: {}", i)));
    s.forward(sink).await?;
    if let Err(e) = handler.await {
        error!("sink response error: {:?}", e);
    }
    Ok(())
}
