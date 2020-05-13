use std::str::FromStr;
use std::net::IpAddr;

use futures::prelude::*;
use nsq_in_rust::{
    config::{
        Config,
        Compress,
    },
    Connection,
    Error,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    pretty_env_logger::init();

    let host = "localhost";
    let mut config: Config = Default::default();
    config.compress = Compress::Snappy;
    let conn = Connection::connect((IpAddr::from_str(host).unwrap(), 4150), &config).await?;
    let mut producer = conn.into_producer();
    let topic = "foo";

    log::info!("single publish");
    for i in 1..=10 {
        producer.publish(topic, format!("hello world with single publish: {}", i)).await?;
    }

    log::info!("multiple publish");
    let msgs = (1..=10).into_iter().map(|i| format!("hello world multiple publish: {}", i)).collect::<Vec<_>>();
    producer.multi_publish(topic, msgs).await?;

    log::info!("defereed publish");
    for i in 1..=10 {
        producer.deferred_publish(topic, 1000 + (i * 1000), format!("hello world with deferred publish: {}", i)).await?;
    }

    log::info!("use produer sink");
    let (sink, handler) = producer.into_sink(topic);
    let s = futures::stream::iter(1..=10).map(|i| Ok::<_, Error>(format!("hello world with sink: {}", i)));
    s.forward(sink).await?;
    if let Err(e) = handler.await {
        log::error!("sink response error: {:?}", e);
    }
    Ok(())
}
