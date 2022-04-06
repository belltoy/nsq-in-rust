use std::net::ToSocketAddrs;
use std::sync::Arc;

use anyhow::Result;
use tracing::{warn, info};
use futures::{
    future::{
        FutureExt,
        TryFutureExt,
    },
    stream::{
        self,
        StreamExt,
        TryStreamExt,
    }
};
use tower::{
    Service, ServiceExt, MakeService,
    reconnect::Reconnect,
};
use tokio_tower::pipeline::client::Client;

use nsq_in_rust::{
    config::Config,
    Connection,
    producer::PublishProducer,
};
use nsq_in_rust::{Lookup, lookup::Producer as LookupProducer};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let nsq_config = Arc::new(Config::default());

    // create a lookup `Service`, which can be used to discover the nsqd brokers
    let mut lookup_service = tower::service_fn(|endpointer| {
        fetch_topics_match(endpointer, "smart")
    });
    let rst = lookup_service.call("http://127.0.0.1:4161").await?;
    info!("Matched topics info: {:?}", rst);

    // Make a service for nsqd brokers
    let mut mk_service = tower::service_fn(|(addr, config): (String, Arc<Config>)| async move {
        make_client(addr, &config).await
    }.boxed()); // `Reconnect` needs the future must be `Unpin`

    let target = ("127.0.0.1:4150".to_string(), Arc::clone(&nsq_config));
    let mut producer = mk_service.make_service(target.clone()).await?;

    // Produce by calling the `Service`, check `ready` and `call` publish
    let _ = producer.ready().await?
        .call(("smart".into(), "foooooo".as_bytes().to_vec())).await?;

    for i in 0..10 {
        let rsp = producer.ready().await?
            .call(("smart".into(), format!("foooooo {}", i).as_bytes().to_vec())).await?;
        info!("response: {:?}", rsp);
    }

    // Reconnect<
    //    ServiceFn<|(String, Arc<Config>)| -> impl Future<Output = Result<
    //                                                                     Client<PublishProducer, Error, (String, Vec<u8>)>,
    //                                                                     Error
    //                                                                    >
    //                                                    >
    //             >,
    //    (String, Arc<Config>)>
    // above is the type of `reconnectable`
    //
    // Here we use `Reconnect` to make a reconnectable client
    let mut reconnectable = Reconnect::with_connection(producer, mk_service, target);

    for i in 0..100 {
        loop {
            let rsp = reconnectable.ready()
                .await
                .map_err(|e| anyhow::anyhow!("NSQ connection ready error: {:?}", e))?
                .call(("smart".into(), format!("re foooooo {}", i).as_bytes().to_vec()))
                .await
                .map_err(|e| anyhow::anyhow!("NSQ producer publish error: {:?}", e));
            match rsp {
                Ok(rsp) => {
                    info!("reconnect producer pub {i} response: {:?}", rsp);
                    break;
                }
                Err(e) => {
                    warn!("error: {:?}, try again after reconnect", e);
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    Ok(())
}

type Request = (String, Vec<u8>);
type ProducerClient = Client<PublishProducer, anyhow::Error, Request>;
async fn make_client<S: ToSocketAddrs>(addr: S, nsq_config: &Config) -> Result<ProducerClient, anyhow::Error>
{
    let addr = addr.to_socket_addrs()?.next().ok_or_else(|| anyhow::anyhow!("no address"))?;
    let connection = Connection::connect(addr, &nsq_config).await?;
    let producer: PublishProducer = connection.into();

    // Create a new pipeline client for the PublishProducer
    let client = Client::<_, anyhow::Error, _>::new(producer);
    Ok(client)
}

async fn fetch_topics_match(endpoint: &str, pattern: &str) -> Result<Vec<LookupProducer>> {
    let lookup = Lookup::new(endpoint)?;
    let topics = lookup.topics().await?
        .topics.into_iter().filter(|topic| {
            // TODO: use regex or simple pattern
            topic == pattern
        });

    stream::iter(topics).then(|topic| async {
        lookup.lookup(topic).await
    })
    .map_ok(|lookup_result| {
        stream::iter(lookup_result.producers)
            .map(|p| Ok::<_, nsq_in_rust::Error>(p))
    })
    .try_flatten()
    .try_collect::<Vec<_>>()
    .map_err(From::from)
    .await
}
