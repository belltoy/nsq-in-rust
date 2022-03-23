use nsq_in_rust::Lookup;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let lookup = Lookup::new("http://127.0.0.1:4161")?;

    let topics = lookup.topics().await?;
    println!("{:#?}", topics);

    let lookup_resp = lookup.lookup("foo").await?;
    println!("{:#?}", lookup_resp);

    let channels = lookup.channels("foo").await?;
    println!("{:#?}", channels);

    let nodes = lookup.nodes().await?;
    println!("{:#?}", nodes);

    lookup.tombstone("foo", &nodes.producers[0]).await?;

    assert!(lookup.ping().await.is_ok());

    let info = lookup.info().await?;
    println!("{:#?}", info);

    Ok(())
}
