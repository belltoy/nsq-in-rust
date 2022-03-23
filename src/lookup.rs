use std::time::Duration;

use crate::error::{UrlParseError, Error, Result};
use serde::Deserialize;
use reqwest::Url;

pub static DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Lookup client
pub struct Lookup {
    http_addr: Url,
    client: reqwest::Client,
}

#[derive(Debug, Deserialize)]
pub struct LookupResponse {
    pub channels: Vec<String>,
    pub producers: Vec<Producer>,
}

#[derive(Debug, Deserialize)]
pub struct Producer {
    pub broadcast_address: String,
    pub hostname: String,
    pub remote_address: String,
    pub tcp_port: u16,
    pub http_port: u16,
    pub version: String,
}

#[derive(Debug, Deserialize)]
pub struct TopicsResponse {
    pub topics: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ChannelsResponse {
    pub channels: Vec<String>,
}
#[derive(Debug, Deserialize)]
pub struct NodesResponse {
    pub producers: Vec<Node>,
}

#[derive(Debug, Deserialize)]
pub struct Node {
    pub broadcast_address: String,
    pub hostname: String,
    pub remote_address: String,
    pub tcp_port: u16,
    pub http_port: u16,
    pub version: String,
    pub tombstones: Vec<bool>,
    pub topics: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct InfoResponse {
    pub version: String,
}

impl Lookup {

    /// Create a new lookup client from a given http address.
    ///
    /// The `url` must be a valid http address, which means it must start with `http://` or `https://`.
    pub fn new<I: TryInto<Url>>(url: I) -> std::result::Result<Self, UrlParseError>
        where UrlParseError: From<<I as TryInto<Url>>::Error>
    {
        let client = reqwest::Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .build().expect("Build HTTP Client error");
        let url = url.try_into()?;
        Ok(Self {
            http_addr: url,
            client,
        })
    }

    /// Returns a list of producers for a topic
    pub async fn lookup(&self, topic: impl AsRef<str>) -> Result<LookupResponse> {
        self.client.get(self.url("/lookup")?)
            .query(&[("topic", topic.as_ref())])
            .send().await?
            .json().await
            .map_err(From::from)
    }

    /// Returns a list of all known topics
    pub async fn topics(&self) -> Result<TopicsResponse> {
        self.client.get(self.url("/topics")?)
            .send().await?
            .json().await
            .map_err(From::from)
    }

    /// Returns a list of all known channels of a topic
    pub async fn channels(&self, topic: impl AsRef<str>) -> Result<ChannelsResponse> {
        self.client.get(self.url("/channels")?)
            .query(&[("topic", topic.as_ref())])
            .send().await?
            .json().await
            .map_err(From::from)
    }

    /// Returns a list of all known `nsqd`
    pub async fn nodes(&self) -> Result<NodesResponse> {
        self.client.get(self.url("/nodes")?)
            .send().await?
            .json().await
            .map_err(From::from)
    }

    /// Add a topic to nsqlookupd’s registry
    pub async fn create_topic(&self, topic: impl AsRef<str>) -> Result<()> {
        let _ = self.client.post(self.url("/topic/create")?)
            .query(&[("topic", topic.as_ref())])
            .send().await?;
        Ok(())
    }

    /// Deletes an existing topic
    pub async fn delete_topic(&self, topic: impl AsRef<str>) -> Result<()> {
        let _ = self.client.post(self.url("/topic/delete")?)
            .query(&[("topic", topic.as_ref())])
            .send().await?;
        Ok(())
    }

    /// Add a channel to nsqlookupd’s registry
    pub async fn create_channel(&self, topic: impl AsRef<str>, channel: impl AsRef<str>) -> Result<()> {
        let _ = self.client.post(self.url("/topic/create")?)
            .query(&[
                ("topic", topic.as_ref()),
                ("channel", channel.as_ref())
            ])
            .send().await?;
        Ok(())
    }

    /// Deletes an existing channel of an existing topic
    pub async fn delete_channel(&self, topic: impl AsRef<str>, channel: impl AsRef<str>) -> Result<()> {
        let _ = self.client.post(self.url("/topic/delete")?)
            .query(&[
                ("topic", topic.as_ref()),
                ("channel", channel.as_ref())
            ])
            .send().await?;
        Ok(())
    }

    /// Tombstones a specific producer of an existing topic.
    ///
    /// See [deletion and tombstones](https://nsq.io/components/nsqlookupd.html#deletion_tombstones).
    pub async fn tombstone(&self, topic: impl AsRef<str>, node: &Node) -> Result<()> {
        let _ = self.client.post(self.url("/topic/tombstone")?)
            .query(&[
                ("topic", topic.as_ref()),
                ("node", format!("{}:{}", node.broadcast_address, node.http_port).as_ref())
            ])
            .send().await?;
        Ok(())
    }

    /// Monitoring endpoint, should return OK
    pub async fn ping(&self) -> Result<()> {
        let resp = self.client.get(self.url("/ping")?).send().await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(Error::UnknownError("Unknown ping error from lookupd".into()))
        }
    }

    /// Returns version information
    pub async fn info(&self) -> Result<InfoResponse> {
        self.client.get(self.url("/info")?)
            .send().await?
            .json().await
            .map_err(From::from)
    }

    fn url(&self, endpoint: &str) -> std::result::Result<Url, UrlParseError> {
        self.http_addr.join(endpoint)
    }
}
