use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::net::SocketAddr;
use futures::{
    ready,
    prelude::*,
    channel::oneshot::Receiver,
};

use crate::config::Config;
use crate::error::Error;
use crate::command::{Command, MessageBody};
use crate::conn::{Connection, Response, connection::ConnSink};

pub struct Producer {
    conn: Connection
}

pub struct SinkProducer {
    topic: String,
    sink: ConnSink,
    state: Option<Receiver<Error>>,
}


impl Producer {
    pub async fn connect<A: Into<SocketAddr>>(addr: A, config: &Config) -> Result<Self, Error> {
        let conn = Connection::connect(addr, config).await?;
        Ok(Self { conn })
    }

    pub(crate) fn from_connection(conn: Connection) -> Self {
        Self { conn }
    }

    /// Publish a message to a topic
    pub async fn publish(&mut self, topic: impl Into<String>, msg: impl Into<MessageBody>) -> Result<(), Error> {
        self.conn.send(Command::Pub(topic.into(), msg.into())).await?;
        self.response().await
    }

    /// Publish multiple messages to a topic (atomically):
    ///
    /// NOTE: available in nsqd v0.2.16+
    pub async fn multi_publish(&mut self, topic: impl Into<String>, msgs: Vec<impl Into<MessageBody>>) -> Result<(), Error> {
        let msgs = msgs.into_iter().map(|s| s.into()).collect();
        self.conn.send(Command::Mpub(topic.into(), msgs)).await?;
        self.response().await
    }

    /// Publish a deferred message to a topic:
    ///
    /// NOTE: available in nsqd v0.3.6+
    pub async fn deferred_publish(&mut self, topic: impl Into<String>, defer: u64, msg: impl Into<MessageBody>) -> Result<(), Error> {
        self.conn.send(Command::Dpub(topic.into(), defer, msg.into())).await?;
        self.response().await
    }

    async fn response(&mut self) -> Result<(), Error> {
        match self.conn.receive().await? {
            Response::Ok => Ok(()),
            Response::Err(e) => Err(e.into()),
            Response::Msg(_) => unreachable!(),
        }
    }

    pub fn into_sink(self, topic: impl Into<String>) -> (SinkProducer, tokio::task::JoinHandle<()>) {
        let (tx, rx) = futures::channel::oneshot::channel();
        let (sink, mut stream) = self.conn.split();
        let handler = tokio::spawn(async move {
            loop {
                log::debug!("read loop");
                match stream.next().await {
                    Some(Ok(Response::Ok)) => {
                        log::debug!("Response Ok");
                        continue;
                    }
                    Some(Ok(Response::Msg(_))) => {
                        unreachable!();
                    }
                    Some(Ok(Response::Err(e))) => {
                        log::debug!("Response err: {:?}", e);
                        let _ = tx.send(e.into());
                        break;
                    }
                    Some(Err(e)) => {
                        log::debug!("rx err: {:?}", e);
                        let _ = tx.send(e);
                        break;
                    }
                    None => {
                        log::debug!("tx dropped");
                        break;
                    }
                };
            }
            log::debug!("exit read loop");
        });

        (SinkProducer {
            topic: topic.into(),
            sink,
            state: Some(rx),
        }, handler)
    }
}

impl SinkProducer {
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        match self.state {
            None => return Poll::Ready(Err(io::Error::from(io::ErrorKind::UnexpectedEof).into())),
            Some(ref mut fut) => {
                match Pin::new(fut).poll(cx) {
                    Poll::Ready(Ok(e)) => Poll::Ready(Err(e)),
                    Poll::Ready(Err(_)) => return Poll::Ready(Err(io::Error::from(io::ErrorKind::UnexpectedEof).into())),
                    Poll::Pending => Poll::Ready(Ok(())),
                }
            }
        }
    }
}

impl<S> Sink<S> for SinkProducer
where S: Into<MessageBody>,
{
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll(cx))?;
        Pin::new(&mut self.sink).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: S) -> Result<(), Self::Error> {
        let topic = self.topic.clone();
        let item = Command::Pub(topic, item.into());
        Pin::new(&mut self.sink).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll(cx))?;
        Pin::new(&mut self.sink).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll(cx))?;
        Pin::new(&mut self.sink).poll_close(cx)
    }
}
