use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::net::SocketAddr;
use futures::{
    ready,
    prelude::*,
    channel::oneshot::Receiver,
};

use tracing::debug;

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

    /// Ping causes the Producer to connect to it's configured nsqd (if not already
    /// connected) and send a `Nop` command, returning any error that might occur.
    ///
    /// TODO reconnect
    ///
    /// This method can be used to verify that a newly-created Producer instance is
    /// configured correctly, rather than relying on the lazy "connect on Publish"
    /// behavior of a Producer.
    pub async fn ping(&mut self) -> Result<(), Error> {
        self.conn.send(Command::Nop).await?;
        Ok(())
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
            debug!("read loop");
            while let Some(res) = stream.next().await {
                match res {
                    Ok(Response::Ok) => {
                        debug!("Response Ok");
                        continue;
                    }
                    Ok(Response::Msg(_)) => {
                        unreachable!();
                    }
                    Ok(Response::Err(e)) => {
                        debug!("Response err: {:?}", e);
                        let _ = tx.send(e.into());
                        break;
                    }
                    Err(e) => {
                        debug!("rx err: {:?}", e);
                        let _ = tx.send(e);
                        break;
                    }
                }
            }
            debug!("exit read loop");
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
