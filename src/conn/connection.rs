//! NSQ TCP connecting steps:
//!
//! ```plain
//! >>  V2
//! >>IDENTIFY
//! <<{
//!       "client_id": "nsq-rust",
//!       "hostname": "localhsot",
//!       "feature_negotiation": true,
//!       "heartbeat_interval": 5000,
//!       "output_buffer_size": 16 * 1024,
//!       "output_buffer_timeout": 250,
//!       "tls_v1": false,
//!       "snappy": false,
//!       "deflate": true,
//!       "deflate_level": 3,
//!       "sample_rate": 0,
//!       "user_agent": "nsq-rust/0.1.0",
//!       "msg_timeout": 100000
//!   }
//! //  upgrade to tls_v1 if `tls_v1` is `true`
//! //  upgrade to snappy or deflate if is `true`
//!
//! >>PUB/MPUB/DPUB
//! // Or
//! >>SUB topic channel
//! >>RDY 1
//!
//! ```
//! See [NSQ TCP Protocol Spec](https://nsq.io/clients/tcp_protocol_spec.html) to read more.

use std::io;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::net::SocketAddr;

use bytes::{Buf, BufMut};
use tokio::net::TcpStream;
use tokio::io::{AsyncRead, AsyncWrite};
use futures::{
    prelude::*,
    stream::{SplitSink, SplitStream},
};
use tokio_util::codec::{Framed, FramedParts};
use serde_derive::Deserialize;
#[cfg(feature = "tls")]
use native_tls::TlsConnector;
#[cfg(feature = "tls")]
use tokio_native_tls::TlsStream;
use log::{debug, error};

use crate::error::Error;
use crate::codec::{NsqCodec, NsqFramed, RawResponse};
use crate::command::Command;
use crate::conn::{Heartbeat, Response};
use super::codec::Codec;
use crate::config::{Config, TlsConfig};
use crate::producer::Producer;

pub struct Connection(Heartbeat);

trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin {}
impl AsyncReadWrite for TcpStream {}

#[cfg(feature = "tls")]
impl<S> AsyncReadWrite for TlsStream<S>
    where S: AsyncRead + AsyncWrite + Unpin {}

#[derive(Debug, Deserialize)]
struct IdentifyResponse {
    max_rdy_count: i64,
    auth_required: bool,
    deflate: bool,
    deflate_level: u32,
    max_deflate_level: u64,
    max_msg_timeout: u64,
    msg_timeout: u64,
    output_buffer_size: i64,
    output_buffer_timeout: u64,
    sample_rate: i32,
    snappy: bool,
    tls_v1: bool,
    version: String,
}

#[derive(Debug, Deserialize)]
pub struct AuthResponse {
    pub identify: String,
    pub identify_url: Option<String>,
    pub permission_count: i64,
}

impl Connection {
    pub async fn connect<A: Into<SocketAddr>>(addr: A, config: &Config) -> Result<Self, Error> {
        let (transport, _identify) = connect(addr, config).await?;
        Ok(Self(transport))
    }

    pub fn into_producer(self) -> Producer {
        Producer::from_connection(self)
    }

    /// Send `Command` to the server
    pub async fn send(&mut self, cmd: Command) -> Result<(), Error> {
        self.0.send(cmd).await
    }

    /// Receive from the server
    pub async fn receive(&mut self) -> Result<Response, Error> {
        match self.0.next().await {
            Some(r) => r,
            None => {
                Err(io::Error::from(io::ErrorKind::UnexpectedEof).into())
            }
        }
    }

    pub fn split(self) -> (ConnSink, ConnStream) {
        let (sink, stream) = self.0.split();
        (ConnSink(sink), ConnStream(stream))
    }
}

pub struct ConnSink(SplitSink<Heartbeat, Command>);
pub struct ConnStream(SplitStream<Heartbeat>);

impl Stream for ConnStream {
    type Item = Result<Response, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let s: Pin<&mut SplitStream<_>> = Pin::new(&mut self.0);
        s.poll_next(cx)
    }
}

impl Sink<Command> for ConnSink {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let s: Pin<&mut SplitSink<_, _>> = Pin::new(&mut self.0);
        s.poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Command) -> Result<(), Self::Error> {
        let s: Pin<&mut SplitSink<_, _>> = Pin::new(&mut self.0);
        s.start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let s: Pin<&mut SplitSink<_, _>> = Pin::new(&mut self.0);
        s.poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let s: Pin<&mut SplitSink<_, _>> = Pin::new(&mut self.0);
        s.poll_close(cx)
    }
}

async fn connect<A: Into<SocketAddr>>(addr: A, config: &Config) -> Result<(Heartbeat, IdentifyResponse), Error> {
    let tcp = TcpStream::connect(addr.into()).await?;
    let nsq_codec = NsqCodec::new(true);
    let mut framed = Framed::new(tcp, Codec::new(nsq_codec));
    framed.send(Command::Version).await?;

    let identify = config.identify()?;
    debug!("send identify: {:?}", &identify);
    framed.send(identify).await?;

    let response = if let Some(response) = framed.next().await {
        response
    } else {
        return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
    };

    debug!("identify response: {:?}", response);

    // TODO
    let identify: IdentifyResponse = match response? {
        // feature_negotiation false response Ok
        NsqFramed::Response(RawResponse::Ok) => {
            unreachable!();
        }

        // feature_negotiation true response Json object
        NsqFramed::Response(RawResponse::Json(value)) => {
            serde_json::from_value(value)?
        }

        // Reponse heartbeat
        NsqFramed::Response(RawResponse::Heartbeat) => {
            // Wrong response
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
        }
        NsqFramed::Response(RawResponse::CloseWait) => {
            // EOF
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
        }
        NsqFramed::Message(_) => {
            // Wrong response
            unreachable!();
        }
        NsqFramed::Error(e) => {
            // NSQ Error
            // TODO
            error!("IDENTIFY response error: {:?}", e);
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
        }
    };

    let mut framed = {
        if identify.tls_v1 {
            let domain = match config.tls_v1 {
                TlsConfig::Enabled{ref domain, ..} => domain.as_str(),
                _ => unreachable!(),
            };
            upgrade_tls(domain, framed).await?
        } else {
            let FramedParts { io, codec, read_buf, write_buf, .. } = framed.into_parts();
            let framed = Framed::new(BaseIo::Tcp(io), codec);
            let mut parts = framed.into_parts();
            parts.read_buf = read_buf;
            parts.write_buf = write_buf;
            Framed::from_parts(parts)
        }
    };

    if identify.snappy {
        framed.codec_mut().use_snappy();
    } else if identify.deflate {
        framed.codec_mut().use_deflate(identify.deflate_level);
    }

    if identify.auth_required {
        let auth_response = auth(config, &mut framed).await?;
        debug!("connection auth response: {:?}", auth_response);
    }

    // handle heartbeat
    Ok((Heartbeat::new(framed), identify))
}

async fn upgrade_tls(domain: &str, framed: Framed<TcpStream, Codec>) -> Result<Framed<BaseIo, Codec>, Error> {
    let connector = TlsConnector::new().unwrap();
    let connector = tokio_native_tls::TlsConnector::from(connector);
    let FramedParts { io, codec, read_buf, write_buf, .. } = framed.into_parts();
    let tls_socket = connector.connect(domain, io).await?;
    let framed = Framed::new(BaseIo::Tls(tls_socket), codec);
    let mut parts = framed.into_parts();
    parts.read_buf = read_buf;
    parts.write_buf = write_buf;
    let mut framed = Framed::from_parts(parts);
    if let Some(Ok(NsqFramed::Response(RawResponse::Ok))) = framed.next().await {
        Ok(framed)
    } else {
        Err(io::Error::from(io::ErrorKind::UnexpectedEof).into())
    }
}

async fn auth<T>(config: &Config, transport: &mut T) -> Result<AuthResponse, Error>
    where T: Sink<Command, Error = Error>,
          T: Stream<Item = Result<NsqFramed, Error>>,
          T: Unpin,
{
    let secret = if let Some(ref secret) = config.auth_secret {
        secret.clone()
    } else {
        return Err(Error::Auth("Required auth secret".into()));
    };

    let auth = Command::Auth(secret);
    transport.send(auth).await?;
    let response = if let Some(res) = transport.next().await {
        match res? {
            NsqFramed::Response(RawResponse::Json(value)) => {
                serde_json::from_value(value)?
            }
            NsqFramed::Error(e) => {
                return Err(e.into());
            }
            _ => {
                unreachable!();
            }
        }
    } else {
        return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
    };

    Ok(response)
}

pub(crate) enum BaseIo {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl AsyncRead for BaseIo {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize, tokio::io::Error>> {
        match *self {
            BaseIo::Tcp(ref mut s) => Pin::new(s).poll_read(cx, buf),
            BaseIo::Tls(ref mut s) => Pin::new(s).poll_read(cx, buf),
        }
    }

    unsafe fn prepare_uninitialized_buffer(&self, _buf: &mut [MaybeUninit<u8>]) -> bool {
        false
    }

    fn poll_read_buf<B: BufMut>(mut self: Pin<&mut Self>, cx: &mut Context, buf: &mut B) -> Poll<Result<usize, tokio::io::Error>>
    where
        Self: Sized,
    {
        match *self {
            BaseIo::Tcp(ref mut s) => Pin::new(s).poll_read_buf(cx, buf),
            BaseIo::Tls(ref mut s) => Pin::new(s).poll_read_buf(cx, buf),
        }
    }
}

impl AsyncWrite for BaseIo {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize, tokio::io::Error>> {
        match *self {
            BaseIo::Tcp(ref mut s) => Pin::new(s).poll_write(cx, buf),
            BaseIo::Tls(ref mut s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), tokio::io::Error>> {
        match *self {
            BaseIo::Tcp(ref mut s) => Pin::new(s).poll_flush(cx),
            BaseIo::Tls(ref mut s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), tokio::io::Error>> {
        match *self {
            BaseIo::Tcp(ref mut s) => Pin::new(s).poll_shutdown(cx),
            BaseIo::Tls(ref mut s) => Pin::new(s).poll_shutdown(cx),
        }
    }

    fn poll_write_buf<B: Buf>(mut self: Pin<&mut Self>, cx: &mut Context, buf: &mut B) -> Poll<Result<usize, tokio::io::Error>>
    where
        Self: Sized,
    {
        match *self {
            BaseIo::Tcp(ref mut s) => Pin::new(s).poll_write_buf(cx, buf),
            BaseIo::Tls(ref mut s) => Pin::new(s).poll_write_buf(cx, buf),
        }
    }
}
