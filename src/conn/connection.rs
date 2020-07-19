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
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use tokio::net::TcpStream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_snappy::SnappyIO;
use futures::{
    prelude::*,
    stream::{SplitSink, SplitStream},
};
use tokio_util::codec::Framed;
use serde::Deserialize;
#[cfg(feature = "tls-native")]
use tokio_native_tls::{TlsConnector, TlsStream, native_tls};
#[cfg(feature = "tls-tokio")]
use tokio_rustls::{
    rustls::RootCertStore,
    rustls::client::{
        ClientConfig,
        ServerName,
    },
    TlsConnector, client::TlsStream,
};
use tracing::{trace, debug, error};

use crate::error::Error;
use crate::codec::{Decoder, Encoder, NsqCodec, NsqFramed, RawResponse};
use crate::command::Command;
use crate::conn::{Heartbeat, Response};
use crate::config::{Config, TlsConfig};
use crate::producer::Producer;
use crate::conn::deflate::DeflateStream;

pub struct Connection(Heartbeat);

pub type ConnSink = SplitSink<Heartbeat, Command>;
pub type ConnStream = SplitStream<Heartbeat>;

pub trait AsyncRW: AsyncRead + AsyncWrite {}
impl<T> AsyncRW for T where T: AsyncRead + AsyncWrite {}

// #[cfg(feature = "tls-native,tls-tokio")]
// impl<S> AsyncReadWrite for TlsStream<S>
//     where S: AsyncRead + AsyncWrite + Unpin {}

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
        self.0.split()
    }
}

async fn connect<A: Into<SocketAddr>>(addr: A, config: &Config) -> Result<(Heartbeat, IdentifyResponse), Error> {
    let mut tcp = TcpStream::connect(addr.into()).await?;
    let mut nsq_codec = NsqCodec::new(true);

    let mut write_buf = BytesMut::new();
    nsq_codec.encode(Command::Version, &mut write_buf)?;
    let identify = config.identify()?;
    trace!("send identify: {:?}", &identify);
    nsq_codec.encode(identify, &mut write_buf)?;

    tcp.write_all(&write_buf.split()[..]).await?;
    let response = read_response(&mut tcp, &mut nsq_codec).await?;
    trace!("identify response: {:?}", response);

    // TODO
    let identify: IdentifyResponse = match response {
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
    let socket = tcp;
    // let socket = BaseIo::Tcp(tcp);
    // let codec = Codec::new(nsq_codec);
    // let mut framed = Framed::new(BaseIo::Tcp(tcp), codec);

    // let socket = {
    //     if identify.tls_v1 {
    //         let tls_config = config.tls_v1.as_ref().unwrap();
    //         let domain = match config.tls_v1 {
    //             Some(TlsConfig{ref domain, ..}) => domain.as_str(),
    //             _ => unreachable!(),
    //         };
    //         let tls_stream = upgrade_tls(domain, tcp, tls_config, &mut nsq_codec).await?;
    //         BaseIo::Tls(tls_stream)
    //     } else {
    //         BaseIo::Tcp(tcp)
    //         // let FramedParts { io, codec, read_buf, write_buf, .. } = framed.into_parts();
    //         // let framed = Framed::new(BaseIo::Tcp(io), codec);
    //         // let mut parts = framed.into_parts();
    //         // parts.read_buf = read_buf;
    //         // parts.write_buf = write_buf;
    //         // Framed::from_parts(parts)
    //     }
    // };

    let boxed_stream: Box<dyn AsyncRW + Send + Unpin> = if identify.snappy {
        let mut snappy_stream = upgrade_snappy(socket);
        if let NsqFramed::Response(RawResponse::Ok) = read_response(&mut snappy_stream, &mut nsq_codec).await? {
            Box::new(snappy_stream)
        } else {
            return Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "compression negotiation expected OK",
            )));
        }
    } else if identify.deflate {
        let (read_half, write_half) = tokio::io::split(socket);
        let mut deflate_stream = upgrade_deflate(read_half, write_half, identify.deflate_level);
        if let NsqFramed::Response(RawResponse::Ok) = read_response(&mut deflate_stream, &mut nsq_codec).await? {
            Box::new(deflate_stream)
        } else {
            return Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "compression negotiation expected OK",
            )));
        }
    } else {
        Box::new(socket)
    };
    let mut framed = Framed::new(boxed_stream, nsq_codec);

    if identify.auth_required {
        let auth_response = auth(config, &mut framed).await?;
        debug!("connection auth response: {:?}", auth_response);
    }

    // handle heartbeat
    Ok((Heartbeat::new(framed), identify))
}

#[cfg(feature = "tls-tokio")]
async fn upgrade_tls(domain: &str, inner: TcpStream, tls_config: &TlsConfig, nsq_codec: &mut NsqCodec)
    -> Result<TlsStream<TcpStream>, Error>
{
    // todo!()
    // TODO from config
    let root_certs = RootCertStore { roots: vec![] };
    let client_config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_certs)
        .with_no_client_auth();
    let client_config = Arc::new(client_config);
    let connector = TlsConnector::from(client_config);
    // TODO FIXME data from peer may have already read in the buffer
    let mut tls_socket = connector.connect(ServerName::try_from(tls_config.domain.as_str())?, inner).await?;
    if let NsqFramed::Response(RawResponse::Ok) = read_response(&mut tls_socket, nsq_codec).await? {
        // Ok(Box::new(tls_socket))
        Ok(tls_socket)
    } else {
        Err(io::Error::from(io::ErrorKind::UnexpectedEof).into())
    }
}

#[cfg(feature = "tls-native")]
async fn upgrade_tls<T>(domain: &str, inner: T, tls_config: &TlsConfig, nsq_codec: &mut NsqCodec)
    // -> Result<TlsStream, Error>
    -> Result<Box<dyn AsyncRead + AsyncWrite + Unpin>, Error>
    where T: AsyncRead + AsyncWrite + Unpin,
{
    let connector = native_tls::TlsConnector::new().unwrap();
    let connector = TlsConnector::from(connector);
    // TODO FIXME data from peer may have already read in the buffer
    let tls_socket = connector.connect(domain, inner).await?;
    if let NsqFramed::Response(RawResponse::ok) = read_response(&mut tls_socket, nsq_codec).await?{
        Ok(Box::new(tls_socket))
    } else {
        Err(io::Error::from(io::ErrorKind::UnexpectedEof).into())
    }
}

fn upgrade_deflate<R, W>(reader: R, writer: W, level: u32) -> DeflateStream<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    DeflateStream::new(reader, writer, level)
}

fn upgrade_snappy<T>(inner: T) -> SnappyIO<T>
    where T: AsyncRead + AsyncWrite + Unpin,
{
    SnappyIO::new(inner)
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

async fn read_response<T>(socket: &mut T, nsq_codec: &mut NsqCodec) -> Result<NsqFramed, Error>
where T: AsyncRead + Unpin,
{
    let mut read_buf = BytesMut::new();
    read_buf.resize(4, 0);
    socket.read_exact(&mut read_buf[..4]).await?;
    let len = (&read_buf[..4]).get_i32() as usize;
    read_buf.resize(len + 4, 0);
    socket.read_exact(&mut read_buf[4..len + 4]).await?;
    if let Some(response) = nsq_codec.decode(&mut read_buf)? {
        Ok(response)
    } else {
        Err(io::Error::from(io::ErrorKind::UnexpectedEof).into())
    }
}
