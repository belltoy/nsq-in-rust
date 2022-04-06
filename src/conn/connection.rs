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
use std::pin::Pin;
use std::task::{Context, Poll};
use std::net::SocketAddr;
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
use super::tls::TlsStream;
use tracing::{trace, debug, error};

use crate::error::Error;
use crate::codec::{Decoder, Encoder, NsqCodec, NsqFramed, RawResponse};
use crate::command::Command;
use crate::conn::{Heartbeat, Response, BaseIo};
use crate::config::{Config, TlsConfig};
use crate::producer::Producer;
use crate::conn::deflate::DeflateStream;

pub struct Connection(pub(crate) Heartbeat<BaseIo>);

pub type ConnSink = SplitSink<Heartbeat<BaseIo>, Command>;
pub type ConnStream = SplitStream<Heartbeat<BaseIo>>;

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
        let (transport, identify) = connect(addr, config).await?;
        trace!("connected to nsqd, with identify: {:?}", identify);
        Ok(Self(transport))
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

impl From<Connection> for Producer {
    fn from(conn: Connection) -> Self {
        Self::from_connection(conn)
    }
}

async fn connect<A>(addr: A, config: &Config)
    -> Result<(Heartbeat<BaseIo>, IdentifyResponse), Error>
where
    A: Into<SocketAddr>,
{
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

    let boxed_stream = if identify.snappy {
        let mut snappy_stream = upgrade_snappy(socket);
        if let NsqFramed::Response(RawResponse::Ok) = read_response(&mut snappy_stream, &mut nsq_codec).await? {
            BaseIo::Snappy(snappy_stream)
        } else {
            return Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "compression negotiation expected OK",
            )));
        }
    } else if identify.deflate {
        let mut deflate_stream = upgrade_deflate(socket, identify.deflate_level);
        if let NsqFramed::Response(RawResponse::Ok) = read_response(&mut deflate_stream, &mut nsq_codec).await? {
            BaseIo::Deflate(deflate_stream)
        } else {
            return Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "compression negotiation expected OK",
            )));
        }
    } else {
        BaseIo::NoCompress(socket)
    };
    let mut framed = Framed::new(boxed_stream, nsq_codec);

    if identify.auth_required {
        let auth_response = auth(config, &mut framed).await?;
        debug!("connection auth response: {:?}", auth_response);
    }

    // handle heartbeat
    Ok((Heartbeat::new(framed), identify))
}


fn upgrade_deflate<T>(io: T, level: u32) -> DeflateStream<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    DeflateStream::new(io, level)
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
