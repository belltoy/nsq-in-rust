use std::pin::Pin;
use std::task::{Context, Poll};

use crate::error::{Error, NsqError};
use crate::codec::{NsqMsg, NsqFramed};
use crate::command::Command;

use pin_project::pin_project;
use futures::prelude::*;
use tokio_snappy::SnappyIO;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use self::deflate::DeflateStream;

mod deflate;
mod heartbeat;
mod tls;
pub mod connection;

pub(crate) trait Transport: Stream<Item = Result<NsqFramed, Error>> + Sink<Command, Error = Error> + Unpin {}
pub(crate) trait MessageStream: Stream<Item = Result<Response, Error>> + Sink<Command, Error = Error> + Unpin {}
pub(crate) use heartbeat::Heartbeat;
use self::tls::TlsStream;
pub use connection::Connection;

#[derive(Debug)]
pub enum Response {
    Ok,
    Err(NsqError),
    Msg(NsqMsg),
}

#[pin_project(project = BaseIoProj)]
pub enum BaseIo
{
    Snappy(#[pin] SnappyIO<TcpStream>),
    SnappyTls(#[pin] SnappyIO<TlsStream<TcpStream>>),
    Deflate(#[pin] DeflateStream<TcpStream>),
    DeflateTls(#[pin] DeflateStream<TlsStream<TcpStream>>),
    NoCompress(#[pin] TcpStream),
    NoCompressTsl(#[pin] TlsStream<TcpStream>),
}

impl AsyncRead for BaseIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let this: BaseIoProj = self.project();
        match this {
            BaseIoProj::Snappy(s) => {
                let s: Pin<&mut SnappyIO<TcpStream>> = s;
                s.poll_read(cx, buf)
            }
            BaseIoProj::SnappyTls(s) => {
                let s: Pin<&mut SnappyIO<TlsStream<TcpStream>>> = s;
                s.poll_read(cx, buf)
            }
            BaseIoProj::Deflate(s) => {
                let s: Pin<&mut DeflateStream<TcpStream>> = s;
                s.poll_read(cx, buf)
            }
            BaseIoProj::DeflateTls(s) => {
                let s: Pin<&mut DeflateStream<TlsStream<TcpStream>>> = s;
                s.poll_read(cx, buf)
            }
            BaseIoProj::NoCompress(s) => {
                let s: Pin<&mut TcpStream> = s;
                s.poll_read(cx, buf)
            }
            BaseIoProj::NoCompressTsl(s) => {
                let s: Pin<&mut TlsStream<TcpStream>> = s;
                s.poll_read(cx, buf)
            }
        }
    }
}

impl AsyncWrite for BaseIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let this: BaseIoProj = self.project();
        match this {
            BaseIoProj::Snappy(s) => {
                let s: Pin<&mut SnappyIO<TcpStream>> = s;
                s.poll_write(cx, buf)
            }
            BaseIoProj::SnappyTls(s) => {
                let s: Pin<&mut SnappyIO<TlsStream<TcpStream>>> = s;
                s.poll_write(cx, buf)
            }
            BaseIoProj::Deflate(s) => {
                let s: Pin<&mut DeflateStream<TcpStream>> = s;
                s.poll_write(cx, buf)
            }
            BaseIoProj::DeflateTls(s) => {
                let s: Pin<&mut DeflateStream<TlsStream<TcpStream>>> = s;
                s.poll_write(cx, buf)
            }
            BaseIoProj::NoCompress(s) => {
                let s: Pin<&mut TcpStream> = s;
                s.poll_write(cx, buf)
            }
            BaseIoProj::NoCompressTsl(s) => {
                let s: Pin<&mut TlsStream<TcpStream>> = s;
                s.poll_write(cx, buf)
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        let this: BaseIoProj = self.project();
        match this {
            BaseIoProj::Snappy(s) => {
                let s: Pin<&mut SnappyIO<TcpStream>> = s;
                s.poll_flush(cx)
            }
            BaseIoProj::SnappyTls(s) => {
                let s: Pin<&mut SnappyIO<TlsStream<TcpStream>>> = s;
                s.poll_flush(cx)
            }
            BaseIoProj::Deflate(s) => {
                let s: Pin<&mut DeflateStream<TcpStream>> = s;
                s.poll_flush(cx)
            }
            BaseIoProj::DeflateTls(s) => {
                let s: Pin<&mut DeflateStream<TlsStream<TcpStream>>> = s;
                s.poll_flush(cx)
            }
            BaseIoProj::NoCompress(s) => {
                let s: Pin<&mut TcpStream> = s;
                s.poll_flush(cx)
            }
            BaseIoProj::NoCompressTsl(s) => {
                let s: Pin<&mut TlsStream<TcpStream>> = s;
                s.poll_flush(cx)
            }
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        let this: BaseIoProj = self.project();
        match this {
            BaseIoProj::Snappy(s) => {
                let s: Pin<&mut SnappyIO<TcpStream>> = s;
                s.poll_shutdown(cx)
            }
            BaseIoProj::SnappyTls(s) => {
                let s: Pin<&mut SnappyIO<TlsStream<TcpStream>>> = s;
                s.poll_shutdown(cx)
            }
            BaseIoProj::Deflate(s) => {
                let s: Pin<&mut DeflateStream<TcpStream>> = s;
                s.poll_shutdown(cx)
            }
            BaseIoProj::DeflateTls(s) => {
                let s: Pin<&mut DeflateStream<TlsStream<TcpStream>>> = s;
                s.poll_shutdown(cx)
            }
            BaseIoProj::NoCompress(s) => {
                let s: Pin<&mut TcpStream> = s;
                s.poll_shutdown(cx)
            }
            BaseIoProj::NoCompressTsl(s) => {
                let s: Pin<&mut TlsStream<TcpStream>> = s;
                s.poll_shutdown(cx)
            }
        }
    }
}
