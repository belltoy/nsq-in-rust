use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, BufReader};
use async_compression::Level;
use async_compression::tokio::{
    bufread::DeflateDecoder,
    write::DeflateEncoder,
};

#[derive(Debug)]
pub struct DeflateStream<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    reader: BufReader<DeflateDecoder<BufReader<R>>>,
    writer: DeflateEncoder<W>,
}

impl<R, W> DeflateStream<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    pub fn new(reader: R, writer: W, level: u32) -> Self {
        let writer = DeflateEncoder::with_quality(writer, Level::Precise(level));
        let reader = BufReader::new(DeflateDecoder::new(BufReader::new(reader)));
        Self {
            reader,
            writer,
        }
    }
}

impl<R, W> AsyncWrite for DeflateStream<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.writer).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.writer).poll_shutdown(cx)
    }
}

impl<R, W> AsyncRead for DeflateStream<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}
