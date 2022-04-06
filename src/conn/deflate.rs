use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, BufReader, ReadHalf, WriteHalf};
use async_compression::Level;
use async_compression::tokio::{
    bufread::DeflateDecoder,
    write::DeflateEncoder,
};

#[derive(Debug)]
pub struct DeflateStream<T>
where
    T: AsyncRead + AsyncWrite,
{
    reader: BufReader<DeflateDecoder<BufReader<ReadHalf<T>>>>,
    writer: DeflateEncoder<WriteHalf<T>>,
}

impl<T> DeflateStream<T>
where
    T: AsyncRead + AsyncWrite,
{
    pub fn new(io: T, level: u32) -> Self {
        let (reader, writer) = tokio::io::split(io);
        let writer = DeflateEncoder::with_quality(writer, Level::Precise(level));
        let reader = BufReader::new(DeflateDecoder::new(BufReader::new(reader)));
        Self {
            reader,
            writer,
        }
    }
}

impl<T> AsyncWrite for DeflateStream<T>
where
    T: AsyncRead + AsyncWrite,
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

impl<T> AsyncRead for DeflateStream<T>
where
    T: AsyncRead + AsyncWrite,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}
