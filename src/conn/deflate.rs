use crate::{
    command::Command,
    error::Error,
    codec::{
        NsqCodec,
        NsqFramed,
    },
};
use tokio_util::codec::{Encoder, Decoder};
use bytes::BytesMut;
use flate2::{
    Compress,
    Compression,
    Decompress,
    FlushCompress,
    FlushDecompress,
};

pub(crate) struct DeflateCodec {
    inner_codec: NsqCodec,
    compress: Compress,
    decompress: Decompress,
}

impl DeflateCodec {
    pub fn new(codec: NsqCodec, level: u32) -> Self {
        let compress = Compress::new(Compression::new(level), false);
        let decompress = Decompress::new(false);
        Self {
            inner_codec: codec,
            compress,
            decompress,
        }
    }
}

impl Decoder for DeflateCodec {
    type Item = NsqFramed;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut buf = BytesMut::with_capacity(1024);
        self.decompress.decompress(src, &mut buf, FlushDecompress::None)?;
        // let len = decompress_len(&src)?;
        // let _decompressed_size = self.decoder.decompress(src.as_ref(), buf.as_mut())?;
        self.inner_codec.decode(&mut buf)
    }
}

impl Encoder<Command> for DeflateCodec {
    type Error = Error;

    fn encode(&mut self, item: Command, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut buf = BytesMut::with_capacity(1024);
        self.inner_codec.encode(item, &mut buf)?;
        self.compress.compress(&buf, dst.as_mut(), FlushCompress::None)?;
        Ok(())
    }
}
