use snap::raw::*;
use tokio_util::codec::{Encoder, Decoder};
use bytes::BytesMut;
use crate::{
    command::Command,
    error::Error,
    codec::{
        NsqCodec,
        NsqFramed,
    },
};

pub(crate) struct SnappyCodec {
    encoder: snap::raw::Encoder,
    decoder: snap::raw::Decoder,
    inner_codec: NsqCodec,
}

impl SnappyCodec {
    pub fn new(codec: NsqCodec) -> Self {
        Self {
            encoder: snap::raw::Encoder::new(),
            decoder: snap::raw::Decoder::new(),
            inner_codec: codec,
        }
    }
}

impl Decoder for SnappyCodec {
    type Item = NsqFramed;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let len = decompress_len(&src)?;
        let mut buf = BytesMut::with_capacity(len);
        let _decompressed_size = self.decoder.decompress(src.as_ref(), buf.as_mut())?;
        self.inner_codec.decode(&mut buf)
    }
}

impl Encoder<Command> for SnappyCodec {
    type Error = Error;

    fn encode(&mut self, item: Command, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut buf = BytesMut::with_capacity(1024);
        self.inner_codec.encode(item, &mut buf)?;
        self.encoder.compress(&buf, dst.as_mut())?;
        Ok(())
    }
}

impl Decoder for Box<SnappyCodec> {
    type Item = NsqFramed;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.as_mut().decode(src)
    }
}

impl Encoder<Command> for Box<SnappyCodec> {
    type Error = Error;

    fn encode(&mut self, item: Command, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.as_mut().encode(item, dst)
    }
}
