use tokio_util::codec::{Encoder, Decoder};
use bytes::BytesMut;
use super::{
    deflate::DeflateCodec,
    snappy::SnappyCodec,
};
use crate::{
    command::Command,
    error::Error,
    codec::{
        NsqCodec,
        NsqFramed,
    },
};

pub(crate) struct Codec(Compress);

enum Compress {
    Plain(NsqCodec),
    Snappy(SnappyCodec),
    Deflate(DeflateCodec),
    None,
}

impl Codec {
    pub fn new(nsq_codec: NsqCodec) -> Self {
        Self(Compress::Plain(nsq_codec))
    }

    pub fn use_snappy(&mut self) {
        self.0.use_snappy()
    }

    pub fn use_deflate(&mut self, level: u32) {
        self.0.use_deflate(level)
    }
}

impl Compress {
    fn use_snappy(&mut self) {
        let codec = if let Compress::Plain(nsq) = std::mem::replace(self, Compress::None) {
            SnappyCodec::new(nsq)
        } else {
            panic!("can update to snappy codec")
        };
        *self = Compress::Snappy(codec)
    }

    fn use_deflate(&mut self, level: u32) {
        let codec = if let Compress::Plain(nsq) = std::mem::replace(self, Compress::None) {
            DeflateCodec::new(nsq, level)
        } else {
            panic!("can update to deflate codec")
        };
        *self = Compress::Deflate(codec)
    }
}

impl Decoder for Codec {
    type Item = NsqFramed;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.0 {
            Compress::Plain(ref mut codec) => {
                codec.decode(src)
            }
            Compress::Snappy(ref mut snappy) => {
                snappy.decode(src)
            }
            Compress::Deflate(ref mut deflate) => {
                deflate.decode(src)
            }
            Compress::None => unreachable!(),
        }
    }
}

impl Encoder<Command> for Codec {
    type Error = Error;

    fn encode(&mut self, item: Command, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match self.0 {
            Compress::Plain(ref mut codec) => {
                codec.encode(item, dst)
            }
            Compress::Snappy(ref mut snappy) => {
                snappy.encode(item, dst)
            }
            Compress::Deflate(ref mut deflate) => {
                deflate.encode(item, dst)
            }
            Compress::None => unreachable!(),
        }
    }
}
