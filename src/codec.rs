use std::str;
use std::io::{self, Cursor};

use serde_json::{self, Value as JsonValue};
use bytes::{Buf, BytesMut, BufMut, IntoBuf};
use tokio_io::codec::{Encoder, Decoder};

use command::{Command, Body};
use error::{Result, Error, NsqError};

// const SIZE_LEN: u8 = 4;
// const FRAME_TYPE_LEN: u8 = 4;
// const TIMESTAMP_LEN: u8 = 8;
// const ATTEMPTS_LEN: u8 = 2;
// const MESSAGE_ID_LEN: u8 = 16;

const FRAME_TYPE_RESPONSE: i32 = 0;
const FRAME_TYPE_ERROR:    i32 = 1;
const FRAME_TYPE_MESSAGE:  i32 = 2;

const HEARTBEAT_RESPONSE: &str = "_heartbeat_";
const OK_RESPONSE: &str = "OK";
const CLOSE_WAIT: &str = "CLOSE_WAIT";

#[derive(Debug)]
pub struct NsqCodec {
    feature_negotiation: bool,
}

impl NsqCodec {
    pub fn new(feature_negotiation: bool) -> Self {
        Self {
            feature_negotiation
        }
    }
}

#[derive(Debug)]
pub enum NsqFramed {
    Response(Response),
    Error(NsqError),
    Message(NsqMessage),
}

#[derive(Debug)]
pub struct NsqMessage {
    timestamp: u64,
    attempts: u16,
    message_id: String,
    body: Vec<u8>,
}

#[derive(Debug)]
pub enum Response {
    Ok,
    Heartbeat,
    CloseWait,
    Json(JsonValue),
}

impl Encoder for NsqCodec {
    type Item = Command;
    type Error = Error;

    fn encode(&mut self, cmd: Self::Item, buf: &mut BytesMut) -> Result<()> {

        let header = cmd.header();
        buf.reserve(header.len());
        buf.extend(header.as_bytes());

        match cmd.body() {
            Some(Body::Binary(bin)) => {
                println!("==== binary");
                buf.reserve(bin.len() + 4);
                buf.put_u32_be(bin.len() as u32);
                buf.extend(bin);
                println!("==== binary end: len: {}, {:?}", bin.len(), bin);
            }
            Some(Body::Messages(msgs)) => {
                let len = msgs.iter().map(|msg| msg.len()).fold(0, |acc, len| acc + len);
                let mut bufs: Vec<u8> = Vec::with_capacity(len + 4);
                bufs.put_u32_be(len as u32);
                let bufs = msgs.iter().fold(bufs, |mut bufs, msg| {
                    bufs.put_u32_be(msg.len() as u32);
                    bufs.put(msg);
                    bufs
                });
                buf.reserve(bufs.len());
                buf.extend(bufs);
            }
            Some(Body::Json(json)) => {
                let body = serde_json::to_string(&json)?;
                let body = body.as_bytes();
                buf.reserve(body.len() + 4);
                buf.put_u32_be(body.len() as u32);
                buf.extend(body);
            }

            _ => {}
        }

        Ok(())
    }
}

impl Decoder for NsqCodec {
    type Item = NsqFramed;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>> {
        {
            let buf_len = buf.len();

            if buf_len < 8 {
                return Ok(None)
            }

            let len = buf.split_at(4).1.into_buf().get_i32_be();
            if buf_len < len as usize + 4 {
                return Ok(None)
            }
        }

        buf.advance(4);
        let item_type = buf.split_to(4).into_buf().get_i32_be();

        let item = match item_type {
            FRAME_TYPE_RESPONSE => {
                NsqFramed::Response(decode_response(buf)?)
            }
            FRAME_TYPE_ERROR => {
                NsqFramed::Error(decode_error(buf)?)
            }
            FRAME_TYPE_MESSAGE => {
                NsqFramed::Message(decode_message(buf))
            }
            _x => {
                unreachable!("Wrong frame type")
            }
        };

        Ok(Some(item))
    }
}

fn decode_message(buf: &mut BytesMut) -> NsqMessage {
    let mut buf = Cursor::new(buf);
    let timestamp = buf.get_u64_be();
    let attempts = buf.get_u16_be();
    let buf = buf.into_inner();
    let head = buf.split_to(8);
    let message_id = str::from_utf8(&head).unwrap().to_string();
    let body       = buf;

    NsqMessage {
        timestamp: timestamp,
        attempts: attempts,
        message_id: message_id,
        body: body.to_vec(),
    }
}

fn decode_error(buf: &mut BytesMut) -> Result<NsqError> {
    let err = buf.take();
    let err = str::from_utf8(&err)?;
    let err = match err.find(" ") {
        Some(idx) => {
            let (code, desc) = err.split_at(idx);
            NsqError::new(code, desc.trim())
        }
        None => {
            NsqError::new("Unknown", err)
        }
    };
    Ok(err)
}

fn decode_response(buf: &mut BytesMut) -> Result<Response> {
    let buf = buf.take();

    match str::from_utf8(&buf) {
        Ok(OK_RESPONSE) => Ok(Response::Ok),
        Ok(CLOSE_WAIT) => Ok(Response::CloseWait),
        Ok(HEARTBEAT_RESPONSE) => Ok(Response::Heartbeat),
        Ok(body) => {
            let json = match serde_json::from_str(body) {
                Ok(json) => json,
                Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e).into()),
            };
            Ok(Response::Json(json))
        }
        Err(e) => {
            Err(io::Error::new(io::ErrorKind::Other, e).into())
        }
    }
}
