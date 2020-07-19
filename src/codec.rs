// Data Format
//
// Data is streamed asynchronously to the client and framed in order to support the various reply bodies, ie:
//
// [x][x][x][x][x][x][x][x][x][x][x][x]...
// |  (int32) ||  (int32) || (binary)
// |  4-byte  ||  4-byte  || N-byte
// ------------------------------------...
//     size     frame type     data
//
// A client should expect one of the following frame types:
//
// FrameTypeResponse int32 = 0
// FrameTypeError    int32 = 1
// FrameTypeMessage  int32 = 2
//
// And finally, the message format:
//
// [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
// |       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
// |       8-byte         ||    ||                 16-byte                      || N-byte
// ------------------------------------------------------------------------------------------...
//   nanosecond timestamp    ^^                   message ID                       message body
//                        (uint16)
//                         2-byte
//                        attempts
//
use std::str;
use std::io;

use tracing::trace;
use serde_json::{self, Value as JsonValue};
use bytes::{Buf, BytesMut, BufMut};
use tokio_util::codec::LengthDelimitedCodec;
pub(crate) use tokio_util::codec::{Encoder, Decoder};

use crate::command::{Command, Body};
use crate::error::{Result, Error, NsqError};

// const SIZE_LEN: usize = 4;
// const FRAME_TYPE_LEN: usize = 4;
// const TIMESTAMP_LEN: usize = 8;
// const ATTEMPTS_LEN: usize = 2;
const MESSAGE_ID_LEN: usize = 16;
const MESSAGE_SIZE_LEN: usize = 4;

const FRAME_TYPE_RESPONSE: i32 = 0;
const FRAME_TYPE_ERROR:    i32 = 1;
const FRAME_TYPE_MESSAGE:  i32 = 2;

const HEARTBEAT_RESPONSE: &str = "_heartbeat_";
const OK_RESPONSE: &str = "OK";
const CLOSE_WAIT: &str = "CLOSE_WAIT";

#[derive(Debug)]
pub struct NsqCodec {
    feature_negotiation: bool,

    // decode nsq response, witch is length delimited protocol
    length_delimited_codec: LengthDelimitedCodec,
}

impl NsqCodec {
    pub fn new(feature_negotiation: bool) -> Self {
        Self {
            feature_negotiation,
            length_delimited_codec: LengthDelimitedCodec::new(),
        }
    }
}

#[derive(Debug)]
pub enum NsqFramed {
    Response(RawResponse),
    Error(NsqError),
    Message(NsqMsg),
}

#[derive(Debug)]
pub struct NsqMsg {
    timestamp: u64,
    attempts: u16,
    message_id: String,
    body: Vec<u8>,
}

#[derive(Debug)]
pub enum RawResponse {
    Ok,
    Heartbeat,
    CloseWait,
    Json(JsonValue),
}

impl Encoder<Command> for NsqCodec {
    type Error = Error;

    fn encode(&mut self, cmd: Command, buf: &mut BytesMut) -> Result<()> {
        let header = cmd.header();
        buf.reserve(header.len());
        buf.extend(header.as_bytes());

        if let Some(body) = cmd.body() {
            match body {
                Body::Binary(bin) => {
                    buf.reserve(bin.len() + 4);
                    buf.put_u32(bin.len() as u32);
                    // buf.extend(bin);
                    buf.put(bin.as_slice());
                }
                Body::Messages(msgs) => {
                    // let len = msgs.iter().map(|msg| msg.len()).fold(0, |acc, len| acc + len);
                    let body_len = msgs.iter().fold(8, |acc, msg| acc + msg.len() + MESSAGE_SIZE_LEN);
                    buf.reserve(body_len);
                    buf.put_u32(body_len as u32);
                    buf.put_u32(msgs.len() as u32);
                    let _buf = msgs.iter().fold(buf, |buf, msg| {
                        buf.put_u32(msg.len() as u32);
                        buf.put(msg.as_slice());
                        buf
                    });
                }
                Body::Json(json) => {
                    let body = serde_json::to_string(&json)?;
                    trace!("send json: {}", &body);
                    let body = body.as_bytes();
                    buf.reserve(body.len() + 4);
                    buf.put_u32(body.len() as u32);
                    // buf.extend(body);
                    buf.put(body);
                }
            }
        }

        Ok(())
    }
}

impl Encoder<Command> for Box<NsqCodec> {
    type Error = Error;
    fn encode(&mut self, cmd: Command, buf: &mut BytesMut) -> Result<()> {
        self.as_mut().encode(cmd, buf)
    }
}

impl Decoder for NsqCodec {
    type Item = NsqFramed;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>> {
        let mut buf = match self.length_delimited_codec.decode(buf)? {
            Some(buf) => buf,
            None => return Ok(None),
        };

        let frame_type = buf.get_i32();

        let item = match frame_type {
            FRAME_TYPE_RESPONSE => {
                NsqFramed::Response(decode_raw_response(buf)?)
            }
            FRAME_TYPE_ERROR => {
                NsqFramed::Error(decode_error(buf)?)
            }
            FRAME_TYPE_MESSAGE => {
                NsqFramed::Message(decode_message(buf)?)
            }
            _x => {
                return Err(io::Error::new(io::ErrorKind::Other, "unknown frame type").into());
            }
        };

        Ok(Some(item))
    }
}

impl Decoder for Box<NsqCodec> {
    type Item = NsqFramed;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>> {
        self.as_mut().decode(buf)
    }
}

fn decode_message(mut buf: BytesMut) -> Result<NsqMsg> {
    let timestamp = buf.get_u64();
    let attempts = buf.get_u16();
    let buf = buf.as_ref();
    let (head, body) = buf.split_at(MESSAGE_ID_LEN);
    let message_id = str::from_utf8(&head)?.to_string();

    Ok(NsqMsg {
        timestamp,
        attempts,
        message_id,
        body: body.to_vec(),
    })
}

fn decode_error(buf: BytesMut) -> Result<NsqError> {
    let err = str::from_utf8(buf.as_ref())?;
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

fn decode_raw_response(buf: BytesMut) -> Result<RawResponse> {
    match str::from_utf8(buf.as_ref())? {
        OK_RESPONSE => Ok(RawResponse::Ok),
        CLOSE_WAIT => Ok(RawResponse::CloseWait),
        HEARTBEAT_RESPONSE => Ok(RawResponse::Heartbeat),
        body => Ok(RawResponse::Json(serde_json::from_str(body)?)),
    }
}
