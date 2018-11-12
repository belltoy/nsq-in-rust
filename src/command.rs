use serde_json::Value as JsonValue;

pub const HEARTBEAT_RESPONSE: &str = "_heartbeat_";
pub const OK_RESPONSE: &str = "OK";

type MessageBody = Vec<u8>;

pub enum Command {
    Version,
    Identify(JsonValue),
    Sub(String, String),
    Pub(String, MessageBody),
    Mpub(String, Vec<MessageBody>),
    Dpub(String, u64, MessageBody),
    Rdy(u64),
    Fin(String),
    Req(String, u64),
    Touch(String),
    Close,
    Nop,
    Auth(MessageBody),
}

pub enum Body<'a> {
    Binary(&'a MessageBody),
    Messages(&'a Vec<MessageBody>),
    Json(&'a JsonValue),
}

impl Command {
    pub fn header(&self) -> String {
        use self::Command::*;
        let cmd_name = self.cmd();
        match *self {
            Version                     => cmd_name.to_string(),
            Identify(..)                => format!("{}\n",       cmd_name),
            Sub(ref topic, ref channel) => format!("{} {} {}\n", cmd_name, topic, channel),
            Pub(ref topic, _)           => format!("{} {}\n",    cmd_name, topic),
            Mpub(ref topic, _)          => format!("{} {}\n",    cmd_name, topic),
            Dpub(ref topic, defer, _)   => format!("{} {} {}\n", cmd_name, topic, defer),
            Rdy(count)                  => format!("{} {}\n",    cmd_name, count),
            Fin(ref id)                 => format!("{} {}\n",    cmd_name, id),
            Req(ref id, timeout)        => format!("{} {} {}\n", cmd_name, id, timeout),
            Touch(ref id)               => format!("{} {}\n",    cmd_name, id),
            Close                       => format!("{}\n",       cmd_name),
            Nop                         => format!("{}\n",       cmd_name),
            Auth(..)                    => format!("{}\n",       cmd_name),
        }
    }

    pub fn body(&self) -> Option<Body> {
        use self::Command::*;
        match *self {
            Identify(ref value) => Body::Json(value).into(),
            Version | Sub(..) | Rdy(..) | Fin(..) | Req(..) | Touch(..) | Close | Nop => None,
            Pub(_, ref body) | Dpub(_, _, ref body) | Auth(ref body) => Body::Binary(body).into(),
            Mpub(_, ref messages) => Body::Messages(messages).into(),
        }
    }

    fn cmd(&self) -> &str {
        use self::Command::*;
        match *self {
            Version => "  V2",
            Identify(..) => "IDENTIFY",
            Sub(..) => "SUB",
            Pub(..) => "PUB",
            Mpub(..) => "MPUB",
            Dpub(..) => "DPUB",
            Rdy(..) => "RDY",
            Fin(..) => "FIN",
            Req(..) => "REQ",
            Touch(..) => "TOUCH",
            Close => "CLS",
            Nop => "NOP",
            Auth(..) => "AUTH",
        }
    }
}
