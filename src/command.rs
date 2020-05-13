use serde_json::Value as JsonValue;

pub type MessageBody = Vec<u8>;

#[derive(Debug)]
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
    Auth(String),
}

pub(crate) enum Body {
    Binary(MessageBody),
    Messages(Vec<MessageBody>),
    Json(JsonValue),
}

impl Command {
    pub(crate) fn header(&self) -> String {
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

    pub(crate) fn body(self) -> Option<Body> {
        use self::Command::*;
        match self {
            Identify(value) => Body::Json(value).into(),
            Version | Sub(..) | Rdy(..) | Fin(..) | Req(..) | Touch(..) | Close | Nop => None,
            Pub(_, body) | Dpub(_, _, body) => Body::Binary(body).into(),
            Mpub(_, messages) => Body::Messages(messages).into(),
            Auth(secret) => Body::Binary(secret.into_bytes()).into(),
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
