use std::time::Duration;
use serde::{Serialize, Serializer, ser::SerializeMap};
use crate::command::Command;
use crate::Error;

const DEFAULT_CLIENT_NAME: &str = "nsq_in_rust";

#[derive(Debug, Clone, Serialize)]
pub struct Config {
    pub client_id: String,
    pub hostname: String,
    pub user_agent: String,

    #[serde(serialize_with = "serialize_tls")]
    pub tls_v1: Option<TlsConfig>,

    #[serde(flatten, serialize_with = "serialize_compress")]
    pub compress: Compress,

    // Duration of time between heartbeats. This must be less than ReadTimeout
    #[serde(serialize_with = "duration_to_ms")]
    pub heartbeat_interval: Duration,

    // Maximum number of times this consumer will attempt to process a message before giving up
    pub max_attempts: u16,

    // Maximum number of messages to allow in flight (concurrency knob)
    pub max_in_flight: usize,

    // Size of the buffer (in bytes) used by nsqd for buffering writes to this connection
    pub output_buffer_size: usize,

    // Timeout used by nsqd before flushing buffered writes (set to 0 to disable).
    //
    // WARNING: configuring clients with an extremely low
    // (< 25ms) output_buffer_timeout has a significant effect
    // on nsqd CPU usage (particularly with > 50 clients connected).
    #[serde(serialize_with = "duration_to_ms")]
    pub output_buffer_timeout: Duration,

    // The server-side message timeout for messages delivered to this client
    #[serde(serialize_with = "duration_to_ms")]
    pub msg_timeout: Duration,

    pub sample_rate: u8,

    // secret for nsqd authentication (requires nsqd 0.2.29+)
    #[serde(skip_serializing)]
    pub auth_secret: Option<String>,

    pub feature_negotiation: bool,
}

impl Config {
    pub fn identify(&self) -> Result<Command, Error> {
        let obj = serde_json::to_value(self)?;
        Ok(Command::Identify(obj))
    }

    /// Validate checks that all values are within specified min/max ranges
    pub fn validate(&self) -> Result<(), Error> {
        unimplemented!()
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            client_id: DEFAULT_CLIENT_NAME.into(),
            hostname: ::hostname::get_hostname().unwrap_or_else(|| "unknown".to_owned()),
            user_agent: crate::USER_AGENT.into(),
            tls_v1: None,
            compress: Compress::Disabled,
            heartbeat_interval: Duration::from_secs(30),
            max_attempts: 5,
            max_in_flight: 8,
            output_buffer_size: 1024*16,
            output_buffer_timeout: Duration::from_millis(250),
            msg_timeout: Duration::from_millis(5000),
            sample_rate: 0,
            auth_secret: None,
            feature_negotiation: true,
        }
    }
}

fn duration_to_ms<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_u64(duration.as_millis() as u64)
}

fn serialize_tls<S: Serializer>(tls_config: &Option<TlsConfig>, serializer: S) -> Result<S::Ok, S::Error> {
    if tls_config.is_some() {
        serializer.serialize_bool(true)
    } else {
        serializer.serialize_bool(false)
    }
}

fn serialize_compress<S: Serializer>(compress: &Compress, serializer: S) -> Result<S::Ok, S::Error> {
    match compress {
        Compress::Disabled => {
            let mut map = serializer.serialize_map(Some(2))?;
            map.serialize_entry("snappy", &false)?;
            map.serialize_entry("deflate", &false)?;
            map.end()
        }
        Compress::Snappy => {
            let mut map = serializer.serialize_map(Some(1))?;
            map.serialize_entry("snappy", &true)?;
            map.end()
        }
        Compress::Deflate{ level } => {
            let mut map = serializer.serialize_map(Some(2))?;
            map.serialize_entry("deflate", &true)?;
            map.serialize_entry("deflate_level", &level)?;
            map.end()
        }
    }
}

#[derive(Debug, Clone)]
pub enum Compress {
    Disabled,
    Snappy,
    Deflate{
        level: u32,
    },
}

impl Compress {
    pub fn is_enabled(&self) -> bool {
        match self {
            Compress::Disabled => false,
            _ => true,
        }
    }

    pub fn is_deflate(&self) -> bool {
        match self {
            Compress::Deflate{..} => true,
            _ => false,
        }
    }

    pub fn is_snappy(&self) -> bool {
        match self {
            Compress::Snappy => true,
            _ => false,
        }
    }
}

impl Default for Compress {
    fn default() -> Self {
        Compress::Disabled
    }
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub domain: String,

    /// String path to file containing root CA
    pub root_ca_file: Option<String>,

    /// String path to file containing public key for certificate
    pub cert_file: Option<String>,

    /// String path to file containing private key for certificate
    pub key_file: Option<String>,

    /// Bool indicates whether this client should verify server certificates
    pub insecure_skip_verify: bool,
}

mod tests {

    #[test]
    fn test_config() {
        use serde_json::Value;

        let mut config = super::Config::default();
        config.compress = super::Compress::Deflate{ level: 6 };
        let json = serde_json::to_string_pretty(&config).unwrap();
        println!("{}", json);

        let value: Value = serde_json::from_str(&json).unwrap();
        let object = value.as_object().unwrap();
        assert_eq!(object.get("deflate"), Some(&Value::from(true)));
        assert_eq!(object.get("deflate_level"), Some(&Value::from(6)));
        assert_eq!(object.get("snappy"), None);
    }
}
