use std::time::Duration;

#[derive(Debug)]
pub struct Config {
    client_id: String,
    hostname: String,
    user_agent: String,

    tls_v1: bool,

    deflate: bool,

    snappy: bool,

    // Duration of time between heartbeats. This must be less than ReadTimeout
    heartbeat_interval: Duration,

    // Maximum number of times this consumer will attempt to process a message before giving up
    max_attempts: u16,

    // Maximum number of messages to allow in flight (concurrency knob)
    max_in_flight: usize,

    // Size of the buffer (in bytes) used by nsqd for buffering writes to this connection
    output_buffer_size: usize,

    // Timeout used by nsqd before flushing buffered writes (set to 0 to disable).
    //
    // WARNING: configuring clients with an extremely low
    // (< 25ms) output_buffer_timeout has a significant effect
    // on nsqd CPU usage (particularly with > 50 clients connected).
    output_buffer_timeout: Duration,

    // The server-side message timeout for messages delivered to this client
    msg_timeout: Duration,

    // secret for nsqd authentication (requires nsqd 0.2.29+)
    auth_secret: Option<String>,
}

#[derive(Debug)]
enum DeflateConfig {
    Enabled {
        level: u32,
    },
    Disabled,
}

#[derive(Debug)]
enum TlsConfig {
    Enabled(TlsSettings),
    Disabled,
}

#[derive(Debug)]
struct TlsSettings {
    cert_file: String,
    key_file: String,
}
