pub struct Lookup {
}

impl Lookup {

    /// Returns a list of producers for a topic
    pub fn lookup<S: AsRef<str>>(&self, topic: S) {
    }

    /// Returns a list of all known topics
    pub fn topics(&self) {
    }

    /// Returns a list of all known channels of a topic
    pub fn channels<S: AsRef<str>>(&self, topic: S) {
    }

    /// Returns a list of all known `nsqd`
    pub fn nodes(&self) -> Vec<Node> {
    }

    pub fn create_topic<S: AsRef<str>>(&mut self, topic: S) {
    }

    pub fn delete_topic<S: AsRef<str>>(&mut self, topic: S) {
    }

    pub fn create_channel<S: AsRef<str>>(&mut self, channel: S) {
    }

    pub fn delete_channel<S: AsRef<str>>(&mut self, channel: S) {
    }

    pub fn tombstone<S: AsRef<str>>(topic: S, node: Node) {
    }

    pub fn ping() {
    }

    pub fn info() {
    }
}
