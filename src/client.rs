pub struct Client {
}

impl Client {
    pub fn connect(addr: SocketAddr, handle: &Handle) -> impl Future<Item = Client, Error = io::Error> {
        let ret = TcpClient::new(NsqProto)
            .connect(addr, handle);

        ret
    }

    pub fn discover(addr: SocketAddr, hadnle: &Handle) -> impl Future<Item = Client, Error = io::Error> {
    }
}
