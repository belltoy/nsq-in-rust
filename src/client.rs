pub struct Client {
}

impl Client {
    pub fn connect(addr: SocketAddr, handle: &Handle) -> Box<Future<Item = Client, Error = io::Error>> {
        let ret = TcpClient::new(NsqProto)
            .connect(addr, handle);

        Box::new(ret)
    }

    pub fn discover(addr: SocketAddr, hadnle: &Handle) -> Box<Future<Item = Client, Error = io::Error>> {
    }
}
