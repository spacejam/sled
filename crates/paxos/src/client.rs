use super::*;

#[derive(Debug)]
pub struct Client {
    proposers: Vec<SocketAddr>,
}

impl Client {
    pub fn get(&self, key: Key) -> Result<Option<Value>, Error> {
        unimplemented!()
    }

    pub fn del(&self, key: Key) -> Result<Option<Value>, Error> {
        unimplemented!()
    }

    pub fn set(&self, key: Key, value: Value) -> Result<Option<Value>, Error> {
        unimplemented!()
    }

    pub fn cas(
        &self,
        key: Key,
        old_value: Value,
        new_value: Value,
    ) -> Result<Option<Value>, Error> {
        unimplemented!()
    }
}

impl Reactor for Client {
    type Peer = SocketAddr;
    type Message = Rpc;

    fn receive(
        &mut self,
        at: SystemTime,
        from: Self::Peer,
        msg: Self::Message,
    ) -> Vec<(Self::Peer, Self::Message)> {
        unimplemented!()
    }

    fn tick(&mut self, at: SystemTime) -> Vec<(Self::Peer, Self::Message)> {
        unimplemented!()
    }
}
