use super::*;

use std::collections::HashMap;

#[derive(Debug, Default, Clone)]
pub struct Client {
    proposers: Vec<String>,
    req_counter: u64,
    in_flight: HashMap<u64, ()>,
}

impl Client {
    pub fn new(proposers: Vec<String>) -> Client {
        Client {
            proposers: proposers,
            req_counter: 0,
            in_flight: HashMap::new(),
        }
    }

    pub fn get(&self) -> Result<Option<Value>, Error> {
        unimplemented!()
    }

    pub fn del(&self) -> Result<Option<Value>, Error> {
        unimplemented!()
    }

    pub fn set(&self, _value: Value) -> Result<Option<Value>, Error> {
        unimplemented!()
    }

    pub fn cas(
        &self,
        _old_value: Value,
        _new_value: Value,
    ) -> Result<Option<Value>, Error> {
        unimplemented!()
    }
}

impl Reactor for Client {
    type Peer = String;
    type Message = Rpc;

    fn receive(
        &mut self,
        _at: SystemTime,
        _from: Self::Peer,
        _msg: Self::Message,
    ) -> Vec<(Self::Peer, Self::Message)> {
        unimplemented!()
    }

    fn tick(
        &mut self,
        _at: SystemTime,
    ) -> Vec<(Self::Peer, Self::Message)> {
        unimplemented!()
    }
}
