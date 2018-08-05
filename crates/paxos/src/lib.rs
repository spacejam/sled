#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate serde;
extern crate sled;

use std::fmt::Debug;
use std::time::{Duration, SystemTime};

use serde::de::DeserializeOwned;
use serde::Serialize;

mod acceptor;
mod client;
mod proposer;
mod storage;

pub use acceptor::Acceptor;
pub use client::Client;
pub use proposer::Proposer;
pub use storage::{MemStorage, SledStorage};

// Reactor is a trait for building simulable systems.
pub trait Reactor: Debug + Clone {
    type Peer: std::net::ToSocketAddrs;
    type Message: Serialize + DeserializeOwned;

    fn receive(
        &mut self,
        at: SystemTime,
        from: Self::Peer,
        msg: Self::Message,
    ) -> Vec<(Self::Peer, Self::Message)>;

    fn tick(
        &mut self,
        _at: SystemTime,
    ) -> Vec<(Self::Peer, Self::Message)> {
        vec![]
    }
}

#[derive(Default, Clone, Debug, PartialOrd, PartialEq, Eq, Hash,
         Ord, Serialize, Deserialize)]
pub struct Ballot(u64);

type Key = Vec<u8>;
type Value = Vec<u8>;

#[derive(PartialOrd, Ord, Eq, PartialEq, Debug, Clone, Serialize,
         Deserialize)]
pub enum Req {
    Get(Key),
    Del(Key),
    Set(Key, Value),
    Cas(Key, Option<Value>, Option<Value>),
}

impl Req {
    fn key(&self) -> Key {
        match *self {
            Req::Get(ref k)
            | Req::Del(ref k)
            | Req::Set(ref k, _)
            | Req::Cas(ref k, _, _) => k.clone(),
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Serialize,
         Deserialize)]
pub enum Rpc {
    ClientRequest(u64, Req),
    ClientResponse(u64, Result<Option<Value>, Error>),
    SetAcceptAcceptors(Vec<String>),
    SetProposeAcceptors(Vec<String>),
    ProposeReq(Ballot, Key),
    ProposeRes {
        req_ballot: Ballot,
        last_accepted_ballot: Ballot,
        last_accepted_value: Option<Value>,
        res: Result<(), Error>,
    },
    AcceptReq(Ballot, Key, Option<Value>),
    AcceptRes(Ballot, Result<(), Error>),
}
use Rpc::*;

impl Rpc {
    pub fn client_req_id(&self) -> Option<u64> {
        match *self {
            ClientResponse(id, _) | ClientRequest(id, _) => Some(id),
            _ => None,
        }
    }

    pub fn client_req(self) -> Option<Req> {
        match self {
            ClientRequest(_, req) => Some(req),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Serialize,
         Deserialize)]
pub enum Error {
    ProposalRejected { last: Ballot },
    AcceptRejected { last: Ballot },
    CasFailed(Option<Value>),
    Timeout,
}

impl Error {
    pub fn is_rejected_accept(&self) -> bool {
        match *self {
            Error::AcceptRejected { .. } => true,
            _ => false,
        }
    }

    pub fn is_rejected_proposal(&self) -> bool {
        match *self {
            Error::ProposalRejected { .. } => true,
            _ => false,
        }
    }

    pub fn is_timeout(&self) -> bool {
        match *self {
            Error::Timeout => true,
            _ => false,
        }
    }

    pub fn is_failed_cas(&self) -> bool {
        match *self {
            Error::CasFailed(_) => true,
            _ => false,
        }
    }
}
