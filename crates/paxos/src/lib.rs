use std::fmt::Debug;
use std::time::{Duration, SystemTime};

mod acceptor;
mod proposer;
mod client;

pub use acceptor::Acceptor;
pub use proposer::Proposer;
pub use client::Client;

// Reactor is a trait for building simulable systems.
pub trait Reactor: Debug + Clone {
    type Peer;
    type Message;

    fn receive(
        &mut self,
        at: SystemTime,
        from: Self::Peer,
        msg: Self::Message,
    ) -> Vec<(Self::Peer, Self::Message)>;

    fn tick(&mut self, _at: SystemTime) -> Vec<(Self::Peer, Self::Message)> {
        vec![]
    }
}

#[derive(Default, Clone, Debug, PartialOrd, PartialEq, Eq, Hash, Ord)]
pub struct Ballot(u64);

type Value = Vec<u8>;

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone)]
pub enum Rpc {
    Get(u64),
    Del(u64),
    Set(u64, Value),
    Cas(u64, Option<Value>, Option<Value>),
    ClientResponse(u64, Result<Option<Value>, Error>),
    SetAcceptAcceptors(Vec<String>),
    SetProposeAcceptors(Vec<String>),
    ProposeReq(Ballot),
    ProposeRes {
        req_ballot: Ballot,
        last_accepted_ballot: Ballot,
        last_accepted_value: Option<Value>,
        res: Result<(), Error>,
    },
    AcceptReq(Ballot, Option<Value>),
    AcceptRes(Ballot, Result<(), Error>),
}
use Rpc::*;

impl Rpc {
    pub fn client_req_id(&self) -> Option<u64> {
        match *self {
            ClientResponse(id, _) |
            Get(id) |
            Del(id) |
            Set(id, _) |
            Cas(id, _, _) => Some(id),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone)]
pub enum Error {
    ProposalRejected { last: Ballot },
    AcceptRejected { last: Ballot },
    CasFailed(Value),
    Timeout,
}
