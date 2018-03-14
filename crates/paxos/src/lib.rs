use std::time::SystemTime;
use std::net::SocketAddr;

mod acceptor;
mod proposer;
mod client;

// Reactor is a trait for building simulable systems.
pub trait Reactor {
    type Peer;
    type Message;

    fn receive(
        &mut self,
        at: SystemTime,
        from: Self::Peer,
        msg: Self::Message,
    ) -> Vec<(Self::Peer, Self::Message)>;

    fn tick(&mut self, at: SystemTime) -> Vec<(Self::Peer, Self::Message)> {
        vec![]
    }
}

#[derive(Default, Clone, Debug, PartialOrd, PartialEq, Eq, Hash)]
pub struct Ballot(u64);

type Key = Vec<u8>;
type Value = Vec<u8>;

#[derive(Debug)]
pub enum Rpc {
    Get,
    Del,
    Set(Value),
    Cas(Option<Value>, Option<Value>),
    ClientResponse(Result<Option<Value>, Error>),
    SetAcceptAcceptors(Vec<SocketAddr>),
    SetProposeAcceptors(Vec<SocketAddr>),
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

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    ProposalRejected { last: Ballot },
    AcceptRejected { last: Ballot },
    CasFailed(Value),
}
