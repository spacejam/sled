use super::*;

use std::collections::HashMap;

type Op = fn(Option<Value>) -> Option<Value>;

enum Phase {
    Propose,
    Accept,
}
use Phase::*;

struct Pending {
    backref: SocketAddr,
    op: Op,
    phase: Phase,
    waiting_for: Vec<SocketAddr>,
    got: usize,
}

#[derive(Debug)]
pub struct Proposer {
    accept_acceptors: Vec<SocketAddr>,
    propose_acceptors: Vec<SocketAddr>,
    ballot_counter: u64,
    in_flight: HashMap<u64, Pending>,
}

impl Proposer {
    fn bump_ballot(&mut self) -> Ballot {
        self.ballot_counter += 1;
        Ballot(self.ballot_counter)
    }

    fn store(&mut self, from: SocketAddr, op: Op) -> Ballot{
        self.in_flight.insert(self.bump_ballot(), Pending {
            backref: from,
            op: op,
        })
    }
}

impl Reactor for Proposer {
    type Peer = SocketAddr;
    type Message = Rpc;

    fn receive(
        &mut self,
        at: SystemTime,
        from: Self::Peer,
        msg: Self::Message,
    ) -> Vec<(Self::Peer, Self::Message)> {
        match msg {
    Get => {
        let ballot = self.store(from, |x| x);

    },
    Del,
    Set(value),
    Cas(old_value, new_value),
    SetAcceptAcceptors(sas) => self.accept_acceptors = sas,
    SetProposeAcceptors(sas) => self.propose_acceptors = sas,
    ProposeRes(ballot, response) => {
            
    }
    AcceptRes(ballot, Result<(), Error>),
    other => panic!("proposer got unhandled rpc: {:?}", other),
        }
        unimplemented!()
    }
}
