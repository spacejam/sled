use super::*;

use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;

struct Op(Box<Fn(Option<Value>) -> Option<Value>>);

impl fmt::Debug for Op {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Op(f(Option<Value>)->Option<Value>)")
    }
}


#[derive(Eq, PartialEq, Debug, Clone)]
enum Phase {
    Propose,
    Accept,
}

#[derive(Clone)]
pub struct Pending {
    client_addr: String,
    id: u64,
    op: Rc<Op>,
    new_v: Option<Value>,
    phase: Phase,
    waiting_for: Vec<String>,
    acks_from: Vec<String>,
    nacks_from: Vec<String>,
    highest_promise_ballot: Ballot,
    highest_promise_value: Option<Value>,
}

impl fmt::Debug for Pending {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Pending {{ 
                client_addr: {},
                id: {}, 
                new_v: {:?}, 
                phase: {:?}, 
                waiting_for: {:?},
                acks_from: {:?},
                nacks_from: {:?},
                highest_promise_ballot: {:?},
                highest_promise_value: {:?},
            }}",
            self.client_addr,
            self.id,
            self.new_v,
            self.phase,
            self.waiting_for,
            self.acks_from,
            self.nacks_from,
            self.highest_promise_ballot,
            self.highest_promise_value,
        )
    }
}

#[derive(Default, Debug, Clone)]
pub struct Proposer {
    pub accept_acceptors: Vec<String>,
    pub propose_acceptors: Vec<String>,
    pub ballot_counter: u64,
    pub in_flight: HashMap<Ballot, Pending>,
}

impl Proposer {
    pub fn new(proposers: Vec<String>) -> Proposer {
        let mut ret = Proposer::default();
        ret.accept_acceptors = proposers.clone();
        ret.propose_acceptors = proposers;
        ret
    }

    fn bump_ballot(&mut self) -> Ballot {
        self.ballot_counter += 1;
        Ballot(self.ballot_counter)
    }

    fn propose(&mut self, from: String, id: u64, op: Op) -> Vec<(String, Rpc)> {
        let ballot = self.bump_ballot();
        self.in_flight.insert(
            ballot.clone(),
            Pending {
                client_addr: from,
                id: id,
                op: Rc::new(op),
                new_v: None,
                phase: Phase::Propose,
                waiting_for: self.propose_acceptors.clone(),
                acks_from: vec![],
                nacks_from: vec![],
                highest_promise_ballot: Ballot(0),
                highest_promise_value: None,
            },
        );

        self.propose_acceptors
            .iter()
            .map(|a| (a.clone(), ProposeReq(ballot.clone())))
            .collect()
    }
}

impl Reactor for Proposer {
    type Peer = String;
    type Message = Rpc;

    fn receive(
        &mut self,
        _at: SystemTime,
        from: Self::Peer,
        msg: Self::Message,
    ) -> Vec<(Self::Peer, Self::Message)> {
        let mut clear_ballot = None;
        let res =
            match msg {
                Get(id) => self.propose(from, id, Op(Box::new(|x| x))),
                Del(id) => self.propose(from, id, Op(Box::new(|_| None))),
                Set(id, value) => {
                    self.propose(
                        from,
                        id,
                        Op(Box::new(move |_| Some(value.clone()))),
                    )
                }
                Cas(id, old_value, new_value) => {
                    self.propose(
                        from,
                        id,
                        Op(Box::new(move |old| if old == old_value {
                            new_value.clone()
                        } else {
                            old
                        })),
                    )
                }
                SetAcceptAcceptors(sas) => {
                    self.accept_acceptors = sas;
                    vec![]
                }
                SetProposeAcceptors(sas) => {
                    self.propose_acceptors = sas;
                    vec![]
                }
                ProposeRes {
                    req_ballot,
                    last_accepted_ballot,
                    last_accepted_value,
                    res,
                } => {
                    if self.ballot_counter < last_accepted_ballot.0 {
                        self.ballot_counter = last_accepted_ballot.0;
                    }

                    if !self.in_flight.contains_key(&req_ballot) {
                        // we've already moved on
                        return vec![];
                    }

                    let mut pending =
                        self.in_flight.get_mut(&req_ballot).unwrap();

                    if pending.phase != Phase::Propose {
                        // we've already moved on
                        return vec![];
                    }

                    assert!(
                        !pending.acks_from.contains(&from) &&
                            !pending.nacks_from.contains(&from),
                        "somehow got a response from this peer already... \
                    we don't do retries in this game yet!"
                    );

                    assert!(
                        pending.waiting_for.contains(&from),
                        "somehow got a response from someone we didn't send \
                    a request to... maybe the network is funky and we \
                    should use a higher level identifier to identify them \
                    than their network address."
                    );

                    if res.is_err() {
                        // some nerd didn't like our request...
                        pending.nacks_from.push(from);

                        let majority = (pending.waiting_for.len() / 2) + 1;

                        return if pending.nacks_from.len() >= majority {
                            vec![
                                (
                                    pending.client_addr.clone(),
                                    ClientResponse(
                                        pending.id,
                                        Err(res.unwrap_err()),
                                    )
                                ),
                            ]
                        } else {
                            vec![]
                        };
                    }

                    assert!(
                        req_ballot.0 > pending.highest_promise_ballot.0,
                        "somehow the acceptor promised us a vote even though \
                    their highest promise ballot is higher than our request..."
                    );

                    pending.acks_from.push(from);

                    if last_accepted_ballot > pending.highest_promise_ballot {
                        pending.highest_promise_ballot = last_accepted_ballot;
                        pending.highest_promise_value = last_accepted_value;
                    }

                    let required_acks = (pending.waiting_for.len() / 2) + 1;

                    if pending.acks_from.len() >= required_acks {
                        // transition to ACCEPT phase
                        // NB assumption: we use CURRENT acceptor list,
                        // rather than the acceptor list when we received
                        // the client request. need to think on this more.
                        pending.phase = Phase::Accept;
                        pending.waiting_for = self.accept_acceptors.clone();
                        pending.acks_from = vec![];
                        pending.nacks_from = vec![];
                        pending.new_v = (pending.op.0)(
                            pending.highest_promise_value.clone(),
                        );

                        self.accept_acceptors
                            .iter()
                            .map(|a| {
                                (
                                    a.clone(),
                                    AcceptReq(
                                        req_ballot.clone(),
                                        pending.new_v.clone(),
                                    ),
                                )
                            })
                            .collect()
                    } else {
                        // still waiting for promises
                        vec![]
                    }
                }
                AcceptRes(ballot, res) => {
                    if !self.in_flight.contains_key(&ballot) || res.is_err() {
                        // we've already moved on, or some nerd
                        // didn't like our request...
                        return vec![];
                    }

                    let mut pending = self.in_flight.get_mut(&ballot).unwrap();

                    assert_eq!(
                        pending.phase,
                        Phase::Accept,
                        "somehow we went back in time and became a proposal..."
                    );

                    assert!(
                        !pending.acks_from.contains(&from) &&
                            !pending.nacks_from.contains(&from),
                        "somehow got a response from this peer already... \
                    we don't do retries in this game yet!"
                    );

                    assert!(
                        pending.waiting_for.contains(&from),
                        "somehow got a response from someone we didn't send \
                    a request to... maybe the network is funky and we \
                    should use a higher level identifier to identify them \
                    than their network address."
                    );

                    if res.is_err() {
                        // some nerd didn't like our request...
                        pending.nacks_from.push(from);

                        let majority = (pending.waiting_for.len() / 2) + 1;

                        return if pending.nacks_from.len() >= majority {
                            vec![
                                (
                                    pending.client_addr.clone(),
                                    ClientResponse(
                                        pending.id,
                                        Err(res.unwrap_err()),
                                    )
                                ),
                            ]
                        } else {
                            vec![]
                        };
                    }

                    pending.acks_from.push(from);

                    let required_acks = (pending.waiting_for.len() / 2) + 1;

                    if pending.acks_from.len() >= required_acks {
                        // respond favorably to the client and nuke pending
                        vec![
                            (
                                pending.client_addr.clone(),
                                ClientResponse(
                                    pending.id,
                                    Ok(pending.new_v.clone()),
                                )
                            ),
                        ]
                    } else {
                        // still waiting for acceptances
                        vec![]
                    }

                }
                other => panic!("proposer got unhandled rpc: {:?}", other),
            };

        if let Some(ballot) = clear_ballot.take() {
            self.in_flight.remove(&ballot);
        }

        res
    }

    // TODO we use tick to handle timeouts
    fn tick(&mut self, _at: SystemTime) -> Vec<(Self::Peer, Self::Message)> {
        vec![]
    }
}
