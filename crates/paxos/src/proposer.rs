use super::*;

use std::collections::HashMap;

type Op = Box<Fn(Option<Value>) -> Option<Value>>;

#[derive(Eq, PartialEq, Debug)]
enum Phase {
    Propose,
    Accept,
}

struct Pending {
    backref: SocketAddr,
    op: Op,
    new_v: Option<Value>,
    phase: Phase,
    waiting_for: Vec<SocketAddr>,
    acks_from: Vec<SocketAddr>,
    highest_promise_ballot: Ballot,
    highest_promise_value: Option<Value>,
}

pub struct Proposer {
    accept_acceptors: Vec<SocketAddr>,
    propose_acceptors: Vec<SocketAddr>,
    ballot_counter: u64,
    in_flight: HashMap<Ballot, Pending>,
}

impl Proposer {
    fn bump_ballot(&mut self) -> Ballot {
        self.ballot_counter += 1;
        Ballot(self.ballot_counter)
    }

    fn propose(&mut self, from: SocketAddr, op: Op) -> Vec<(SocketAddr, Rpc)> {
        let ballot = self.bump_ballot();
        self.in_flight.insert(
            ballot.clone(),
            Pending {
                backref: from,
                op: op,
                new_v: None,
                phase: Phase::Propose,
                waiting_for: self.propose_acceptors.clone(),
                acks_from: vec![],
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
    type Peer = SocketAddr;
    type Message = Rpc;

    fn receive(
        &mut self,
        at: SystemTime,
        from: Self::Peer,
        msg: Self::Message,
    ) -> Vec<(Self::Peer, Self::Message)> {
        let mut clear_ballot = None;
        let res = match msg {
            Get => self.propose(from, Box::new(|x| x)),
            Del => self.propose(from, Box::new(|_| None)),
            Set(value) => {
                self.propose(from, Box::new(move |_| Some(value.clone())))
            }
            Cas(old_value, new_value) => {
                self.propose(
                    from,
                    Box::new(move |old| if old == old_value {
                        new_value.clone()
                    } else {
                        old
                    }),
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
                if !self.in_flight.contains_key(&req_ballot) || res.is_err() {
                    // we've already moved on, or some nerd
                    // didn't like our request...
                    return vec![];
                }

                let mut pending = self.in_flight.get_mut(&req_ballot).unwrap();
                let response = res.unwrap();

                if pending.phase != Phase::Propose {
                    // we've already moved on
                    return vec![];
                }

                assert!(
                    !pending.acks_from.contains(&from),
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
                    pending.phase = Phase::Accept;
                    pending.waiting_for = self.accept_acceptors.clone();
                    pending.acks_from = vec![];
                    pending.new_v =
                        (pending.op)(pending.highest_promise_value.clone());

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
                let response = res.unwrap();

                assert_eq!(
                    pending.phase,
                    Phase::Accept,
                    "somehow we went back in time and became a proposal..."
                );

                assert!(
                    !pending.acks_from.contains(&from),
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

                pending.acks_from.push(from);

                let required_acks = (pending.waiting_for.len() / 2) + 1;

                if pending.acks_from.len() >= required_acks {
                    // respond favorably to the client and nuke pending
                    vec![
                        (
                            pending.backref,
                            ClientResponse(Ok(pending.new_v.clone()))
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
    fn tick(&mut self, at: SystemTime) -> Vec<(Self::Peer, Self::Message)> {
        vec![]
    }
}
