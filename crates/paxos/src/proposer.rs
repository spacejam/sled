use super::*;

use std::collections::HashMap;
use std::fmt;

#[derive(Eq, PartialEq, Debug, Clone)]
enum Phase {
    Propose,
    Accept,
}

#[derive(Clone)]
pub struct Pending {
    client_addr: String,
    id: u64,
    req: Req,
    new_v: Option<Value>,
    phase: Phase,
    waiting_for: Vec<String>,
    acks_from: Vec<String>,
    nacks_from: Vec<String>,
    highest_promise_ballot: Ballot,
    highest_promise_value: Option<Value>,
    received_at: SystemTime,
    cas_failed: Result<(), Error>,
    has_retried_once: bool,
}

impl Pending {
    fn apply_op(&mut self) {
        match self.req {
            Req::Get => {
                self.new_v = self.highest_promise_value.clone();
            }
            Req::Del => {
                self.new_v = None;
            }
            Req::Set(ref new_v) => {
                self.new_v = Some(new_v.clone());
            }
            Req::Cas(ref old_v, ref new_v) => {
                if *old_v == self.highest_promise_value {
                    self.new_v = new_v.clone();
                } else {
                    self.new_v = self.highest_promise_value.clone();
                    self.cas_failed = Err(Error::CasFailed(
                        self.highest_promise_value.clone(),
                    ));
                }
            }
        }
    }

    fn transition_to_accept(&mut self, acceptors: Vec<String>) {
        self.phase = Phase::Accept;
        self.acks_from = vec![];
        self.nacks_from = vec![];
        self.waiting_for = acceptors;
        self.apply_op();
    }
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
                received_at: {:?},
                cas_failed: {:?},
                has_retried_once: {},
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
            self.received_at,
            self.cas_failed,
            self.has_retried_once,
        )
    }
}

#[derive(Default, Debug, Clone)]
pub struct Proposer {
    accept_acceptors: Vec<String>,
    propose_acceptors: Vec<String>,
    ballot_counter: u64,
    in_flight: HashMap<Ballot, Pending>,
    timeout: Duration,
}

impl Proposer {
    pub fn new(timeout_ms: u64, proposers: Vec<String>) -> Proposer {
        let mut ret = Proposer::default();
        ret.accept_acceptors = proposers.clone();
        ret.propose_acceptors = proposers;
        ret.timeout = Duration::from_millis(timeout_ms);
        ret
    }

    fn bump_ballot(&mut self) -> Ballot {
        self.ballot_counter += 1;
        Ballot(self.ballot_counter)
    }

    fn propose(
        &mut self,
        at: SystemTime,
        from: String,
        id: u64,
        req: Req,
        retry: bool,
    ) -> Vec<(String, Rpc)> {
        let ballot = self.bump_ballot();
        self.in_flight.insert(
            ballot.clone(),
            Pending {
                client_addr: from,
                id: id,
                req: req,
                new_v: None,
                phase: Phase::Propose,
                waiting_for: self.propose_acceptors.clone(),
                acks_from: vec![],
                nacks_from: vec![],
                highest_promise_ballot: Ballot(0),
                highest_promise_value: None,
                received_at: at,
                cas_failed: Ok(()),
                has_retried_once: retry,
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
        at: SystemTime,
        from: Self::Peer,
        msg: Self::Message,
    ) -> Vec<(Self::Peer, Self::Message)> {
        let mut clear_ballot = None;
        let mut retry = None;
        let res = match msg {
            ClientRequest(id, r) => self.propose(at, from, id, r, false),
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
                if let Err(Error::ProposalRejected {
                               ref last,
                           }) = res
                {
                    if self.ballot_counter < last.0 {
                        self.ballot_counter = last.0;
                    }
                }

                if !self.in_flight.contains_key(&req_ballot) {
                    // we've already moved on
                    return vec![];
                }

                let mut pending = self.in_flight.get_mut(&req_ballot).unwrap();

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

                if let Err(e) = res {
                    // some nerd didn't like our request...
                    pending.nacks_from.push(from);

                    let majority = (pending.waiting_for.len() / 2) + 1;

                    if pending.nacks_from.len() >= majority {
                        clear_ballot = Some(req_ballot.clone());

                        if e.is_rejected_proposal() &&
                            !pending.has_retried_once
                        {
                            retry = Some((
                                pending.received_at,
                                pending.client_addr.clone(),
                                pending.id,
                                pending.req.clone(),
                            ));
                            vec![]
                        } else {
                            vec![
                                (
                                    pending.client_addr.clone(),
                                    ClientResponse(pending.id, Err(e))
                                ),
                            ]
                        }
                    } else {
                        vec![]
                    }
                } else {
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
                        pending.transition_to_accept(
                            self.accept_acceptors.clone(),
                        );

                        pending
                            .waiting_for
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
            }
            AcceptRes(ballot, res) => {
                if let Err(Error::AcceptRejected {
                               ref last,
                           }) = res
                {
                    if self.ballot_counter < last.0 {
                        self.ballot_counter = last.0;
                    }
                }

                if !self.in_flight.contains_key(&ballot) {
                    // we've already moved on
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

                    if pending.nacks_from.len() >= majority {
                        clear_ballot = Some(ballot);
                        vec![
                            (
                                pending.client_addr.clone(),
                                ClientResponse(pending.id, Err(res.unwrap_err()))
                            ),
                        ]
                    } else {
                        vec![]
                    }
                } else {
                    pending.acks_from.push(from);

                    let required_acks = (pending.waiting_for.len() / 2) + 1;

                    if pending.acks_from.len() >= required_acks {
                        // respond favorably to the client and nuke pending
                        vec![
                            (
                                pending.client_addr.clone(),
                                ClientResponse(
                                    pending.id,
                                    pending.cas_failed.clone().map(
                                        |_| pending.new_v.clone(),
                                    ),
                                )
                            ),
                        ]
                    } else {
                        // still waiting for acceptances
                        vec![]
                    }

                }
            }
            other => panic!("proposer got unhandled rpc: {:?}", other),
        };

        if let Some(ballot) = clear_ballot.take() {
            self.in_flight.remove(&ballot);
        }

        if let Some((received_at, client_addr, id, req)) = retry {
            self.propose(received_at, client_addr, id, req, true)
        } else {
            res
        }
    }

    // we use tick to handle timeouts
    fn tick(&mut self, at: SystemTime) -> Vec<(Self::Peer, Self::Message)> {
        let ret = {
            let late = self.in_flight.values().filter(|i| {
                at.duration_since(i.received_at).unwrap() > self.timeout
            });

            late.map(|pending| {
                (
                    pending.client_addr.clone(),
                    ClientResponse(pending.id, Err(Error::Timeout)),
                )
            }).collect()
        };

        let timeout = self.timeout.clone();
        self.in_flight.retain(|_, i| {
            at.duration_since(i.received_at).unwrap() <= timeout
        });

        ret
    }
}
