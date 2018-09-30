extern crate bincode;
extern crate deterministic;
extern crate serde;
extern crate simulator;
#[macro_use]
extern crate serde_derive;

use std::collections::HashMap;

use deterministic::Reactor;

type ReqId = usize;

struct IdGen {
    peers: Vec<String>,
    in_flight: HashMap<ReqId, Pending>,
    req_counter: usize,
}

struct IdGenConf {
    peers: Vec<String>,
}

struct Pending {
    accepted: usize,
    rejected: usize,
    from: String,
    their_req_id: usize,
}

#[derive(Debug, Serialize, Deserialize)]
enum Msg {
    ClientReq { req_id: usize },
    ClientRes { req_id: usize, new_id: usize },
    ProposeId { req_id: usize },
    AcceptId { req_id: usize },
    RejectId { req_id: usize, highest: usize },
}

use Msg::*;

impl IdGen {
    fn propose(
        &mut self,
        from: String,
        their_req_id: usize,
    ) -> Vec<(String, Msg)> {
        self.req_counter += 1;
        let req_id = self.req_counter;
        self.in_flight.insert(
            req_id,
            Pending {
                accepted: 0,
                rejected: 0,
                from: from,
                their_req_id: their_req_id,
            },
        );
        self.peers
            .iter()
            .map(|p| (p.clone(), ProposeId { req_id }))
            .collect()
    }
}

impl Reactor for IdGen {
    type Config = IdGenConf;
    type Error = ();
    type Peer = String;
    type Message = Msg;

    fn start(config: IdGenConf) -> Result<IdGen, ()> {
        Ok(IdGen {
            peers: config.peers,
            in_flight: HashMap::new(),
            req_counter: 0,
        })
    }

    fn receive(
        &mut self,
        from: Self::Peer,
        msg: Self::Message,
    ) -> Vec<(Self::Peer, Self::Message)> {
        match msg {
            ClientReq { req_id } => self.propose(from, req_id),
            ClientRes { .. } => {
                panic!("somehow received a ClientRes: {:?}", msg)
            }
            ProposeId { req_id } => {
                if req_id > self.req_counter {
                    self.req_counter = req_id;
                    vec![(from, AcceptId { req_id })]
                } else {
                    vec![(
                        from,
                        RejectId {
                            req_id: req_id,
                            highest: self.req_counter,
                        },
                    )]
                }
            }
            AcceptId { req_id } => {
                let mut pending = self.in_flight.remove(&req_id);
                if let Some(mut pending) = pending {
                    pending.accepted += 1;
                    if pending.accepted > self.peers.len() / 2 {
                        vec![(
                            pending.from,
                            ClientRes {
                                req_id: pending.their_req_id,
                                new_id: req_id,
                            },
                        )]
                    } else {
                        self.in_flight.insert(req_id, pending);
                        vec![]
                    }
                } else {
                    vec![]
                }
            }
            RejectId { req_id, highest } => {
                if highest > self.req_counter {
                    self.req_counter = highest;
                }
                let mut pending = self.in_flight.remove(&req_id);
                if let Some(mut pending) = pending {
                    pending.rejected += 1;
                    if pending.rejected > self.peers.len() / 2 {
                        // retry
                        self.propose(
                            pending.from,
                            pending.their_req_id,
                        )
                    } else {
                        self.in_flight.insert(req_id, pending);
                        vec![]
                    }
                } else {
                    vec![]
                }
            }
        }
    }
}

#[test]
fn idgen_is_linearizable() {
    /*
    simulate! {
        IdGen 1..3,
        Get() -> usize {

        }
    }
    */
}
