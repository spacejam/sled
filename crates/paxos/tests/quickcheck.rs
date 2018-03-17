extern crate quickcheck;
extern crate rand;
extern crate paxos;

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::ops::Add;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use self::paxos::*;
use self::quickcheck::{Arbitrary, Gen, QuickCheck, StdGen};

#[derive(PartialOrd, Ord, Eq, PartialEq, Debug, Clone)]
struct Partition {
    at: SystemTime,
    duration: Duration,
    from: String,
    to: String,
}

impl Partition {
    fn generate<G: Gen>(
        g: &mut G,
        clients: usize,
        proposers: usize,
        acceptors: usize,
    ) -> Self {
        static NAMES: [&'static str; 3] = ["client:", "proposer:", "acceptor:"];

        let from_choice = g.gen_range(0, 3);
        let mut to_choice = g.gen_range(0, 3);

        while to_choice == from_choice {
            to_choice = g.gen_range(0, 3);
        }

        let at = UNIX_EPOCH.add(Duration::new(0, g.gen_range(0, 100)));
        let duration = Duration::new(0, g.gen_range(0, 100));

        let mut n = |choice| match choice {
            0 => g.gen_range(0, clients),
            1 => g.gen_range(0, proposers),
            2 => g.gen_range(0, acceptors),
            _ => panic!("too high"),
        };

        let from = format!("{}{}", NAMES[from_choice], n(from_choice));
        let to = format!("{}{}", NAMES[to_choice], n(to_choice));
        Partition {
            at: at,
            duration: duration,
            to: to,
            from: from,
        }
    }
}

#[derive(PartialOrd, Ord, Eq, PartialEq, Debug, Clone)]
enum ClientRequest {
    Get,
    Set(Vec<u8>),
    Cas(Option<Vec<u8>>, Option<Vec<u8>>),
    Del,
}

impl Arbitrary for ClientRequest {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        let choice = g.gen_range(0, 4);

        match choice {
            0 => ClientRequest::Get,
            1 => ClientRequest::Set(vec![g.gen_range(0, 5)]),
            2 => {
                ClientRequest::Cas(
                    if g.gen() {
                        Some(vec![g.gen_range(0, 5)])
                    } else {
                        None
                    },
                    if g.gen() {
                        Some(vec![g.gen_range(0, 5)])
                    } else {
                        None
                    },
                )
            }
            3 => ClientRequest::Del,
            _ => panic!("somehow generated 4+..."),
        }
    }
}

#[derive(Eq, PartialEq, Debug, Clone)]
struct ScheduledMessage {
    at: SystemTime,
    from: String,
    to: String,
    msg: Rpc,
}

// we implement Ord and PartialOrd to make the BinaryHeap
// act like a min-heap on time, rather than the default
// max-heap, so time progresses forwards.
impl Ord for ScheduledMessage {
    fn cmp(&self, other: &ScheduledMessage) -> Ordering {
        other.at.cmp(&self.at)
    }
}

impl PartialOrd for ScheduledMessage {
    fn partial_cmp(&self, other: &ScheduledMessage) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
enum Node {
    Acceptor(Acceptor),
    Proposer(Proposer),
    Client(Client),
}

impl Reactor for Node {
    type Peer = String;
    type Message = Rpc;

    fn receive(
        &mut self,
        at: SystemTime,
        from: Self::Peer,
        msg: Self::Message,
    ) -> Vec<(Self::Peer, Self::Message)> {
        match *self {
            Node::Proposer(ref mut inner) => inner.receive(at, from, msg),
            Node::Acceptor(ref mut inner) => inner.receive(at, from, msg),
            Node::Client(ref mut inner) => inner.receive(at, from, msg),
        }
    }
}

#[derive(Debug, Clone)]
struct Cluster {
    peers: HashMap<String, Node>,
    partitions: Vec<Partition>,
    in_flight: BinaryHeap<ScheduledMessage>,
    client_responses: Vec<ScheduledMessage>,
}

impl Cluster {
    fn step(&mut self) -> Option<()> {
        let pop = self.in_flight.pop();
        if let Some(sm) = pop {
            if sm.to.starts_with("client:") {
                // We'll check linearizability later
                // for client responses.
                self.client_responses.push(sm);
                return Some(());
            }
            let mut node = self.peers.remove(&sm.to).unwrap();
            let at = sm.at.clone();
            for (to, msg) in node.receive(sm.at, sm.from, sm.msg) {
                let from = &*sm.to;
                if self.is_partitioned(sm.at, &*to, from) {
                    // don't push this message on the priority queue
                    continue;
                }
                // TODO clock messin'
                let new_sm = ScheduledMessage {
                    at: at.add(Duration::new(0, 1)),
                    from: sm.to.clone(),
                    to: to,
                    msg: msg,
                };
                self.in_flight.push(new_sm);
            }
            self.peers.insert(sm.to, node);
            Some(())
        } else {
            None
        }
    }

    fn is_partitioned(&mut self, at: SystemTime, to: &str, from: &str) -> bool {
        let mut to_clear = vec![];
        let mut ret = false;
        for (i, partition) in self.partitions.iter().enumerate() {
            if partition.at > at {
                break;
            }

            if partition.at <= at && partition.at.add(partition.duration) < at {
                to_clear.push(i);
                continue;
            }

            // the partition is in effect at this time
            if &*partition.to == to && &*partition.from == from {
                ret = true;
                println!(
                    "partition {:?} effects message from {} to {} at {:?}",
                    partition,
                    from,
                    to,
                    at
                );
                break;
            }
        }

        // clear partitions that are no longer relevant
        for i in to_clear.into_iter().rev() {
            self.partitions.remove(i);
        }

        ret
    }
}

unsafe impl Send for Cluster {}

impl Arbitrary for Cluster {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        let n_clients = g.gen_range(1, 4);
        let client_addrs: Vec<String> =
            (0..n_clients).map(|i| format!("client:{}", i)).collect();

        let n_proposers = g.gen_range(1, 4);
        let proposer_addrs: Vec<String> = (0..n_proposers)
            .map(|i| format!("proposer:{}", i))
            .collect();

        let n_acceptors = g.gen_range(1, 4);
        let acceptor_addrs: Vec<String> = (0..n_acceptors)
            .map(|i| format!("acceptor:{}", i))
            .collect();

        let clients: Vec<(String, Node)> = client_addrs
            .iter()
            .map(|addr| {
                (
                    addr.clone(),
                    Node::Client(Client::new(proposer_addrs.clone())),
                )
            })
            .collect();

        let proposers: Vec<(String, Node)> = proposer_addrs
            .iter()
            .map(|addr| {
                let timeout_ms = g.gen_range(0, 10);
                (
                    addr.clone(),
                    Node::Proposer(
                        Proposer::new(timeout_ms, acceptor_addrs.clone()),
                    ),
                )
            })
            .collect();

        let acceptors: Vec<(String, Node)> = acceptor_addrs
            .iter()
            .map(|addr| (addr.clone(), Node::Acceptor(Acceptor::default())))
            .collect();

        let mut requests = vec![];

        for client_addr in client_addrs {
            let n_requests = g.gen_range(1, 10);

            for r in 0..n_requests {
                let msg = match ClientRequest::arbitrary(g) {
                    ClientRequest::Get => Rpc::Get(r),
                    ClientRequest::Set(v) => Rpc::Set(r, v),
                    ClientRequest::Cas(ov, nv) => Rpc::Cas(r, ov, nv),
                    ClientRequest::Del => Rpc::Del(r),
                };

                let at = g.gen_range(0, 100);

                requests.push(ScheduledMessage {
                    at: UNIX_EPOCH.add(Duration::new(0, at)),
                    from: client_addr.clone(),
                    to: g.choose(&proposer_addrs).unwrap().clone(),
                    msg: msg,
                });
            }
        }

        let n_partitions = g.gen_range(0, 10);
        let mut partitions = vec![];
        for _ in 0..n_partitions {
            partitions.push(Partition::generate(
                g,
                n_clients,
                n_proposers,
                n_acceptors,
            ));
        }
        partitions.sort();

        Cluster {
            peers: clients
                .into_iter()
                .chain(proposers.into_iter())
                .chain(acceptors.into_iter())
                .collect(),
            partitions: partitions,
            in_flight: requests.clone().into_iter().collect(),
            client_responses: vec![],
        }
    }

    fn shrink(&self) -> Box<Iterator<Item = Cluster>> {
        let mut ret = vec![];

        for i in 0..self.in_flight.len() {
            let mut in_flight: Vec<_> =
                self.in_flight.clone().into_iter().collect();
            in_flight.remove(i);
            let mut c = self.clone();
            c.in_flight = in_flight.into_iter().collect();
            ret.push(c);
        }

        Box::new(ret.into_iter())
    }
}

#[derive(PartialOrd, Ord, Eq, PartialEq, Debug, Clone)]
struct Interval {
    start: SystemTime,
    end: SystemTime,
    value: Option<Vec<u8>>,
}

fn check_linearizability(
    request_rpcs: Vec<ScheduledMessage>,
    response_rpcs: Vec<ScheduledMessage>,
) -> bool {
    // only look at successful completed responses
    let mut responses: HashMap<_, _> = response_rpcs
        .into_iter()
        .filter_map(|r| if let Rpc::ClientResponse(id, Ok(value)) = r.msg {
            Some((id, (r.at, value)))
        } else {
            None
        })
        .collect();

    // filter out requests that were not successful & completed
    let mut intervals: Vec<_> = request_rpcs
        .into_iter()
        .filter_map(|r| match r.msg {
            Rpc::Get(id) |
            Rpc::Del(id) |
            Rpc::Set(id, _) |
            Rpc::Cas(id, _, _) if responses.contains_key(&id) => {
                let (end, value) = responses.remove(&id).unwrap();
                Some(Interval {
                    start: r.at,
                    end: end,
                    value: value,
                })
            }
            _ => None,
        })
        .collect();

    intervals.sort();

    assert!(
        responses.is_empty(),
        "failed to match responses to initial requests"
    );

    // ensure that requests are

    true
}

fn prop_cluster_linearizability(mut cluster: Cluster) -> bool {
    let client_requests: Vec<_> =
        cluster.in_flight.clone().into_iter().collect();

    while let Some(_) = cluster.step() {}

    check_linearizability(client_requests, cluster.client_responses)
}

#[test]
fn test_quickcheck_pagecache_works() {
    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 100))
        .tests(1000)
        .max_tests(1000000)
        .quickcheck(prop_cluster_linearizability as fn(Cluster) -> bool);
}
