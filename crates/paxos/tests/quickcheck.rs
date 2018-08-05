extern crate paxos;
/// Simulation for network partitions. Like Jepsen but thousands of times faster.
extern crate quickcheck;
extern crate rand;

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
        static NAMES: [&'static str; 3] =
            ["client:", "proposer:", "acceptor:"];

        let from_choice = g.gen_range(0, 3);
        let mut to_choice = g.gen_range(0, 3);

        while to_choice == from_choice {
            to_choice = g.gen_range(0, 3);
        }

        let at =
            UNIX_EPOCH.add(Duration::new(0, g.gen_range(0, 100)));
        let duration = Duration::new(0, g.gen_range(0, 100));

        let mut n = |choice| match choice {
            0 => g.gen_range(0, clients),
            1 => g.gen_range(0, proposers),
            2 => g.gen_range(0, acceptors),
            _ => panic!("too high"),
        };

        let from =
            format!("{}{}", NAMES[from_choice], n(from_choice));
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
        // NB ONLY GENERATE CAS
        // the linearizability checker can't handle
        // anything else!
        let choice = g.gen_range(3, 10);

        match choice {
            0 => ClientRequest::Get,
            1 => ClientRequest::Del,
            2 => ClientRequest::Set(vec![g.gen_range(0, 2)]),
            _ => ClientRequest::Cas(
                if g.gen_weighted_bool(3) {
                    None
                } else {
                    Some(vec![g.gen_range(0, 2)])
                },
                Some(vec![g.gen_range(0, 2)]),
            ),
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
    fn partial_cmp(
        &self,
        other: &ScheduledMessage,
    ) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
enum Node {
    Acceptor(Acceptor<paxos::MemStorage>),
    Proposer(Proposer),
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
            Node::Proposer(ref mut inner) => {
                inner.receive(at, from, msg)
            }
            Node::Acceptor(ref mut inner) => {
                inner.receive(at, from, msg)
            }
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
            for (to, msg) in node.receive(sm.at, sm.from, sm.msg) {
                let from = &*sm.to;
                if self.is_partitioned(sm.at, &*to, from) {
                    // don't push this message on the priority queue
                    continue;
                }
                // TODO clock messin'
                let new_sm = ScheduledMessage {
                    at: sm.at.add(Duration::new(0, 1)),
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

    fn is_partitioned(
        &mut self,
        at: SystemTime,
        to: &str,
        from: &str,
    ) -> bool {
        let mut to_clear = vec![];
        let mut ret = false;
        for (i, partition) in self.partitions.iter().enumerate() {
            if partition.at > at {
                break;
            }

            if partition.at <= at
                && partition.at.add(partition.duration) < at
            {
                to_clear.push(i);
                continue;
            }

            // the partition is in effect at this time
            if &*partition.to == to && &*partition.from == from {
                ret = true;
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

        let proposers: Vec<(String, Node)> = proposer_addrs
            .iter()
            .map(|addr| {
                let timeout_ms = g.gen_range(0, 10);
                (
                    addr.clone(),
                    Node::Proposer(Proposer::new(
                        timeout_ms,
                        acceptor_addrs.clone(),
                    )),
                )
            })
            .collect();

        let acceptors: Vec<(String, Node)> = acceptor_addrs
            .iter()
            .map(|addr| {
                (addr.clone(), Node::Acceptor(Acceptor::default()))
            })
            .collect();

        let mut requests = vec![];
        let mut req_counter = 0;

        for client_addr in client_addrs {
            let n_requests = g.gen_range(1, 10);

            for _ in 0..n_requests {
                req_counter += 1;
                let k = g.gen_range(0, 3);
                let req = match ClientRequest::arbitrary(g) {
                    ClientRequest::Get => Req::Get(vec![k]),
                    ClientRequest::Set(v) => Req::Set(vec![k], v),
                    ClientRequest::Cas(ov, nv) => {
                        Req::Cas(vec![k], ov, nv)
                    }
                    ClientRequest::Del => Req::Del(vec![k]),
                };

                let msg = Rpc::ClientRequest(req_counter, req);

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
            peers: proposers
                .into_iter()
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
enum Act {
    Publish(Option<Vec<u8>>),
    Observe(Option<Vec<u8>>),
    Consume(Option<Vec<u8>>),
}

#[derive(PartialOrd, Ord, Eq, PartialEq, Debug, Clone)]
struct Event {
    at: SystemTime,
    act: Act,
    client_req_id: u64,
}

// simple (simplistic, not exhaustive) linearizability checker:
// main properties:
//   1. an effect must NOT be observed before
//      its causal operation starts
//   2. after its causal operation ends,
//      an effect MUST be observed
//
// for each successful operation end time
//   * populate a pending set for possibly
//     successful operations that start before
//     then (one pass ever, iterate along-side
//     the end times by start times)
//   * for each successful operation that reads
//     a previous write, ensure that it is present
//     in the write set.
//   * for each successful operation that "consumes"
//     a previous write (CAS, Del) we try to pop
//     its consumed value out of our pending set
//     after filling the pending set with any write
//     that could have happened before then.
//     if its not there, we failed linearizability.
//
fn check_linearizability(
    request_rpcs: Vec<ScheduledMessage>,
    response_rpcs: Vec<ScheduledMessage>,
) -> bool {
    use Req::*;
    // publishes "happen" as soon as a not-explicitly-failed
    // request begins
    let mut publishes = vec![];
    // observes "happen" at the end of a succesful response or
    // cas failure
    let mut observes = vec![];
    // consumes "happen" at the end of a successful response
    let mut consumes = vec![];

    let responses: std::collections::BTreeMap<_, _> = response_rpcs
        .into_iter()
        .filter_map(|r| {
            if let Rpc::ClientResponse(id, res) = r.msg {
                Some((id, (r.at, res)))
            } else {
                panic!("non-ClientResponse sent to client")
            }
        })
        .collect();

    for r in request_rpcs {
        let (id, req) = if let Rpc::ClientRequest(id, req) = r.msg {
            (id, req)
        } else {
            panic!("Cluster started with non-ClientRequest")
        };

        let begin = r.at;

        //  reasoning about effects:
        //
        //  OP  | res  | consumes | publishes | observes
        // -----------------------------------------
        //  CAS | ok   | old      | new       | old
        //  CAS | casf | -        | -         | actual
        //  CAS | ?    | ?        | ?         | ?
        //  DEL | ok   | ?        | None      | -
        //  DEL | ?    | ?        | None?     | -
        //  SET | ok   | ?        | value     | -
        //  SET | ?    | ?        | value?    | -
        //  GET | ok   | -        | -         | value
        //  GET | ?    | -        | -         | value?
        match responses.get(&id) {
            None |
            Some(&(_, Err(Error::Timeout))) |
            Some(&(_,
                   Err(Error::AcceptRejected {
                           ..
                       }))) => {
                // not sure if this actually took effect or not.
                // NB this is sort of weird, because even if an accept was
                // rejected by a majority, it may be used as a later message.
                // so as a client we have to treat it as being in a weird pending
                // state.
                match req {
                    Cas(k, _old, new) => publishes.push((k, begin, new, id)),
                    Del(k) => publishes.push((k, begin, None, id)),
                    Set(k, value) => {
                        publishes.push((k, begin, Some(value), id))
                    }
                    Get(_k) => {}
                }
            }
            Some(&(end, Ok(ref v))) => {
                match req {
                    Cas(k, old, new) => {
                        consumes.push((k.clone(), end, old.clone(), id));
                        observes.push((k.clone(), end, old, id));
                        publishes.push((k.clone(), begin, new, id));
                    }
                    Get(k) => observes.push((k, end, v.clone(), id)),
                    Del(k) => publishes.push((k, end, None, id)),
                    Set(k, value) => publishes.push((k, end, Some(value), id)),
                }
            }
            Some(&(end, Err(Error::CasFailed(ref witnessed)))) => {
                match req {
                    Cas(k, _old, _new) => {
                        observes.push((k, end, witnessed.clone(), id));
                    }
                    _ => panic!("non-cas request found for CasFailed response"),
                }
            }
            _ => {
                // propose/accept failure, no actionable info can be derived
            }
        }
    }

    let mut events_per_k = HashMap::new();

    for (k, time, value, id) in publishes {
        let mut events = events_per_k.entry(k).or_insert(vec![]);
        events.push(Event {
            at: time,
            act: Act::Publish(value),
            client_req_id: id,
        });
    }

    for (k, time, value, id) in consumes {
        let mut events = events_per_k.entry(k).or_insert(vec![]);
        events.push(Event {
            at: time,
            act: Act::Consume(value),
            client_req_id: id,
        });
    }

    for (k, time, value, id) in observes {
        let mut events = events_per_k.entry(k).or_insert(vec![]);
        events.push(Event {
            at: time,
            act: Act::Observe(value),
            client_req_id: id,
        });
    }

    for (_k, mut events) in events_per_k.into_iter() {
        events.sort();

        let mut value_pool = HashMap::new();
        value_pool.insert(None, 1);

        for event in events {
            // println!("k: {:?}, event: {:?}", _k, event);
            match event.act {
                Act::Publish(v) => {
                    let mut entry = value_pool.entry(v).or_insert(0);
                    *entry += 1;
                }
                Act::Observe(v) => {
                    let count = value_pool.get(&v).unwrap();
                    assert!(
                        *count > 0,
                        "expect to be able to witness {:?} at time {:?} for req {}",
                        v,
                        event.at,
                        event.client_req_id
                    )
                }
                Act::Consume(v) => {
                    let mut count = value_pool.get_mut(&v).unwrap();
                    assert!(*count > 0);
                    *count -= 1;
                }
            }
        }
    }

    true
}

fn prop_cluster_linearizability(mut cluster: Cluster) -> bool {
    let client_requests: Vec<_> =
        cluster.in_flight.clone().into_iter().collect();

    while let Some(_) = cluster.step() {}

    check_linearizability(client_requests, cluster.client_responses)
}

#[test]
fn test_quickcheck_paxos_linearizes() {
    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 100))
        .tests(10000)
        .max_tests(1000000)
        .quickcheck(
            prop_cluster_linearizability as fn(Cluster) -> bool,
        );
}

#[test]
fn linearizability_bug_01() {
    // postmortem: was not considering that requests that received
    // explicit AcceptRejected messages from a quorum could actually
    // be used as the input to a later round, as long as they landed
    // a single ACCEPT on any node.
    prop_cluster_linearizability(Cluster {
        peers: vec![
            (
                "acceptor:0".to_owned(),
                Node::Acceptor(Acceptor::default()),
            ),
            (
                "acceptor:1".to_owned(),
                Node::Acceptor(Acceptor::default()),
            ),
            (
                "acceptor:2".to_owned(),
                Node::Acceptor(Acceptor::default()),
            ),
            (
                "proposer:0".to_owned(),
                Node::Proposer(Proposer::new(
                    8,
                    vec![
                        "acceptor:0".to_owned(),
                        "acceptor:1".to_owned(),
                        "acceptor:2".to_owned(),
                    ],
                )),
            ),
            (
                "proposer:1".to_owned(),
                Node::Proposer(Proposer::new(
                    8,
                    vec![
                        "acceptor:0".to_owned(),
                        "acceptor:1".to_owned(),
                        "acceptor:2".to_owned(),
                    ],
                )),
            ),
            (
                "proposer:2".to_owned(),
                Node::Proposer(Proposer::new(
                    8,
                    vec![
                        "acceptor:0".to_owned(),
                        "acceptor:1".to_owned(),
                        "acceptor:2".to_owned(),
                    ],
                )),
            ),
        ].into_iter()
            .collect(),
        partitions: vec![],
        in_flight: vec![
            ScheduledMessage {
                at: UNIX_EPOCH.add(Duration::new(0, 6)),
                from: "client:1".to_owned(),
                to: "proposer:1".to_owned(),
                msg: Rpc::ClientRequest(
                    9,
                    Req::Cas(
                        b"k1".to_vec(),
                        Some(vec![1]),
                        Some(vec![1]),
                    ),
                ),
            },
            ScheduledMessage {
                at: UNIX_EPOCH.add(Duration::new(0, 20)),
                from: "client:0".to_owned(),
                to: "proposer:2".to_owned(),
                msg: Rpc::ClientRequest(
                    1,
                    Req::Cas(b"k1".to_vec(), None, Some(vec![1])),
                ),
            },
            ScheduledMessage {
                at: UNIX_EPOCH.add(Duration::new(0, 22)),
                from: "client:1".to_owned(),
                to: "proposer:1".to_owned(),
                msg: Rpc::ClientRequest(
                    11,
                    Req::Cas(
                        b"k1".to_vec(),
                        Some(vec![1]),
                        Some(vec![1]),
                    ),
                ),
            },
            ScheduledMessage {
                at: UNIX_EPOCH.add(Duration::new(0, 28)),
                from: "client:0".to_owned(),
                to: "proposer:0".to_owned(),
                msg: Rpc::ClientRequest(
                    6,
                    Req::Cas(
                        b"k1".to_vec(),
                        Some(vec![0]),
                        Some(vec![1]),
                    ),
                ),
            },
            ScheduledMessage {
                at: UNIX_EPOCH.add(Duration::new(0, 30)),
                from: "client:0".to_owned(),
                to: "proposer:2".to_owned(),
                msg: Rpc::ClientRequest(
                    3,
                    Req::Cas(
                        b"k1".to_vec(),
                        Some(vec![0]),
                        Some(vec![1]),
                    ),
                ),
            },
            ScheduledMessage {
                at: UNIX_EPOCH.add(Duration::new(0, 45)),
                from: "client:0".to_owned(),
                to: "proposer:2".to_owned(),
                msg: Rpc::ClientRequest(
                    4,
                    Req::Cas(
                        b"k1".to_vec(),
                        Some(vec![0]),
                        Some(vec![0]),
                    ),
                ),
            },
            ScheduledMessage {
                at: UNIX_EPOCH.add(Duration::new(0, 45)),
                from: "client:0".to_owned(),
                to: "proposer:2".to_owned(),
                msg: Rpc::ClientRequest(
                    7,
                    Req::Cas(
                        b"k1".to_vec(),
                        Some(vec![0]),
                        Some(vec![1]),
                    ),
                ),
            },
            ScheduledMessage {
                at: UNIX_EPOCH.add(Duration::new(0, 51)),
                from: "client:0".to_owned(),
                to: "proposer:2".to_owned(),
                msg: Rpc::ClientRequest(
                    8,
                    Req::Cas(
                        b"k1".to_vec(),
                        Some(vec![1]),
                        Some(vec![1]),
                    ),
                ),
            },
            ScheduledMessage {
                at: UNIX_EPOCH.add(Duration::new(0, 66)),
                from: "client:0".to_owned(),
                to: "proposer:0".to_owned(),
                msg: Rpc::ClientRequest(
                    2,
                    Req::Cas(
                        b"k1".to_vec(),
                        Some(vec![0]),
                        Some(vec![1]),
                    ),
                ),
            },
            ScheduledMessage {
                at: UNIX_EPOCH.add(Duration::new(0, 84)),
                from: "client:1".to_owned(),
                to: "proposer:1".to_owned(),
                msg: Rpc::ClientRequest(
                    10,
                    Req::Cas(
                        b"k1".to_vec(),
                        Some(vec![0]),
                        Some(vec![0]),
                    ),
                ),
            },
            ScheduledMessage {
                at: UNIX_EPOCH.add(Duration::new(0, 87)),
                from: "client:0".to_owned(),
                to: "proposer:1".to_owned(),
                msg: Rpc::ClientRequest(
                    5,
                    Req::Cas(
                        b"k1".to_vec(),
                        Some(vec![0]),
                        Some(vec![1]),
                    ),
                ),
            },
        ].into_iter()
            .collect(),
        client_responses: vec![],
    });
}
