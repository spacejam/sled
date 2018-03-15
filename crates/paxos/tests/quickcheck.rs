#[macro_use]
extern crate quickcheck;
extern crate rand;
extern crate paxos;

use std::collections::{BinaryHeap, HashMap};
use std::ops::Add;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use self::paxos::*;
use self::quickcheck::{Arbitrary, Gen};

#[derive(PartialOrd, Ord, Eq, PartialEq, Debug, Clone)]
struct ScheduledMessage {
    at: SystemTime,
    from: String,
    to: String,
    msg: Rpc,
}

#[derive(Debug, Clone)]
enum Node {
    Acceptor(Acceptor),
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
            Node::Proposer(ref mut proposer) => proposer.receive(at, from, msg),
            Node::Acceptor(ref mut acceptor) => acceptor.receive(at, from, msg),
        }
    }
}

#[derive(Debug, Clone)]
struct Cluster {
    peers: HashMap<String, Node>,
    omniscient_time: u64,
    in_flight: BinaryHeap<ScheduledMessage>,
}

impl Cluster {
    fn step(&mut self) -> Option<Result<(), ()>> {
        let pop = self.in_flight.pop();
        if let Some(sm) = pop {
            // println!("trying to get peer {:?} from {:?}", sm.to, self.peers);
            if sm.to == "BIG_GOD_DADDY" {
                // TODO handle god stuff
                println!("god got {:?}", sm.msg);
                return Some(Ok(()));
            }
            let node = self.peers.get_mut(&sm.to).unwrap();
            let at = sm.at.clone();
            for (to, msg) in node.receive(sm.at, sm.from, sm.msg) {
                // TODO partitions
                // TODO clock messin'
                let new_sm = ScheduledMessage {
                    at: at.add(Duration::new(0, 1)),
                    from: sm.to.clone(),
                    to: to,
                    msg: msg,
                };
                self.in_flight.push(new_sm);
            }
            Some(Ok(()))
        } else {
            None
        }
    }
}

unsafe impl Send for Cluster {}

impl Arbitrary for Cluster {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        let god_addr_1 = "BIG_GOD_DADDY".to_owned();
        let proposer_addr = "1.1.1.1:1".to_owned();
        let acceptor_addr = "2.2.2.2:2".to_owned();

        let proposer = Proposer::new(vec![acceptor_addr.clone()]);

        let acceptor = Acceptor::default();

        let initial_messages = vec![
            ScheduledMessage {
                at: UNIX_EPOCH.add(Duration::new(0, 1_000_000_001)),
                from: god_addr_1.clone(),
                to: proposer_addr.clone(),
                msg: Rpc::Set(1, b"yo".to_vec()),
            },
            ScheduledMessage {
                at: UNIX_EPOCH.add(Duration::new(0, 9_000_000)),
                from: god_addr_1.clone(),
                to: proposer_addr.clone(),
                msg: Rpc::Cas(1, Some(b"yo".to_vec()), Some(b"boo".to_vec())),
            },
            ScheduledMessage {
                at: UNIX_EPOCH.add(Duration::new(0, 8_000_000)),
                from: god_addr_1.clone(),
                to: proposer_addr.clone(),
                msg: Rpc::Get(1),
            },
        ].into_iter()
            .collect();

        let peers = vec![
            (proposer_addr, Node::Proposer(proposer)),
            (acceptor_addr, Node::Acceptor(acceptor)),
        ].into_iter()
            .collect();

        Cluster {
            peers: peers,
            omniscient_time: 1_000_000_000,
            in_flight: initial_messages,
        }
    }
}

quickcheck! {
    fn check_cluster(
        cluster: Cluster
    ) -> bool {
        let mut cluster = cluster;

        while let Some(r) = cluster.step() {
            r.unwrap()
        } 

        true
    }
}
