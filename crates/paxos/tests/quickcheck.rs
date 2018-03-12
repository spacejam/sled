#[macro_use]
extern crate quickcheck;
extern crate rand;
extern crate paxos;

use self::paxos::*;
use self::quickcheck::{Arbitrary, Gen};

#[derive(Clone, Debug)]
struct Partition {
    from: u8,
    to: u8,
    starts_at: u64,
    duration: u64,
}

impl Arbitrary for Partition {
    fn arbitrary<G: Gen>(g: &mut G) -> Partition {
        Partition {
            from: g.gen(),
            to: g.gen(),
            starts_at: g.gen_range(0, 1000),
            duration: g.gen_range(0, 1000),
        }
    }
}

struct Cluster {
    peers: Vec<ElectionPeer<u8>>,
}

impl Cluster {
    fn new(n_peers: u8) -> Cluster {
        let mut peers = vec![];

        for i in 0..n_peers {
            let mut peer_ids: Vec<u8> = (0..n_peers).collect();
            peer_ids.remove(i as usize);

            peers.push(ElectionPeer {
                state: PeerState::Init {
                    since: 0,
                },
                peers: peer_ids,
                epoch: 0,
                clock: Box::new(0),
                transport: Box::new(vec![]),
            });
        }

        Cluster {
            peers: peers,
        }
    }

    fn idx(&self, peer: u8) -> usize {
        peer as usize % self.peers.len()
    }

    fn tick_node(
        &mut self,
        at: u64,
        node_id: u8,
    ) -> Vec<(u8, ElectionMessage)> {
        let idx = self.idx(node_id);
        self.peers[idx].clock = Box::new(at);
        self.peers[idx].tick();
        self.peers[idx].transport.drain(0..).collect();
    }

    fn receive_node(
        &mut self,
        at: u64,
        to: u8,
        from: u8,
        msg: ElectionMessage,
    ) -> Vec<(u8, ElectionMessage)> {
        let idx = self.idx(to);
        self.peers[idx].clock = Box::new(at);
        self.peers[idx].receive(from, msg);
        self.peers[idx].transport.drain(0..).collect();
    }
}

fn partitioned(partitions: &Vec<Partition>, at: u64, from: u8, to: u8) -> bool {
    for partition in partitions {
        if partitioned.from == from && partitioned.to == to &&
            partitioned.starts_at <= at &&
            partitioned.duration + partitioned.starts_at > at
        {
            return true;
        }
    }
    false
}

quickcheck! {
    fn election_maintains_invariants(
        partitions: Vec<Partition>, 
        ticks: Vec<(u8, u64)>, 
        n_peers: u8
    ) -> bool {
        let mut cluster = Cluster::new(n_peers);
        let mut time = 0;
        let mut deliveries = std::collections::BinaryHeap::new();
        
        for (to, after) in ticks {
            loop {
                if let Some((delivery_time, _, _, _)) = deliveries.peek() {
                    if delivery_time >= time + after {
                        break
                    }
                } else {
                    break
                }

                let (delivery_time, from, to, msg) = deliveries.pop();
                if !partitioned(partitions, delivery_time, from, to) {
                    let outgoing = cluster.receive_node(time, to, from, msg);
                    // TODO add each thing in outgoing to deliveries
                }
                cluster.verify_invariants();
            }

            time += after;
            let outgoing = cluster.tick_node(time, to % n_peers);
            // TODO add each thing in outgoing to deliveries
            cluster.verify_invariants();
        }

        true
    }
}
