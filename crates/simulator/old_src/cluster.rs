use super::*;

#[derive(Debug, Clone)]
struct Cluster {
    peers: HashMap<String, impl Reactor>,
    partitions: Vec<Partition>,
    in_flight: BinaryHeap<Event>,
}

unsafe impl Send for Cluster {}

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

impl Arbitrary for Cluster {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        Cluster {
            peers: vec![],
            partitions: vec![],
            in_flight: vec![].into_iter().collect(),
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
