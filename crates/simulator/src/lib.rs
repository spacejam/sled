#[macro_use]
extern crate proptest;
extern crate deterministic;

use deterministic::{Reactor, now, set_time};
use proptest::prelude::*;

#[macro_export]
macro_rules! simulate {
    () => {}
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

#[derive(Debug, Clone)]
struct Cluster<R>
    where R: Reactor
{
    peers: HashMap<String, R>,
    partitions: Vec<Partition>,
    in_flight: BinaryHeap<Event>,
}

unsafe impl<R> Send for Cluster<R> {}

impl Cluster {
    fn step(&mut self) -> Option<()> {
        let pop = self.in_flight.pop();
        None
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
