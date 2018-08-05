extern crate deterministic;
extern crate quickcheck;

use std::collections::{BinaryHeap, HashMap};
use std::fmt::Debug;
use std::ops::Add;
use std::time::{Duration, SystemTime};

use deterministic::Reactor;
use quickcheck::{Arbitrary, Gen};

#[macro_export]
macro_rules! simulate {
    () => {};
}

impl<R> Arbitrary for Cluster<R>
where
    R: 'static + Reactor + Clone,
    R::Peer: Debug + Clone,
{
    fn arbitrary<G: Gen>(_g: &mut G) -> Self {
        Cluster {
            peers: HashMap::new(),
            partitions: vec![],
            in_flight: vec![].into_iter().collect(),
        }
    }

    fn shrink(&self) -> Box<Iterator<Item = Cluster<R>>> {
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
struct Partition<R>
where
    R: Reactor,
{
    at: SystemTime,
    duration: Duration,
    from: R::Peer,
    to: R::Peer,
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
struct Event;

#[derive(Debug, Clone)]
struct Cluster<R>
where
    R: Reactor + Clone,
    R::Peer: Debug + Clone,
{
    peers: HashMap<String, R>,
    partitions: Vec<Partition<R>>,
    in_flight: BinaryHeap<Event>,
}

unsafe impl<R> Send for Cluster<R>
where
    R: Reactor + Clone,
    R::Peer: Debug + Clone,
{
}

impl<R> Cluster<R>
where
    R: Reactor + Clone,
    R::Peer: Eq + Clone + Debug,
{
    fn _step(&mut self) -> Option<()> {
        let _pop = self.in_flight.pop();
        None
    }

    fn _is_partitioned(
        &mut self,
        at: SystemTime,
        to: R::Peer,
        from: R::Peer,
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
            if partition.to == to && partition.from == from {
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
