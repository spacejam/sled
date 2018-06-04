/// Simulation for network partitions. Like Jepsen but thousands of times faster.
extern crate quickcheck;
extern crate rand;
extern crate deterministic;

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::time::{Duration, SystemTime};

struct Event<S>
    where S: Simulable
{
    at: SystemTime,
    action: Action<S>,
}

// we implement Ord and PartialOrd to make the BinaryHeap
// act like a min-heap on time, rather than the default
// max-heap, so time progresses forwards.
impl<A> Ord for Event<A>
    where Event<A>: Eq,
          A: Simulable
{
    fn cmp(&self, other: &Event<A>) -> Ordering {
        other.at.cmp(&self.at)
    }
}

impl<A> PartialOrd for Event<A>
    where Event<A>: PartialEq + Ord,
          A: Simulable
{
    fn partial_cmp(&self, other: &Event<A>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

enum Action<S>
    where S: Simulable
{
    Invocation(S::Invocation),
    Response(S::Response),
    Msg {
        from: S::Addr,
        to: S::Addr,
        msg: S::Msg,
    },
    Sleep(Duration),
    Restart,
    Partition,
}

struct Simulator<S>
    where S: Simulable
{
    actors: HashMap<S::Addr, S>,
    queue: BinaryHeap<Event<S>>,
}

trait Simulable {
    type Invocation;
    type Response;
    type Msg;
    type Addr;

    fn restart(self) -> Self;
    fn receive();
}

#[test]
fn model_based() {
    let ops = vec![];
    let model = ();
    let implementation = ();
    for op in ops {
        assert_eq!(op(model), op(implementation));
    }
}

#[test]
fn concurrent_model() {
    let ops = vec![];
    let threads = vec![];
    let subhistories = threads.map(ops);
    // try to find a sequential history of ops that
    // matches subhistories for all threads
}

#[test]
fn distributed_model() {
    let ops = vec![];
    let cluster = ();
    let model = ();
    for op in ops {
        assert_eq!(op(model), op(cluster));
    }
}

#[test]
fn distributed_concurrent() {
    let ops = vec![];
    let cluster = ();
    let clients = vec![];
    let subhistories = clients.map((ops, cluster));
    // try to find a sequential history of ops that
    // matches subhistories for all clients
}
