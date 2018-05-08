use std::net::SocketAddr;

use rand::{Rng, SeedableRng, StdRng};
use bincode::serialize;

use super::*;

#[derive(Debug, Default)]
pub struct Context(Mutex<ContextInner>);

#[derive(Debug)]
struct ContextInner {
    seed: Option<usize>,
    rng: StdRng,
    clock: SystemTime,
    filesystem: file::Filesystem,
    scheduler: Option<SyncSender<Call>>,
}

fn with_context<B, F>(f: F) -> B
    where F: FnOnce(&mut ContextInner) -> B
{
    let context_mu = context();
    let mut context = context_mu.0.lock().unwrap();
    f(&mut context)
}

pub fn set_time(now: SystemTime) {
    with_context(|c| c.clock = now);
}

pub fn sleep(duration: Duration) {
    with_context(|c| match c.scheduler {
        Some(ref sender) => {
            sender.send(Call::Sleep(duration)).expect(
                "scheduler should not be poisoned",
            );
        }
        _ => {
            println!("not sleeping, no scheduler registered");
        }
    })
}

pub fn send<M: Serialize>(to: SocketAddr, msg: M) {
    with_context(|c| match c.scheduler {
        Some(ref sender) => {
            sender.send(Call::SendMsg(to, serialize(&msg).unwrap()));
        }
        _ => println!("dropped message to {}", to),
    })
}

pub fn seed() -> usize {
    if let Some(seed) = with_context(|c| c.seed) {
        seed
    } else {
        let seed = match std::env::var("DETERMINISTIC_SEED") {
            Ok(val) => val.parse::<usize>().unwrap_or(0),
            Err(_) => 0,
        };
        set_seed(seed);
        seed
    }
}

pub fn set_seed(seed: usize) {
    with_context(|c| {
        let seed_slice: &[usize] = &[seed];
        c.rng.reseed(seed_slice);
        c.seed = Some(seed)
    });
}

pub fn now() -> SystemTime {
    with_context(|c| c.clock)
}

pub fn register_scheduler(sender: SyncSender<Call>) {
    with_context(move |c| c.scheduler = Some(sender));
}

pub struct Rand;

impl Rng for Rand {
    fn next_u32(&mut self) -> u32 {
        with_context(|c| c.rng.next_u32())
    }
}

pub fn thread_rng() -> Rand {
    Rand
}

impl Default for ContextInner {
    fn default() -> ContextInner {
        let seed: &[_] = &[0];
        ContextInner {
            seed: None,
            clock: UNIX_EPOCH,
            rng: SeedableRng::from_seed(seed),
            filesystem: file::Filesystem::default(),
            scheduler: None,
        }
    }
}
