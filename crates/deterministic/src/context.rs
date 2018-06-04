use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Condvar;

use rand::{Rng, SeedableRng, StdRng};

use super::*;

#[derive(Debug, Default)]
pub struct Context(Mutex<ContextInner>);

#[derive(Debug)]
struct ContextInner {
    seed: Option<usize>,
    rng: StdRng,
    clock: SystemTime,
    time_updated: Arc<Condvar>,
    filesystem: Filesystem,
}

#[derive(Default, Debug)]
pub struct Filesystem {
    files: HashMap<PathBuf, file::File>,
}

fn with_context<B, F>(f: F) -> B
    where F: FnOnce(&mut ContextInner) -> B
{
    let context_mu = context();
    let mut context = context_mu.0.lock().unwrap();
    f(&mut context)
}

/// set the time for this thread and its `spawn`ed descendents
pub fn set_time(now: SystemTime) {
    with_context(|c| {
        c.clock = now;
        c.time_updated.notify_all();
    });
}

pub fn sleep(dur: Duration) {
    let context_mu = context();
    let mut context =
        context_mu.0.lock().expect("context should not be poisoned");
    let time_updated = context.time_updated.clone();
    let wakeup = context.clock + dur;
    while context.clock < wakeup {
        context = time_updated.wait(context).expect(
            "context should not be poisoned",
        );
    }
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
            time_updated: Arc::new(Condvar::new()),
            rng: SeedableRng::from_seed(seed),
            filesystem: Filesystem::default(),
        }
    }
}
