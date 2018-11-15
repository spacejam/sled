use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Condvar;

use rand::{rngs::StdRng, RngCore, SeedableRng};

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
where
    F: FnOnce(&mut ContextInner) -> B,
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
        context = time_updated
            .wait(context)
            .expect("context should not be poisoned");
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
    let mut seed_slice = [0u8; 32];
    let seed_bytes: [u8; std::mem::size_of::<usize>()] =
        unsafe { std::mem::transmute(seed) };

    seed_slice[0..seed_bytes.len()].copy_from_slice(&seed_bytes);

    with_context(move |c| {
        c.rng = SeedableRng::from_seed(seed_slice);
        c.seed = Some(seed)
    });
}

pub fn now() -> SystemTime {
    with_context(|c| c.clock)
}

pub struct Rand;

impl RngCore for Rand {
    fn next_u32(&mut self) -> u32 {
        with_context(|c| c.rng.next_u32())
    }

    fn next_u64(&mut self) -> u64 {
        with_context(|c| c.rng.next_u64())
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        with_context(|c| c.rng.fill_bytes(dest));
    }

    fn try_fill_bytes(
        &mut self,
        dest: &mut [u8],
    ) -> Result<(), rand::Error> {
        Ok(self.fill_bytes(dest))
    }
}

pub fn thread_rng() -> Rand {
    Rand
}

impl Default for ContextInner {
    fn default() -> ContextInner {
        let seed = [0u8; 32];
        ContextInner {
            seed: None,
            clock: UNIX_EPOCH,
            time_updated: Arc::new(Condvar::new()),
            rng: SeedableRng::from_seed(seed),
            filesystem: Filesystem::default(),
        }
    }
}
