use std::collections::BTreeMap;
use std::panic::Location;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};

static COUNTER: AtomicU64 = AtomicU64::new(0);
static CHECK_INS: LazyLock<BlockChecker> = LazyLock::new(|| {
    std::thread::spawn(move || {
        let mut last_top_10 = Default::default();
        loop {
            std::thread::sleep(std::time::Duration::from_secs(5));
            last_top_10 = CHECK_INS.report(last_top_10);
        }
    });

    BlockChecker::default()
});

type LocationMap = BTreeMap<u64, &'static Location<'static>>;

#[derive(Default)]
pub(crate) struct BlockChecker {
    state: Mutex<LocationMap>,
}

impl BlockChecker {
    fn report(&self, last_top_10: LocationMap) -> LocationMap {
        let state = self.state.lock().unwrap();
        println!("top 10 longest blocking sections:");

        let top_10: LocationMap =
            state.iter().take(10).map(|(k, v)| (*k, *v)).collect();

        for (id, location) in &top_10 {
            if last_top_10.contains_key(id) {
                println!("id: {}, location: {:?}", id, location);
            }
        }

        top_10
    }

    fn check_in(&self, location: &'static Location) -> BlockGuard {
        let next_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.lock().unwrap();
        state.insert(next_id, location);
        BlockGuard { id: next_id }
    }

    fn check_out(&self, id: u64) {
        let mut state = self.state.lock().unwrap();
        state.remove(&id);
    }
}

pub(crate) struct BlockGuard {
    id: u64,
}

impl Drop for BlockGuard {
    fn drop(&mut self) {
        CHECK_INS.check_out(self.id)
    }
}

#[track_caller]
pub(crate) fn track_blocks() -> BlockGuard {
    let caller = Location::caller();
    CHECK_INS.check_in(caller)
}
