use parking_lot::Mutex;

use crate::{Lazy, Map};

type Hm = Map<&'static str, u64>;

static ACTIVE: Lazy<Mutex<Hm>, fn() -> Mutex<Hm>> = Lazy::new(init);

fn init() -> Mutex<Hm> {
    Mutex::new(Hm::default())
}

/// Returns `true` if the given failpoint is active.
pub fn is_active(name: &'static str) -> bool {
    let mut active = ACTIVE.lock();
    if let Some(bitset) = active.get_mut(&name) {
        let bit = *bitset & 1;
        *bitset >>= 1;
        if *bitset == 0 {
            active.remove(&name);
        }
        bit != 0
    } else {
        false
    }
}

/// Enable a particular failpoint
pub fn set(name: &'static str, bitset: u64) {
    ACTIVE.lock().insert(name, bitset);
}

/// Clear all active failpoints.
pub fn reset() {
    ACTIVE.lock().clear();
}
