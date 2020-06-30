use std::collections::HashMap;

use parking_lot::Mutex;

use crate::Lazy;

type HM = HashMap<
    &'static str,
    u64,
    std::hash::BuildHasherDefault<fxhash::FxHasher64>,
>;

static ACTIVE: Lazy<Mutex<HM>, fn() -> Mutex<HM>> = Lazy::new(init);

fn init() -> Mutex<HM> {
    Mutex::new(HM::default())
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
