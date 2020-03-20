use std::collections::HashSet;

use parking_lot::Mutex;

use crate::Lazy;

type HS =
    HashSet<&'static str, std::hash::BuildHasherDefault<fxhash::FxHasher64>>;

static ACTIVE: Lazy<Mutex<HS>, fn() -> Mutex<HS>> = Lazy::new(init);

fn init() -> Mutex<HS> {
    Mutex::new(HS::default())
}

/// Returns `true` if the given failpoint is active.
pub fn is_active(name: &'static str) -> bool {
    ACTIVE.lock().contains(&name)
}

/// Enable a particular failpoint
pub fn set(name: &'static str) {
    ACTIVE.lock().insert(name);
}

/// Clear all active failpoints.
pub fn reset() {
    ACTIVE.lock().clear();
}
