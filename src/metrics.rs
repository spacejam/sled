use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

use historian::Histo;

#[derive(Default, Debug)]
pub struct Metrics {
    pub write_snapshot: Histo,
    pub tree_set: Histo,
    pub tree_get: Histo,
    pub tree_del: Histo,
    pub tree_cas: Histo,
    pub tree_scan: Histo,
    pub page_in: Histo,
    pub page_out: Histo,
    pub pull: Histo,
    pub serialize: Histo,
    pub deserialize: Histo,
    pub compress: Histo,
    pub decompress: Histo,
    pub make_stable: Histo,
    pub reserve: Histo,
    pub write_to_log: Histo,
    pub read: Histo,
    pub tree_loops: AtomicUsize,
    pub log_loops: AtomicUsize,
}

impl Metrics {
    pub fn tree_looped(&self) {
        self.tree_loops.fetch_add(1, Relaxed);
    }

    pub fn log_looped(&self) {
        self.log_loops.fetch_add(1, Relaxed);
    }
}
