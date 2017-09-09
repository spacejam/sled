use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::iter::repeat;

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
    pub merge_page: Histo,
    pub page_out: Histo,
    pub pull: Histo,
    pub serialize: Histo,
    pub deserialize: Histo,
    pub compress: Histo,
    pub decompress: Histo,
    pub make_stable: Histo,
    pub reserve: Histo,
    pub write_to_log: Histo,
    pub written_bytes: Histo,
    pub written_padding: Histo,
    pub punch_hole: Histo,
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

    pub fn print_profile(&self) {
        println!(
            "sled profile:\n\
            {0: >17} | {1: >10} | {2: >10} | {3: >10} | {4: >10} | {5: >10} | {6: >10} | {7: >10}",
            "op",
            "min (us)",
            "90 (us)",
            "99 (us)",
            "99.9 (us)",
            "max (us)",
            "count",
            "sum (s)"
        );
        println!("{}", repeat("-").take(103).collect::<String>());

        let p = |mut tuples: Vec<(String, _, _, _, _, _, _, _)>| {
            tuples.sort_by_key(|t| (t.7 * -1. * 1e3) as i64);
            for v in tuples.into_iter() {
                println!(
                    "{0: >17} | {1: >10.1} | {2: >10.1} | {3: >10.1} \
                | {4: >10.1} | {5: >10.1} | {6: >10.1} | {7: >10.3}",
                    v.0,
                    v.1,
                    v.2,
                    v.3,
                    v.4,
                    v.5,
                    v.6,
                    v.7
                );
            }
        };

        let f = |name: &str, histo: &Histo| {
            (
                name.to_string(),
                histo.percentile(0.) / 1e3,
                histo.percentile(90.) / 1e3,
                histo.percentile(99.) / 1e3,
                histo.percentile(99.9) / 1e3,
                histo.percentile(100.) / 1e3,
                histo.count(),
                histo.sum() as f64 / 1e9,
            )
        };

        println!("tree:");
        p(vec![
            f("get", &self.tree_get),
            f("set", &self.tree_set),
            f("del", &self.tree_del),
            f("cas", &self.tree_cas),
            f("scan", &self.tree_scan),
        ]);
        println!("tree contention loops: {}", self.tree_loops.load(Acquire));

        println!("{}", repeat("-").take(103).collect::<String>());
        println!("pagecache:");
        p(vec![
            f("snapshot", &self.write_snapshot),
            f("page_in", &self.page_in),
            f("merge", &self.merge_page),
            f("pull", &self.pull),
            f("page_out", &self.page_out),
        ]);

        println!("{}", repeat("-").take(103).collect::<String>());
        println!("serialization and compression:");
        p(vec![
            f("serialize", &self.serialize),
            f("deserialize", &self.deserialize),
            f("compress", &self.compress),
            f("decompress", &self.decompress),
        ]);

        println!("{}", repeat("-").take(103).collect::<String>());
        println!("log:");
        p(vec![
            f("make_stable", &self.make_stable),
            f("read", &self.read),
            f("write", &self.write_to_log),
            f("written bytes", &self.written_bytes),
            f("written padding", &self.written_padding),
            f("reserve", &self.reserve),
            f("punch_hole", &self.punch_hole),
        ]);
        println!("log contention loops: {}", self.log_loops.load(Acquire));
    }
}
