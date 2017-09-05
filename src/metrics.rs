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
            "rsdb profile:\n\
            {0: >12} | {1: >10} | {2: >10} | {3: >10} | {4: >10} | {5: >10} | {6: >10} | {7: >10}",
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

        let p = |v: (String, _, _, _, _, _, _, _)| {
            println!(
                "{0: >12} | {1: >10.1} | {2: >10.1} | {3: >10.1} \
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

        let mut tuples = vec![
            f("get", &self.tree_get),
            f("set", &self.tree_set),
            f("del", &self.tree_del),
            f("cas", &self.tree_cas),
            f("scan", &self.tree_scan),
            f("snapshot", &self.write_snapshot),
            f("make_stable", &self.make_stable),
            f("page_in", &self.page_in),
            f("merge", &self.merge_page),
            f("pull", &self.pull),
            f("read", &self.read),
            f("page_out", &self.page_out),
            f("serialize", &self.serialize),
            f("deserialize", &self.deserialize),
            f("compress", &self.compress),
            f("decompress", &self.decompress),
            f("reserve log", &self.reserve),
            f("write", &self.write_to_log),
        ];

        tuples.sort_by_key(|t| (t.7 * -1.) as i64);

        for tuple in tuples.into_iter() {
            p(tuple);
        }

        println!("tree contention loops: {}", self.tree_loops.load(Acquire));
        println!("log contention loops: {}", self.log_loops.load(Acquire));
    }
}
