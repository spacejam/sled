#![allow(unused_results)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::float_arithmetic)]

use std::sync::atomic::AtomicUsize;

#[cfg(not(target_arch = "x86_64"))]
use std::time::{Duration, Instant};

#[cfg(not(feature = "metrics"))]
use std::marker::PhantomData;

#[cfg(feature = "metrics")]
use std::sync::atomic::Ordering::{Acquire, Relaxed};

use num_format::{Locale, ToFormattedString};

use crate::Lazy;

use super::*;

/// A metric collector for all pagecache users running in this
/// process.
pub static M: Lazy<Metrics, fn() -> Metrics> = Lazy::new(Metrics::default);

#[allow(clippy::cast_precision_loss)]
pub(crate) fn clock() -> u64 {
    if cfg!(not(feature = "metrics")) {
        0
    } else {
        #[cfg(target_arch = "x86_64")]
        #[allow(unsafe_code)]
        unsafe {
            let mut aux = 0;
            core::arch::x86_64::__rdtscp(&mut aux)
        }

        #[cfg(not(target_arch = "x86_64"))]
        {
            let u = uptime();
            (u.as_secs() * 1_000_000_000) + u64::from(u.subsec_nanos())
        }
    }
}

// not correct, since it starts counting at the first observance...
#[cfg(not(target_arch = "x86_64"))]
pub(crate) fn uptime() -> Duration {
    static START: Lazy<Instant, fn() -> Instant> = Lazy::new(Instant::now);

    if cfg!(not(feature = "metrics")) {
        Duration::new(0, 0)
    } else {
        START.elapsed()
    }
}

/// Measure the duration of an event, and call `Histogram::measure()`.
pub struct Measure<'h> {
    start: u64,
    histo: &'h Histogram,
    #[cfg(not(feature = "metrics"))]
    _pd: PhantomData<&'h ()>,
}

impl<'h> Measure<'h> {
    /// The time delta from ctor to dtor is recorded in `histo`.
    #[inline]
    #[allow(unused_variables)]
    pub fn new(histo: &'h Histogram) -> Measure<'h> {
        Measure {
            #[cfg(not(feature = "metrics"))]
            _pd: PhantomData,
            histo,
            start: clock(),
        }
    }
}

impl<'h> Drop for Measure<'h> {
    #[inline]
    fn drop(&mut self) {
        #[cfg(feature = "metrics")]
        self.histo.measure(clock() - self.start);
    }
}

#[derive(Default, Debug)]
pub struct Metrics {
    pub accountant_bump_tip: Histogram,
    pub accountant_hold: Histogram,
    pub accountant_lock: Histogram,
    pub accountant_mark_link: Histogram,
    pub accountant_mark_replace: Histogram,
    pub accountant_next: Histogram,
    pub accountant_stabilize: Histogram,
    pub advance_snapshot: Histogram,
    pub assign_offset: Histogram,
    pub bytes_written_heap_item: CachePadded<AtomicUsize>,
    pub bytes_written_heap_ptr: CachePadded<AtomicUsize>,
    pub bytes_written_replace: CachePadded<AtomicUsize>,
    pub bytes_written_link: CachePadded<AtomicUsize>,
    pub bytes_written_other: CachePadded<AtomicUsize>,
    pub fuzzy_snapshot: Histogram,
    pub compress: Histogram,
    pub decompress: Histogram,
    pub deserialize: Histogram,
    pub get_page: Histogram,
    pub get_pagetable: Histogram,
    pub link_page: Histogram,
    pub log_reservation_attempts: CachePadded<AtomicUsize>,
    pub log_reservations: CachePadded<AtomicUsize>,
    pub make_stable: Histogram,
    pub page_out: Histogram,
    pub pull: Histogram,
    pub read: Histogram,
    pub read_segment_message: Histogram,
    pub replace_page: Histogram,
    pub reserve_lat: Histogram,
    pub reserve_sz: Histogram,
    pub rewrite_page: Histogram,
    pub segment_read: Histogram,
    pub segment_utilization_startup: Histogram,
    pub segment_utilization_shutdown: Histogram,
    pub serialize: Histogram,
    pub snapshot_apply: Histogram,
    pub start_pagecache: Histogram,
    pub start_segment_accountant: Histogram,
    pub tree_cas: Histogram,
    pub tree_child_split_attempt: CachePadded<AtomicUsize>,
    pub tree_child_split_success: CachePadded<AtomicUsize>,
    pub tree_del: Histogram,
    pub tree_get: Histogram,
    pub tree_loops: CachePadded<AtomicUsize>,
    pub tree_merge: Histogram,
    pub tree_parent_split_attempt: CachePadded<AtomicUsize>,
    pub tree_parent_split_success: CachePadded<AtomicUsize>,
    pub tree_reverse_scan: Histogram,
    pub tree_root_split_attempt: CachePadded<AtomicUsize>,
    pub tree_root_split_success: CachePadded<AtomicUsize>,
    pub tree_scan: Histogram,
    pub tree_set: Histogram,
    pub tree_start: Histogram,
    pub tree_traverse: Histogram,
    pub write_to_log: Histogram,
    pub written_bytes: Histogram,
}

impl Metrics {
    #[inline]
    pub fn tree_looped(&self) {
        self.tree_loops.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn log_reservation_attempted(&self) {
        self.log_reservation_attempts.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn log_reservation_success(&self) {
        self.log_reservations.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn tree_child_split_attempt(&self) {
        self.tree_child_split_attempt.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn tree_child_split_success(&self) {
        self.tree_child_split_success.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn tree_parent_split_attempt(&self) {
        self.tree_parent_split_attempt.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn tree_parent_split_success(&self) {
        self.tree_parent_split_success.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn tree_root_split_attempt(&self) {
        self.tree_root_split_attempt.fetch_add(1, Relaxed);
    }

    #[inline]
    pub fn tree_root_split_success(&self) {
        self.tree_root_split_success.fetch_add(1, Relaxed);
    }

    pub fn format_profile(&self) -> String {
        let mut ret = String::new();
        ret.push_str(&format!(
                "sled profile:\n\
             {0: >17} | {1: >10} | {2: >10} | {3: >10} | {4: >10} | {5: >10} | {6: >10} | {7: >10} | {8: >10} | {9: >10}\n",
             "op",
             "min (us)",
             "med (us)",
             "90 (us)",
             "99 (us)",
             "99.9 (us)",
             "99.99 (us)",
             "max (us)",
             "count",
             "sum (s)"
             ));
        ret.push_str(&format!("{}\n", "-".repeat(134)));

        let p =
            |mut tuples: Vec<(String, _, _, _, _, _, _, _, _, _)>| -> String {
                tuples.sort_by_key(|t| (t.9 * -1. * 1e3) as i64);
                let mut to_ret = String::new();
                for v in tuples {
                    to_ret.push_str(&format!(
                        "{0: >17} | {1: >10.1} | {2: >10.1} | {3: >10.1} \
                     | {4: >10.1} | {5: >10.1} | {6: >10.1} | {7: >10.1} \
                     | {8: >10.1} | {9: >10.1}\n",
                        v.0, v.1, v.2, v.3, v.4, v.5, v.6, v.7, v.8, v.9,
                    ));
                }
                to_ret
            };

        let lat = |name: &str, histo: &Histogram| {
            (
                name.to_string(),
                histo.percentile(0.) / 1e3,
                histo.percentile(50.) / 1e3,
                histo.percentile(90.) / 1e3,
                histo.percentile(99.) / 1e3,
                histo.percentile(99.9) / 1e3,
                histo.percentile(99.99) / 1e3,
                histo.percentile(100.) / 1e3,
                histo.count(),
                histo.sum() as f64 / 1e9,
            )
        };

        let sz = |name: &str, histo: &Histogram| {
            (
                name.to_string(),
                histo.percentile(0.),
                histo.percentile(50.),
                histo.percentile(90.),
                histo.percentile(99.),
                histo.percentile(99.9),
                histo.percentile(99.99),
                histo.percentile(100.),
                histo.count(),
                histo.sum() as f64,
            )
        };

        ret.push_str("tree:\n");

        ret.push_str(&p(vec![
            lat("traverse", &self.tree_traverse),
            lat("get", &self.tree_get),
            lat("set", &self.tree_set),
            lat("merge", &self.tree_merge),
            lat("del", &self.tree_del),
            lat("cas", &self.tree_cas),
            lat("scan", &self.tree_scan),
            lat("rev scan", &self.tree_reverse_scan),
        ]));
        let total_loops = self.tree_loops.load(Acquire);
        let total_ops = self.tree_get.count()
            + self.tree_set.count()
            + self.tree_merge.count()
            + self.tree_del.count()
            + self.tree_cas.count()
            + self.tree_scan.count()
            + self.tree_reverse_scan.count();
        let loop_pct = total_loops * 100 / (total_ops + 1);
        ret.push_str(&format!(
            "tree contention loops: {} ({}% retry rate)\n",
            total_loops, loop_pct
        ));
        ret.push_str(&format!(
                "tree split success rates: child({}/{}) parent({}/{}) root({}/{})\n",
                self.tree_child_split_success.load(Acquire)
                .to_formatted_string(&Locale::en)
                ,
                self.tree_child_split_attempt.load(Acquire)
                .to_formatted_string(&Locale::en)
                ,
                self.tree_parent_split_success.load(Acquire)
                .to_formatted_string(&Locale::en)
                ,
                self.tree_parent_split_attempt.load(Acquire)
                .to_formatted_string(&Locale::en)
                ,
                self.tree_root_split_success.load(Acquire)
                .to_formatted_string(&Locale::en)
                ,
                self.tree_root_split_attempt.load(Acquire)
                .to_formatted_string(&Locale::en)
                ,
                ));

        ret.push_str(&format!("{}\n", "-".repeat(134)));
        ret.push_str("pagecache:\n");
        ret.push_str(&p(vec![
            lat("get", &self.get_page),
            lat("get pt", &self.get_pagetable),
            lat("rewrite", &self.rewrite_page),
            lat("replace", &self.replace_page),
            lat("link", &self.link_page),
            lat("pull", &self.pull),
            lat("page_out", &self.page_out),
        ]));
        let hit_ratio = self.get_page.count().saturating_sub(self.pull.count())
            * 100
            / (self.get_page.count() + 1);
        ret.push_str(&format!("hit ratio: {}%\n", hit_ratio));

        ret.push_str(&format!("{}\n", "-".repeat(134)));
        ret.push_str("serialization:\n");
        ret.push_str(&p(vec![
            lat("serialize", &self.serialize),
            lat("deserialize", &self.deserialize),
        ]));

        ret.push_str(&format!("{}\n", "-".repeat(134)));
        ret.push_str("log:\n");
        ret.push_str(&p(vec![
            lat("make_stable", &self.make_stable),
            lat("read", &self.read),
            lat("write", &self.write_to_log),
            sz("written bytes", &self.written_bytes),
            lat("assign offset", &self.assign_offset),
            lat("reserve lat", &self.reserve_lat),
            sz("reserve sz", &self.reserve_sz),
        ]));
        let log_reservations =
            std::cmp::max(1, self.log_reservations.load(Acquire));
        let log_reservation_attempts =
            std::cmp::max(1, self.log_reservation_attempts.load(Acquire));
        let log_reservation_retry_rate =
            log_reservation_attempts.saturating_sub(log_reservations) * 100
                / (log_reservations + 1);
        ret.push_str(&format!(
            "log reservations:             {:>15}\n",
            log_reservations.to_formatted_string(&Locale::en)
        ));
        ret.push_str(&format!(
            "log res attempts:             {:>15} ({}% retry rate)\n",
            log_reservation_attempts.to_formatted_string(&Locale::en),
            log_reservation_retry_rate,
        ));

        ret.push_str(&format!(
            "heap item reserved bytes:     {:>15}\n",
            self.bytes_written_heap_item
                .load(Acquire)
                .to_formatted_string(&Locale::en)
        ));
        ret.push_str(&format!(
            "heap pointer reserved bytes:  {:>15}\n",
            self.bytes_written_heap_ptr
                .load(Acquire)
                .to_formatted_string(&Locale::en)
        ));
        ret.push_str(&format!(
            "node replace reserved bytes:  {:>15}\n",
            self.bytes_written_replace
                .load(Acquire)
                .to_formatted_string(&Locale::en)
        ));
        ret.push_str(&format!(
            "node link reserved bytes:     {:>15}\n",
            self.bytes_written_link
                .load(Acquire)
                .to_formatted_string(&Locale::en)
        ));
        ret.push_str(&format!(
            "other written reserved bytes: {:>15}\n",
            self.bytes_written_other
                .load(Acquire)
                .to_formatted_string(&Locale::en)
        ));

        ret.push_str(&format!("{}\n", "-".repeat(134)));
        ret.push_str("segment accountant:\n");
        ret.push_str(&p(vec![
            lat("acquire", &self.accountant_lock),
            lat("hold", &self.accountant_hold),
            lat("next", &self.accountant_next),
            lat("stabilize", &self.accountant_stabilize),
            lat("replace", &self.accountant_mark_replace),
            lat("link", &self.accountant_mark_link),
        ]));

        ret.push_str(&format!("{}\n", "-".repeat(134)));
        ret.push_str("recovery:\n");
        ret.push_str(&p(vec![
            lat("start", &self.tree_start),
            lat("advance snapshot", &self.advance_snapshot),
            lat("fuzzy snapshot", &self.fuzzy_snapshot),
            lat("load SA", &self.start_segment_accountant),
            lat("load PC", &self.start_pagecache),
            lat("snap apply", &self.snapshot_apply),
            lat("segment read", &self.segment_read),
            lat("log message read", &self.read_segment_message),
            sz("seg util start", &self.segment_utilization_startup),
            sz("seg util end", &self.segment_utilization_shutdown),
        ]));

        ret
    }
}
