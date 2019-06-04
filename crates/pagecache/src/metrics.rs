use std::{
    sync::atomic::AtomicUsize,
    time::{Duration, Instant},
};

#[cfg(feature = "no_metrics")]
use std::marker::PhantomData;

#[cfg(not(feature = "no_metrics"))]
use std::sync::atomic::Ordering::{Acquire, Relaxed};

use super::*;

use historian::Histo;

lazy_static! {
    /// A metric collector for all pagecache users running in this
    /// process.
    pub static ref M: Metrics = Metrics::default();
}

pub(crate) fn clock() -> f64 {
    if cfg!(feature = "no_metrics") {
        0.
    } else {
        let u = uptime();
        (u.as_secs() * 1_000_000_000) as f64 + f64::from(u.subsec_nanos())
    }
}

// not correct, since it starts counting at the first observance...
pub(crate) fn uptime() -> Duration {
    lazy_static! {
        static ref START: Instant = Instant::now();
    }

    if cfg!(feature = "no_metrics") {
        Duration::new(0, 0)
    } else {
        START.elapsed()
    }
}

/// Measure the duration of an event, and call `Histo::measure()`.
pub struct Measure<'h> {
    start: f64,
    #[cfg(not(feature = "no_metrics"))]
    histo: &'h Histo,
    #[cfg(feature = "no_metrics")]
    _pd: PhantomData<&'h ()>,
}

impl<'h> Measure<'h> {
    /// The time delta from ctor to dtor is recorded in `histo`.
    #[inline(always)]
    pub fn new(histo: &'h Histo) -> Measure<'h> {
        Measure {
            #[cfg(feature = "no_metrics")]
            _pd: PhantomData,
            #[cfg(not(feature = "no_metrics"))]
            histo,
            start: clock(),
        }
    }
}

impl<'h> Drop for Measure<'h> {
    #[inline(always)]
    fn drop(&mut self) {
        #[cfg(not(feature = "no_metrics"))]
        self.histo.measure(clock() - self.start);
    }
}

/// Measure the time spent on calling a given function in a given `Histo`.
#[cfg_attr(not(feature = "no_inline"), inline)]
pub(crate) fn measure<F: FnOnce() -> R, R>(histo: &Histo, f: F) -> R {
    #[cfg(not(feature = "no_metrics"))]
    let _measure = Measure::new(histo);
    f()
}

#[derive(Default, Debug)]
pub struct Metrics {
    pub advance_snapshot: Histo,
    pub tree_set: Histo,
    pub tree_get: Histo,
    pub tree_del: Histo,
    pub tree_cas: Histo,
    pub tree_scan: Histo,
    pub tree_reverse_scan: Histo,
    pub tree_merge: Histo,
    pub tree_start: Histo,
    pub tree_traverse: Histo,
    pub tree_child_split_attempt: CachePadded<AtomicUsize>,
    pub tree_child_split_success: CachePadded<AtomicUsize>,
    pub tree_parent_split_attempt: CachePadded<AtomicUsize>,
    pub tree_parent_split_success: CachePadded<AtomicUsize>,
    pub tree_root_split_attempt: CachePadded<AtomicUsize>,
    pub tree_root_split_success: CachePadded<AtomicUsize>,
    pub get_page: Histo,
    pub rewrite_page: Histo,
    pub replace_page: Histo,
    pub link_page: Histo,
    pub merge_page: Histo,
    pub page_out: Histo,
    pub pull: Histo,
    pub serialize: Histo,
    pub deserialize: Histo,
    pub compress: Histo,
    pub decompress: Histo,
    pub make_stable: Histo,
    pub assign_offset: Histo,
    pub assign_spinloop: Histo,
    pub reserve_lat: Histo,
    pub reserve_sz: Histo,
    pub reserve_current_condvar_wait: Histo,
    pub reserve_written_condvar_wait: Histo,
    pub write_to_log: Histo,
    pub written_bytes: Histo,
    pub read: Histo,
    pub tree_loops: CachePadded<AtomicUsize>,
    pub log_reservations: CachePadded<AtomicUsize>,
    pub log_reservation_attempts: CachePadded<AtomicUsize>,
    pub accountant_lock: Histo,
    pub accountant_hold: Histo,
    pub accountant_next: Histo,
    pub accountant_mark_link: Histo,
    pub accountant_mark_replace: Histo,
    pub accountant_bump_tip: Histo,
    #[cfg(feature = "measure_allocs")]
    pub allocations: CachePadded<AtomicUsize>,
    #[cfg(feature = "measure_allocs")]
    pub allocated_bytes: CachePadded<AtomicUsize>,
}

#[cfg(not(feature = "no_metrics"))]
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

    pub fn print_profile(&self) {
        println!(
            "pagecache profile:\n\
            {0: >17} | {1: >10} | {2: >10} | {3: >10} | {4: >10} | {5: >10} | {6: >10} | {7: >10} | {8: >10} | {9: >10}",
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
        );
        println!("{}", std::iter::repeat("-").take(134).collect::<String>());

        let p = |mut tuples: Vec<(String, _, _, _, _, _, _, _, _, _)>| {
            tuples.sort_by_key(|t| (t.9 * -1. * 1e3) as i64);
            for v in tuples {
                println!(
                    "{0: >17} | {1: >10.1} | {2: >10.1} | {3: >10.1} \
                     | {4: >10.1} | {5: >10.1} | {6: >10.1} | {7: >10.1} \
                     | {8: >10.1} | {9: >10.3}",
                    v.0, v.1, v.2, v.3, v.4, v.5, v.6, v.7, v.8, v.9,
                );
            }
        };

        let lat = |name: &str, histo: &Histo| {
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

        let sz = |name: &str, histo: &Histo| {
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

        println!("tree:");
        p(vec![
            lat("start", &self.tree_start),
            lat("traverse", &self.tree_traverse),
            lat("get", &self.tree_get),
            lat("set", &self.tree_set),
            lat("merge", &self.tree_merge),
            lat("del", &self.tree_del),
            lat("cas", &self.tree_cas),
            lat("scan", &self.tree_scan),
            lat("rev scan", &self.tree_reverse_scan),
        ]);
        println!("tree contention loops: {}", self.tree_loops.load(Acquire));
        println!(
            "tree split success rates: child({}/{}) parent({}/{}) root({}/{})",
            self.tree_child_split_success.load(Acquire),
            self.tree_child_split_attempt.load(Acquire),
            self.tree_parent_split_success.load(Acquire),
            self.tree_parent_split_attempt.load(Acquire),
            self.tree_root_split_success.load(Acquire),
            self.tree_root_split_attempt.load(Acquire),
        );

        println!("{}", std::iter::repeat("-").take(134).collect::<String>());
        println!("pagecache:");
        p(vec![
            lat("snapshot", &self.advance_snapshot),
            lat("get", &self.get_page),
            lat("rewrite", &self.rewrite_page),
            lat("replace", &self.replace_page),
            lat("link", &self.link_page),
            lat("merge", &self.merge_page),
            lat("pull", &self.pull),
            lat("page_out", &self.page_out),
        ]);

        println!("{}", std::iter::repeat("-").take(134).collect::<String>());
        println!("serialization and compression:");
        p(vec![
            lat("serialize", &self.serialize),
            lat("deserialize", &self.deserialize),
            lat("compress", &self.compress),
            lat("decompress", &self.decompress),
        ]);

        println!("{}", std::iter::repeat("-").take(134).collect::<String>());
        println!("log:");
        p(vec![
            lat("make_stable", &self.make_stable),
            lat("read", &self.read),
            lat("write", &self.write_to_log),
            sz("written bytes", &self.written_bytes),
            lat("assign offset", &self.assign_offset),
            lat("assign spinloop", &self.assign_spinloop),
            lat("reserve lat", &self.reserve_lat),
            sz("reserve sz", &self.reserve_sz),
            lat("res cvar r", &self.reserve_current_condvar_wait),
            lat("res cvar w", &self.reserve_written_condvar_wait),
        ]);
        println!("log reservations: {}", self.log_reservations.load(Acquire));
        println!(
            "log res attempts: {}",
            self.log_reservation_attempts.load(Acquire)
        );

        println!("{}", std::iter::repeat("-").take(134).collect::<String>());
        println!("segment accountant:");
        p(vec![
            lat("acquire", &self.accountant_lock),
            lat("hold", &self.accountant_hold),
            lat("next", &self.accountant_next),
            lat("replace", &self.accountant_mark_replace),
            lat("link", &self.accountant_mark_link),
        ]);

        #[cfg(feature = "measure_allocs")]
        {
            println!(
                "{}",
                std::iter::repeat("-").take(134).collect::<String>()
            );
            println!("allocation statistics:");
            println!(
                "total allocations: {}",
                measure_allocs::ALLOCATIONS.load(Acquire)
            );
            println!(
                "allocated bytes: {}",
                measure_allocs::ALLOCATED_BYTES.load(Acquire)
            );
        }
    }
}

#[cfg(feature = "no_metrics")]
impl Metrics {
    pub fn log_reservation_attempted(&self) {}

    pub fn log_reservation_success(&self) {}

    pub fn tree_child_split_attempt(&self) {}

    pub fn tree_child_split_success(&self) {}

    pub fn tree_parent_split_attempt(&self) {}

    pub fn tree_parent_split_success(&self) {}

    pub fn tree_root_split_attempt(&self) {}

    pub fn tree_root_split_success(&self) {}

    pub fn tree_looped(&self) {}

    pub fn log_looped(&self) {}

    pub fn print_profile(&self) {}
}
