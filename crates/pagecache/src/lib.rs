//! `pagecache` is a lock-free pagecache and log for building high-performance databases.
#![deny(missing_docs)]
#![cfg_attr(test, deny(warnings))]
#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]
#![cfg_attr(feature = "clippy", allow(inline_always))]
#![cfg_attr(feature = "nightly", feature(integer_atomics))]
#[cfg(all(not(feature = "nightly"), target_pointer_width = "32"))]
compile_error!(
    "32 bit architectures require a nightly compiler for now.
               See https://github.com/spacejam/sled/issues/145"
);

#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate crossbeam_epoch as epoch;
extern crate historian;
extern crate serde;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log as _log;
#[cfg(unix)]
extern crate libc;
#[cfg(
    any(test, feature = "failpoints", feature = "lock_free_delays")
)]
extern crate rand;
#[cfg(feature = "rayon")]
extern crate rayon;
#[cfg(feature = "zstd")]
extern crate zstd;
#[cfg(feature = "failpoints")]
#[macro_use]
extern crate fail;
extern crate pagetable;

pub use ds::{Radix, Stack};

/// general-purpose configuration
pub use config::{Config, ConfigBuilder};
pub use diskptr::DiskPtr;
pub use io::*;
pub use result::{CacheResult, Error};

macro_rules! maybe_fail {
    ($e:expr) => {
        #[cfg(feature = "failpoints")]
        fail_point!($e, |_| Err(Error::FailPoint));
    };
}

macro_rules! rep_no_copy {
    ($e:expr; $n:expr) => {{
        let mut v = Vec::with_capacity($n);
        for _ in 0..$n {
            v.push($e);
        }
        v
    }};
}

mod config;
mod diskptr;
mod ds;
mod hash;
mod io;
mod metrics;
mod periodic;
mod result;

use ds::*;
use hash::{crc16_arr, crc64};
use historian::Histo;
use metrics::Metrics;

/// An offset for a storage file segment.
pub type SegmentID = usize;

/// A log file offset.
pub type LogID = u64;

/// A pointer to an external blob.
pub type ExternalPointer = Lsn;

/// A logical sequence number.
pub type Lsn = i64;

/// A page identifier.
pub type PageID = usize;

/// A pointer to shared lock-free state bound by a pinned epoch's lifetime.
pub type PagePtr<'g, P> =
    epoch::Shared<'g, ds::stack::Node<io::CacheEntry<P>>>;

// This type exists to communicate that the underlying raw pointer in epoch::Shared
// is Send in the restricted context of pulling data from disk in a parallel
// way by rayon.
#[derive(Debug, Clone, PartialEq)]
struct RayonPagePtr<'g, P>(
    epoch::Shared<'g, ds::stack::Node<io::CacheEntry<P>>>,
)
where
    P: Send + 'static;

use std::ops::Deref;

impl<'g, P> Deref for RayonPagePtr<'g, P>
where
    P: Send,
{
    type Target = PagePtr<'g, P>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe impl<'g, P> Send for RayonPagePtr<'g, P> where P: Send {}

lazy_static! {
    /// A metric collector for all pagecache users running in this
    /// process.
    pub static ref M: Metrics = Metrics::default();
}

fn clock() -> f64 {
    let u = uptime();
    (u.as_secs() * 1_000_000_000) as f64 + f64::from(u.subsec_nanos())
}

// not correct, since it starts counting at the first observance...
fn uptime() -> std::time::Duration {
    lazy_static! {
        static ref START: std::time::Instant =
            std::time::Instant::now();
    }

    START.elapsed()
}

/// Measure the duration of an event, and call `Histo::measure()`.
struct Measure<'h> {
    histo: &'h Histo,
    start: f64,
}

impl<'h> Measure<'h> {
    /// The time delta from ctor to dtor is recorded in `histo`.
    #[inline(always)]
    pub fn new(histo: &'h Histo) -> Measure<'h> {
        Measure {
            histo,
            start: clock(),
        }
    }
}

impl<'h> Drop for Measure<'h> {
    #[inline(always)]
    fn drop(&mut self) {
        self.histo.measure(clock() - self.start);
    }
}

/// Measure the time spent on calling a given function in a given `Histo`.
#[inline(always)]
fn measure<F: FnOnce() -> R, R>(histo: &Histo, f: F) -> R {
    let _measure = Measure::new(histo);
    f()
}

/// This function is useful for inducing random jitter into our atomic
/// operations, shaking out more possible interleavings quickly. It gets
/// fully elliminated by the compiler in non-test code.
#[inline(always)]
pub fn debug_delay() {
    #[cfg(any(test, feature = "lock_free_delays"))]
    {
        use rand::{thread_rng, Rng};

        if thread_rng().gen_weighted_bool(1000) {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

/// Allows arbitrary logic to be injected into mere operations of the `PageCache`.
pub type MergeOperator =
    fn(key: &[u8], last_value: Option<&[u8]>, new_merge: &[u8])
        -> Option<Vec<u8>>;
