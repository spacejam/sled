//! `pagecache` is a lock-free pagecache and log for building high-performance databases.
#![deny(missing_docs)]
#![cfg_attr(test, deny(warnings))]
#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]
#![cfg_attr(feature="clippy", allow(inline_always))]

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate crossbeam_epoch as epoch;
extern crate bincode;
extern crate historian;
#[macro_use]
extern crate lazy_static;
#[cfg(feature = "env_logger")]
extern crate env_logger;
#[macro_use]
extern crate log as _log;
#[cfg(feature = "rayon")]
extern crate rayon;
#[cfg(feature = "zstd")]
extern crate zstd;
#[cfg(feature = "cpuprofiler")]
extern crate cpuprofiler;
#[cfg(any(test, feature = "lock_free_delays"))]
extern crate rand;
#[cfg(unix)]
extern crate libc;

pub use ds::{Radix, Stack};

/// general-purpose configuration
pub use config::{Config, FinalConfig};
pub use io::*;

macro_rules! rep_no_copy {
    ($e:expr; $n:expr) => {
        {
            let mut v = Vec::with_capacity($n);
            for _ in 0..$n {
                v.push($e);
            }
            v
        }
    };
}

/// auxilliary data structures
pub mod ds;
mod io;
mod config;
mod hash;
mod metrics;

// use log::{Iter, MessageHeader, SegmentHeader, SegmentTrailer};
use metrics::Metrics;
use ds::*;
use hash::{crc16_arr, crc64};

/// An offset for a storage file segment.
pub type SegmentID = usize;

/// A log file offset.
pub type LogID = u64;

/// A logical sequence number.
pub type Lsn = isize;

/// A page identifier.
pub type PageID = usize;

type HPtr<'g, P> = epoch::Shared<'g, ds::stack::Node<io::CacheEntry<P>>>;

lazy_static! {
    /// A metric collector for all sled instances running in this
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
        static ref START: std::time::Instant = std::time::Instant::now();
    }

    START.elapsed()
}

/// This function is useful for inducing random jitter into our atomic
/// operations, shaking out more possible interleavings quickly. It gets
/// fully elliminated by the compiler in non-test code.
#[inline(always)]
pub fn debug_delay() {
    #[cfg(any(test, feature = "lock_free_delays"))]
    {
        use rand::{Rng, thread_rng};

        if thread_rng().gen_weighted_bool(1000) {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

// initialize env_logger and/or cpuprofiler if configured to do so.
fn global_init() {
    use std::sync::{ONCE_INIT, Once};

    static ONCE: Once = ONCE_INIT;

    ONCE.call_once(|| {
        #[cfg(feature = "env_logger")]
        {
            let format = |record: &_log::LogRecord| {
                format!(
                    "{:05} {:10} {:10} {}",
                    record.level(),
                    tn(),
                    record.location().module_path().split("::").last().unwrap(),
                    record.args()
                )
            };

            let mut builder = env_logger::LogBuilder::new();
            builder.format(format).filter(
                None,
                _log::LogLevelFilter::Info,
            );

            if std::env::var("RUST_LOG").is_ok() {
                builder.parse(&std::env::var("RUST_LOG").unwrap());
            }

            let _r = builder.init();
        }

        #[cfg(feature = "cpuprofiler")]
        {
            use std::env;

            let key = "CPUPROFILE";
            let path = match env::var(key) {
                Ok(val) => val,
                Err(_) => "sled.profile".to_owned(),
            };
            cpuprofiler::PROFILER.lock().unwrap().start(path).expect(
                "could not start cpu profiler!",
            );
        }
    });
}
