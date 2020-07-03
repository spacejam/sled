//! ## Built-In Profiler
//!
//! To get a summary of latency histograms relating to different operations
//! you've used on a sled database, sled can print a nice table when the Db is
//! dropped by disabling the `no_metrics` default feature and setting
//! `print_profile_on_drop(true)` on a `ConfigBuilder`:
//!
//! ```rust
//! let config = sled::ConfigBuilder::new()
//!     .print_profile_on_drop(true)
//!     .build();
//!
//! let db = sled::Db::start(config).unwrap();
//! ```
//!
//! This is useful for finding outliers, general percentiles about usage, and
//! especially for debugging performance issues if you create an issue on
//! github.
//!
//! ## Use jemalloc
//!
//! jemalloc can dramatically improve performance in some situations, but you
//! should always measure performance before and after using it, because maybe
//! for some use cases it can cause regressions.
//!
//! Cargo.toml:
//! ```toml
//! [dependencies]
//! jemallocator = "0.1"
//! ```
//!
//! `your_code.rs`:
//! ```rust
//! #[global_allocator]
//! static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
//! ```
