//! #### what is sled?
//!
//! * an embedded kv store
//! * a construction kit for stateful systems
//! * ordered map API similar to a Rust `BTreeMap<Vec<u8>, Vec<u8>>`
//! * fully atomic single-key operations, supports CAS
//! * zero-copy reads
//! * merge operators
//! * forward and reverse iterators
//! * a monotonic ID generator capable of giving out 75-125+ million unique IDs
//!   per second, never double allocating even in the presence of crashes
//! * [zstd](https://github.com/facebook/zstd) compression (use the zstd build
//!   feature)
//! * cpu-scalable lock-free implementation
//! * SSD-optimized log-structured storage
//!
//! #### why another kv store?
//!
//! People face unnecessary hardship when working with existing embedded
//! databases. They tend to have sharp performance trade-offs, are difficult to
//! tune, have unclear consistency guarantees, and are generally inflexible.
//! Facebook uses distributed machine learning to find configurations that
//! achieve great performance for specific workloads on rocksdb.  Most engineers
//! don't have access to that kind of infrastructure. We would like to build
//! sled so that it can be optimized using simple local methods, with as little
//! user input as possible, and in many cases exceed the performance of popular
//! systems today.
//!
//! This is how we aim to improve the situation:
//!
//! 1. don't make the user think. the interface should be obvious.
//! 1. don't surprise users with performance traps.
//! 1. don't wake up operators. bring reliability techniques from academia into
//! real-world practice. 1. don't use so much electricity. our data structures
//! should play to modern hardware's strengths.
//!
//! sled is written by people with experience designing, building, testing, and
//! operating databases at high scales. we think the situation can be improved.
//!
//! #### targeted toward our vision of the future
//! Building a database takes years. Designers of databases make bets about
//! target usage and hardware. Here are the trends that we see, which we want to
//! optimize the experience around:
//!
//! 1. more cores on servers, spanning sockets and numa domains
//! 1. the vast majority of content consumption and generation happening on
//! phones 1. compute migrating to the edge, into CDNs
//! 1. conflict-free and OT-based replication techniques at the edge
//! 1. strongly-consistent replication techniques within and between datacenters
//! 1. event-driven architectures which benefit heavily from subscriber/watch
//! semantics

pub mod engineering_practices;
pub mod limits;
pub mod merge_operators;
pub mod performance_guide;
pub mod reactive_semantics;
pub mod sled_architectural_outlook;
pub mod testing_strategies;
