//! We believe operators of stateful systems should get as much sleep as they
//! want. We take testing seriously, and we take pains to avoid the pesticide
//! paradox wherever possible.
//!
//! sled uses the following testing strategies, and is eager to expand their
//! use:
//!
//! * quickcheck-based model testing on the Tree, `PageCache`, and Log
//! * proptest-based model testing on the `PageTable` using the [model](https://docs.rs/model)
//!   testing library
//! * linearizability testing on the `PageTable` using the [model](https://docs.rs/model)
//!   testing library
//! * deterministic concurrent model testing using linux realtime priorities,
//!   approaching the utility of the PULSE system available for the Erlang
//!   ecosystem
//! * `ThreadSanitizer` on a concurrent workload
//! * `LeakSanitizer` on a concurrent workload
//! * failpoints with model testing: at every IO operation, a test can cause the
//!   system to simulate a crash
//! * crash testing: processes are quickly spun up and then `kill -9`'d while
//!   recovering and writing. the recovered data is verified to recover the log
//!   in-order, stopping at the first torn log message or incomplete segment
//! * fuzzing: libfuzzer is used to generate sequences of operations on the Tree
//! * TLA+ has been used to model some of the concurrent algorithms, but much
//!   more is necessary
