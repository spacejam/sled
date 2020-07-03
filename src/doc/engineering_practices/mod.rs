//! Over the years that sled development has been active, some practices have
//! been collected that have helped to reduce risks throughout the codebase.
//!
//! # high-level
//!
//! * Start with the correctness requirements, ignore the performance impact
//!   until the end. You'll usually write something faster by focusing on
//!   keeping things minimal anyway.
//! * Throw away what can't be done in a day of coding. when you rewrite it
//!   tomorrow, it will be simpler.
//!
//! # testing
//!
//! * Don't do what can't be tested to be correct
//! * For concurrent code, it must be delayable to induce strange histories when
//!   running under test
//! * For IO code, it must have a failpoint so that IO errors can be injected
//!   during testing, as most bugs in cloud systems happen in the untested
//!   error-handling code
//! * Lean heavily into model-based property testing. sled should act like a
//!   `BTreeMap`, even after crashes
//!
//! # when testing and performance collide
//!
//! * cold code is buggy code
//! * if you see a significant optimization that will make correctness-critical
//!   codepaths harder to hit in tests, the optimization should only be created
//!   if it's possible to artificially increase the chances of hitting the
//!   codepath in test. Fox example, sled defaults to having an 8mb write
//!   buffer, but during tests we often turn it down to 512 bytes so that we can
//!   really abuse the correctness-critical aspects of its behavior.
//!
//! # numbers
//!
//! * No silent truncation should ever occur when converting numbers
//! * No silent wrapping should occur
//! * Crash or return a `ReportableBug` error in these cases
//! * `as` is forbidden for anything that could lose information
//! * Clippy's cast lints help us here, and it has been added to all pull
//!   requests

//! # package
//!
//! * dependencies should be minimized to keep compilation simple
//!
//! # coding conventions
//!
//! * Self should be avoided. We have a lot of code, and it provides no context
//!   if people are jumping around a lot. Redundancy here improves orientation.
