//! A collection of simple utilities for building
//! flake-free testable systems.
// #![deny(missing_docs)]
#![cfg_attr(test, deny(warnings))]

extern crate serde;
extern crate libc;
extern crate rand;

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::sync::{Arc, Mutex};
use std::cell::RefCell;
use std::fmt::Debug;

use serde::de::DeserializeOwned;
use serde::Serialize;

/// Spawn threads with linux realtime priorities, and
/// inherit the spawner's shared seeded random number
/// generator and clock.
pub mod spawn;

/// Files that can simulate power failures by losing
/// data written after the last sync.
pub mod file;

/// A trait for building networked systems
/// that can be plugged into simulated networks
/// and partition tested in accelerated time.
pub trait Reactor: Debug + Clone {
    type Peer: std::net::ToSocketAddrs;
    type Message: Serialize + DeserializeOwned;
    type Config;
    type Error;

    const PERIODIC_INTERVAL: Option<Duration> = None;

    fn start(config: Self::Config) -> Result<Self, Self::Error>;

    fn receive(
        &mut self,
        from: Self::Peer,
        msg: Self::Message,
    ) -> Vec<(Self::Peer, Self::Message)>;

    fn periodic(&mut self) -> Vec<(Self::Peer, Self::Message)> {
        unimplemented!()
    }
}

thread_local! {
    pub static CONTEXT: RefCell<Arc<Context>> = RefCell::new(Arc::new(Context::default()));
}

fn context<'a>() -> Arc<Context> {
    CONTEXT.with(|c| c.borrow().clone())
}

fn set_context(context: Arc<Context>) {
    CONTEXT.with(|c| *c.borrow_mut() = context);
}

mod context;

use context::Context;

pub use context::{Rand, now, seed, set_seed, set_time, sleep, thread_rng};
