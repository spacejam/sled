//! A collection of simple utilities for building
//! flake-free testable systems.
// #![deny(missing_docs)]
#![cfg_attr(test, deny(warnings))]

use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
pub trait Reactor: Sized {
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

fn context() -> Arc<Context> {
    CONTEXT.with(|c| c.borrow().clone())
}

fn set_context(context: Arc<Context>) {
    CONTEXT.with(|c| *c.borrow_mut() = context);
}

mod context;

use self::context::Context;

pub use self::context::{
    now, seed, set_seed, set_time, sleep, thread_rng, Rand,
};
