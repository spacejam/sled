#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate bincode;
extern crate libc;
extern crate rand;

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::sync::{Arc, mpsc::SyncSender, Mutex, MutexGuard};
use std::cell::RefCell;
use std::net::SocketAddr;
use std::fmt::Debug;

use serde::de::DeserializeOwned;
use serde::Serialize;

/// Spawn threads with linux realtime priorities, and
/// inherit the spawner's shared seeded random number
/// generator and clock.
#[cfg(target_os = "linux")]
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

    fn receive(
        &mut self,
        at: SystemTime,
        from: Self::Peer,
        msg: Self::Message,
    ) -> Vec<(Self::Peer, Self::Message)>;

    fn tick(&mut self, _at: SystemTime) -> Vec<(Self::Peer, Self::Message)> {
        vec![]
    }
}

thread_local! {
    pub static CONTEXT: RefCell<Arc<Context>> = RefCell::new(Arc::new(Context::default()));
}

fn context<'a>() -> Arc<Context> {
    CONTEXT.with(|c| {
        c.borrow().clone()
    })
}

fn set_context(context: Arc<Context>) {
    CONTEXT.with(|c| *c.borrow_mut() = context);
}

/// An inheritable context for children of a thread,
/// containing a clock, rng, filesystem, and network
/// transport handle.
pub mod context;

use context::Context;

pub enum Call {
    Sleep(Duration),
    AtomicOp,
    RwWriteLock(usize),
    RwWriteUnlock(usize),
    RwReadLock(usize),
    RwReadUnlock(usize),
    MutexLock(usize),
    MutexUnlock(usize),
    SyncFile,
    SyncFileMetadata,
    SendMsg(SocketAddr, Vec<u8>),
}
