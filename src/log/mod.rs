use std::cell::UnsafeCell;
use std::fmt::{self, Debug};
use std::fs;
use std::io::{self, Error, ErrorKind, SeekFrom};
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::thread;

#[cfg(target_os="linux")]
use libc::{FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE, fallocate};

use super::*;

mod memlog;
mod lss;
mod iobuf;
mod reservation;

pub use self::memlog::*;
pub use self::lss::*;
pub use self::iobuf::*;
pub use self::reservation::*;

pub trait Log: Send + Sync {
    fn reserve(&self, Vec<u8>) -> Reservation;
    fn write(&self, Vec<u8>) -> LogID;
    fn read(&self, id: LogID) -> io::Result<Option<Vec<u8>>>;
    fn stable_offset(&self) -> LogID;
    fn make_stable(&self, id: LogID);
    fn punch_hole(&self, id: LogID);
}
