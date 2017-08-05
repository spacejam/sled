use std::cell::UnsafeCell;
use std::fmt::{self, Debug};
use std::fs;
use std::io::{self, Error, ErrorKind, SeekFrom};
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

#[cfg(target_os="linux")]
use libc::{FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE, fallocate};

use super::*;

mod lss;
mod iobuf;
mod reservation;

pub use self::lss::*;
pub use self::iobuf::*;
pub use self::reservation::*;

/// A trait for objects which facilitate log-structured storage.
pub trait Log: Send + Sync {
    /// Create a log offset reservation for a particular write,
    /// which may later be filled or canceled.
    fn reserve(&self, Vec<u8>) -> Reservation;

    /// Write a buffer to underlying storage.
    fn write(&self, Vec<u8>) -> LogID;

    /// Read a buffer from underlying storage.
    fn read(&self, id: LogID) -> io::Result<Result<Vec<u8>, usize>>;

    /// Return the current stable offset.
    fn stable_offset(&self) -> LogID;

    /// Try to flush all pending writes up until the
    /// specified log offset.
    fn make_stable(&self, id: LogID);

    /// Mark the provided message as deletable by the
    /// underlying storage.
    fn punch_hole(&self, id: LogID);

    /// Return the configuration in use by the system.
    fn config(&self) -> Config;

    /// Return an iterator over the log, starting with
    /// a specified offset.
    fn iter_from(&self, id: LogID) -> LogIter<Self>
        where Self: Sized
    {
        LogIter {
            next_offset: id,
            log: self,
        }
    }
}

pub struct LogIter<'a, T: 'a + Log> {
    next_offset: LogID,
    log: &'a T,
}


impl<'a, T: Log> Iterator for LogIter<'a, T> {
    type Item = (LogID, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let offset = self.next_offset;
            let log_read = self.log.read(self.next_offset);
            if let Ok(buf_opt) = log_read {
                match buf_opt {
                    Ok(buf) => {
                        self.next_offset += buf.len() as LogID + HEADER_LEN as LogID;
                        return Some((offset, buf));
                    }
                    Err(len) => self.next_offset += len as LogID + HEADER_LEN as LogID,
                }
            } else {
                return None;
            }
        }
    }
}
