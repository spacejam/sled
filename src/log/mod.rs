use std::cell::UnsafeCell;
use std::fmt::{self, Debug};
use std::io::{self, SeekFrom};
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use super::*;

mod lss;
mod iobuf;
mod reservation;
mod periodic_flusher;

pub use self::lss::*;
pub use self::iobuf::*;
pub use self::reservation::*;

/// A trait for objects which facilitate log-structured storage.
pub trait Log: Sized + Send + Sync {
    /// Create a log offset reservation for a particular write,
    /// which may later be filled or canceled.
    fn reserve(&self, Vec<u8>) -> Reservation;

    /// Write a buffer to underlying storage.
    fn write(&self, Vec<u8>) -> LogID;

    /// Read a buffer from underlying storage.
    fn read(&self, id: LogID) -> io::Result<LogRead>;

    /// Return the current stable offset.
    fn stable_offset(&self) -> LogID;

    /// Try to flush all pending writes up until the
    /// specified log offset.
    fn make_stable(&self, id: LogID);

    /// Mark the provided message as deletable by the
    /// underlying storage.
    fn punch_hole(&self, id: LogID);

    /// Return the configuration in use by the system.
    fn config(&self) -> &Config;

    /// Return an iterator over the log, starting with
    /// a specified offset.
    fn iter_from(&self, id: LogID) -> LogIter<Self> {
        LogIter {
            next_offset: id,
            log: self,
        }
    }
}

pub enum LogRead {
    Flush(Vec<u8>, usize),
    Aborted(usize),
    Corrupted(usize),
}

pub struct LogIter<'a, L: 'a + Log> {
    next_offset: LogID,
    log: &'a L,
}

impl<'a, L> Iterator for LogIter<'a, L>
    where L: 'a + Log
{
    type Item = (LogID, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.log.read(self.next_offset) {
                Ok(LogRead::Flush(buf, len)) => {
                    let offset = self.next_offset;
                    self.next_offset += len as LogID + HEADER_LEN as LogID;
                    return Some((offset, buf));
                }
                Ok(LogRead::Aborted(len)) => {
                    self.next_offset += len as LogID + HEADER_LEN as LogID
                }
                _ => return None,
            }
        }
    }
}
