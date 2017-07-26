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
        let offset = self.next_offset;
        let log_read = self.log.read(self.next_offset);
        if let Ok(buf_opt) = log_read {
            if let Some(buf) = buf_opt {
                self.next_offset = offset + buf.len() as LogID + HEADER_LEN as LogID;
                return Some((offset, buf));
            }
        }
        return None;
    }
}
