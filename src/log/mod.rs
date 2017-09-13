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
mod segment_accountant;

pub use self::lss::*;
pub use self::iobuf::*;
pub use self::reservation::*;
pub use self::segment_accountant::*;

/// A trait for objects which facilitate log-structured storage.
pub trait Log: Sized {
    /// Create a log offset reservation for a particular write,
    /// which may later be filled or canceled.
    fn reserve(&self, Vec<u8>) -> Reservation;

    /// Write a buffer to underlying storage.
    fn write(&self, Vec<u8>) -> LogID;

    /// Read a buffer from underlying storage.
    fn read(&self, id: LogID) -> io::Result<LogRead>;

    /// Read a segment of log messages.
    fn read_segment(&self, id: LogID) -> io::Result<Segment>;

    /// Return the current stable offset.
    fn stable_offset(&self) -> LogID;

    /// Try to flush all pending writes up until the
    /// specified log sequence number.
    fn make_stable(&self, lsn: Lsn);

    /// Return the configuration in use by the system.
    fn config(&self) -> &Config;

    /// Return an iterator over the log, starting with
    /// a specified log sequence number.
    fn iter_from(&self, lsn: Lsn) -> LogIter<Self>;
}

#[doc(hidden)]
#[derive(Debug)]
pub enum LogRead {
    Flush(Vec<u8>, usize),
    Zeroed(usize),
    Corrupted(usize),
}

impl LogRead {
    /// Optionally return successfully read bytes, or None if
    /// the data was corrupt or this log entry was aborted.
    pub fn flush(&self) -> Option<Vec<u8>> {
        match *self {
            LogRead::Flush(ref bytes, _) => Some(bytes.clone()),
            _ => None,
        }
    }

    /// Return true if we read a completed write successfully.
    pub fn is_flush(&self) -> bool {
        match *self {
            LogRead::Flush(_, _) => true,
            _ => false,
        }
    }

    /// Return true if we read an aborted flush.
    pub fn is_zeroed(&self) -> bool {
        match *self {
            LogRead::Zeroed(_) => true,
            _ => false,
        }
    }

    /// Return true if we read a corrupted log entry.
    pub fn is_corrupt(&self) -> bool {
        match *self {
            LogRead::Corrupted(_) => true,
            _ => false,
        }
    }

    /// Retrieve the read bytes from a completed, successful write.
    ///
    /// # Panics
    ///
    /// panics if `is_flush()` is false.
    pub fn unwrap(self) -> Vec<u8> {
        match self {
            LogRead::Flush(bytes, _) => bytes,
            _ => panic!("called unwrap on a non-flush LogRead"),
        }
    }
}

pub struct Segment {
    buf: Vec<u8>,
    lsn: Lsn,
    read_offset: usize,
    position: LogID,
}

impl Segment {
    fn read_next(&mut self) -> Option<LogRead> {
        if self.read_offset + HEADER_LEN > self.buf.len() {
            return None;
        }

        let rel_i = self.read_offset;

        let valid = self.buf[rel_i] == 1;
        let len_buf = &self.buf[rel_i + 1..rel_i + 5];
        let crc16_buf = &self.buf[rel_i + 5..rel_i + HEADER_LEN];

        let len32: u32 =
            unsafe { std::mem::transmute([len_buf[0], len_buf[1], len_buf[2], len_buf[3]]) };
        let mut len = len32 as usize;

        if len > self.buf.len() - self.read_offset {
            #[cfg(feature = "log")]
            error!(
                "log read invalid message length, {} should be <= {}",
                len,
                self.buf.len() - self.read_offset
            );
            return Some(LogRead::Corrupted(len));
        } else if len == 0 && !valid {
            assert_eq!(crc16_buf, [0, 0]);
            len = HEADER_LEN;
            for byte in &self.buf[rel_i + HEADER_LEN..] {
                if *byte != 0 {
                    break;
                }
                len += 1;
            }
        }

        if !valid {
            self.read_offset += len;
            return Some(LogRead::Zeroed(len));
        }

        let lower_bound = rel_i + HEADER_LEN;
        let upper_bound = lower_bound + len;
        let buf = self.buf[lower_bound..upper_bound].to_vec();

        self.read_offset = upper_bound;

        Some(LogRead::Flush(buf, len))
    }
}

pub struct LogIter<'a, L: 'a + Log> {
    log: &'a L,
    segment: Option<Segment>,
    segment_iter: Box<Iterator<Item = (Lsn, LogID)>>,
}

impl<'a, L> Iterator for LogIter<'a, L>
    where L: 'a + Log
{
    type Item = (Lsn, LogID, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref mut segment) = self.segment {
                // if segment.read_next is None, roll it
                match segment.read_next() {
                    Some(LogRead::Zeroed(_)) => continue,
                    Some(LogRead::Flush(buf, len)) => {
                        let base = segment.position;
                        let rel_i = segment.read_offset - (len + HEADER_LEN);
                        return Some((segment.lsn, base + rel_i as LogID, buf));
                    }
                    Some(LogRead::Corrupted(_)) => return None,
                    None => {}
                }
            } else {
                // if segment is None, read_segment
                let next = self.segment_iter.next();
                if next.is_none() {
                    return None;
                }
                let (lsn, lid) = next.unwrap();
                let next_segment = self.log.read_segment(lid);
                if let Err(_e) = next_segment {
                    #[cfg(feature = "log")]
                    error!("log read_segment failed: {:?}", _e);
                    return None;
                }

                let next_segment = next_segment.unwrap();
                assert_eq!(lsn, next_segment.lsn);

                self.segment = Some(next_segment);
                continue;
            }
            self.segment.take();
        }
    }
}
