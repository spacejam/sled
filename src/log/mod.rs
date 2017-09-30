use std::cell::UnsafeCell;
use std::fmt::{self, Debug};
use std::io::{self, SeekFrom};

use super::*;

mod lss;
mod iobuf;
mod reservation;
mod periodic_flusher;
mod segment_accountant;
mod iterator;
mod reader;

pub use self::lss::*;
pub use self::iobuf::*;
pub use self::reservation::*;
pub use self::segment_accountant::*;
pub use self::iterator::*;
pub use self::reader::*;

/// All log messages are prepended with this header
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct MessageHeader {
    pub valid: bool,
    pub lsn: Lsn,
    pub len: usize,
    pub crc16: [u8; 2],
}

/// A segment's header contains the new base LSN and a reference to the previous log segment.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct SegmentHeader {
    pub lsn: Lsn,
    pub prev: LogID,
    pub ok: bool,
}

/// A segment's trailer contains the base Lsn for the segment. It is written
/// after the rest of the segment has been fsync'd, and helps us indicate
/// if a segment has been torn.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct SegmentTrailer {
    pub lsn: Lsn,
    pub ok: bool,
}

#[doc(hidden)]
#[derive(Debug)]
pub enum LogRead {
    Flush(Lsn, Vec<u8>, usize),
    Zeroed(usize),
    Corrupted(usize),
}

impl LogRead {
    /// Optionally return successfully read bytes, or None if
    /// the data was corrupt or this log entry was aborted.
    pub fn flush(&self) -> Option<(Lsn, Vec<u8>, usize)> {
        match *self {
            LogRead::Flush(lsn, ref bytes, len) => Some((lsn, bytes.clone(), len)),
            _ => None,
        }
    }

    /// Return true if we read a completed write successfully.
    pub fn is_flush(&self) -> bool {
        match *self {
            LogRead::Flush(_, _, _) => true,
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
    pub fn unwrap(self) -> (Lsn, Vec<u8>, usize) {
        match self {
            LogRead::Flush(lsn, bytes, len) => (lsn, bytes, len),
            _ => panic!("called unwrap on a non-flush LogRead"),
        }
    }
}

impl From<[u8; MSG_HEADER_LEN]> for MessageHeader {
    fn from(buf: [u8; MSG_HEADER_LEN]) -> MessageHeader {
        let valid = buf[0] == 1;

        let lsn_buf = &buf[1..9];
        let mut lsn_arr = [0u8; 8];
        lsn_arr.copy_from_slice(&*lsn_buf);
        let lsn: Lsn = unsafe { std::mem::transmute(lsn_arr) };

        let len_buf = &buf[9..13];
        let mut len_arr = [0u8; 4];
        len_arr.copy_from_slice(&*len_buf);
        let len: u32 = unsafe { std::mem::transmute(len_arr) };

        let crc16 = [buf[13], buf[14]];

        MessageHeader {
            valid: valid,
            lsn: lsn,
            len: len as usize,
            crc16: crc16,
        }
    }
}

impl Into<[u8; MSG_HEADER_LEN]> for MessageHeader {
    fn into(self) -> [u8; MSG_HEADER_LEN] {
        let mut buf = [0u8; MSG_HEADER_LEN];
        if self.valid {
            buf[0] = 1;
        }

        // NB LSN actually gets written after the reservation
        // for the item is claimed, when we actually know the lsn,
        // in PageCache::reserve.
        let lsn_arr: [u8; 8] = unsafe { std::mem::transmute(self.lsn) };
        buf[1..9].copy_from_slice(&lsn_arr);

        let len_arr: [u8; 4] = unsafe { std::mem::transmute(self.len as u32) };
        buf[9..13].copy_from_slice(&len_arr);

        buf[13] = self.crc16[0];
        buf[14] = self.crc16[1];

        // println!("writing MessageHeader buf: {:?}", buf);

        buf
    }
}

impl From<[u8; SEG_HEADER_LEN]> for SegmentHeader {
    fn from(buf: [u8; SEG_HEADER_LEN]) -> SegmentHeader {
        // println!("pulling segment header out of {:?}", buf);
        let crc16 = [buf[0], buf[1]];

        let lsn_buf = &buf[2..10];
        let mut lsn_arr = [0u8; 8];
        lsn_arr.copy_from_slice(&*lsn_buf);
        let lsn: Lsn = unsafe { std::mem::transmute(lsn_arr) };

        let prev_lid_buf = &buf[10..18];
        let mut prev_lid_arr = [0u8; 8];
        prev_lid_arr.copy_from_slice(&*prev_lid_buf);
        let prev_lid: LogID = unsafe { std::mem::transmute(prev_lid_arr) };

        let crc16_tested = crc16_arr(&prev_lid_arr);

        SegmentHeader {
            lsn: lsn,
            prev: prev_lid,
            ok: crc16_tested == crc16,
        }
    }
}

impl Into<[u8; SEG_HEADER_LEN]> for SegmentHeader {
    fn into(self) -> [u8; SEG_HEADER_LEN] {
        let mut buf = [0u8; SEG_HEADER_LEN];

        let lsn_arr: [u8; 8] = unsafe { std::mem::transmute(self.lsn) };
        buf[2..10].copy_from_slice(&lsn_arr);

        let prev_lid_arr: [u8; 8] = unsafe { std::mem::transmute(self.prev) };
        buf[10..18].copy_from_slice(&prev_lid_arr);

        let crc16 = crc16_arr(&prev_lid_arr);
        buf[0] = crc16[0];
        buf[1] = crc16[1];

        // println!("writing segment header {:?}", buf);

        buf
    }
}

impl From<[u8; SEG_TRAILER_LEN]> for SegmentTrailer {
    fn from(buf: [u8; SEG_TRAILER_LEN]) -> SegmentTrailer {
        let crc16 = [buf[0], buf[1]];

        let lsn_buf = &buf[2..10];
        let mut lsn_arr = [0u8; 8];
        lsn_arr.copy_from_slice(&*lsn_buf);
        let lsn: LogID = unsafe { std::mem::transmute(lsn_arr) };

        let crc16_tested = crc16_arr(&lsn_arr);

        SegmentTrailer {
            lsn: lsn,
            ok: crc16_tested == crc16,
        }
    }
}

impl Into<[u8; SEG_TRAILER_LEN]> for SegmentTrailer {
    fn into(self) -> [u8; SEG_TRAILER_LEN] {
        let mut buf = [0u8; SEG_TRAILER_LEN];

        let lsn_arr: [u8; 8] = unsafe { std::mem::transmute(self.lsn) };
        buf[2..10].copy_from_slice(&lsn_arr);

        let crc16 = crc16_arr(&lsn_arr);
        buf[0] = crc16[0];
        buf[1] = crc16[1];

        buf
    }
}
