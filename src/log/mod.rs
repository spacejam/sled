use std::cell::UnsafeCell;
use std::fmt::{self, Debug};
use std::io::{self, SeekFrom};

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

#[derive(Debug, Clone)]
pub struct SegmentIter {
    pub buf: Vec<u8>,
    pub lsn: Lsn,
    pub read_offset: usize,
    pub position: LogID,
    pub max_encountered_lsn: Lsn,
    pub prev: LogID,
    pub trailer: Option<Lsn>,
    pub io_buf_size: usize,
}

impl SegmentIter {
    fn read_next(&mut self) -> Option<LogRead> {
        if self.read_offset + MSG_HEADER_LEN > self.buf.len() {
            let mut copy = self.clone();
            copy.buf = vec![];
            trace!("read_next ret none (at end of buf) at {:?}", copy);
            return None;
        }

        let rel_i = self.read_offset;

        trace!(
            "processing header for entry at id {}: {:?}",
            rel_i + self.position as usize,
            &self.buf[rel_i..rel_i + MSG_HEADER_LEN]
        );

        let mut header_arr = [0u8; MSG_HEADER_LEN];
        header_arr.copy_from_slice(&self.buf[rel_i..rel_i + MSG_HEADER_LEN]);
        let h: MessageHeader = header_arr.into();

        if h.lsn % self.io_buf_size as Lsn != rel_i as Lsn {
            trace!(
                "read corrupt log message with header {:?} at segment offset {}",
                h,
                rel_i
            );
            return None;
        }

        trace!("read header {:?} from offset {}", h, rel_i);

        let mut len = h.len;

        if len > self.buf.len() - rel_i {
            error!(
                "read corrupt message length, {} should be <= {}",
                len,
                self.buf.len() - rel_i
            );
            return Some(LogRead::Corrupted(len));
        } else if len == 0 && !h.valid {
            if h.crc16 != [0, 0] {
                // we've hit garbage, return None
                let mut copy = self.clone();
                copy.buf = vec![];
                error!("read_next none (crc failed) at {:?}", copy);
                return None;
            }
            len = MSG_HEADER_LEN;
            for byte in &self.buf[rel_i + MSG_HEADER_LEN..] {
                if *byte != 0 {
                    break;
                }
                len += 1;
            }
        }

        if !h.valid {
            self.read_offset += len;
            let mut copy = self.clone();
            copy.buf = vec![];
            trace!("read_next zeroed of len {} at {:?}", len, copy);
            return Some(LogRead::Zeroed(len));
        }

        let lower_bound = rel_i + MSG_HEADER_LEN;
        let upper_bound = lower_bound + len;

        if self.buf.len() < upper_bound {
            let mut copy = self.clone();
            copy.buf = vec![];
            trace!(
                "returning none (len is bigger than what's left in buffer) at {:?}",
                copy
            );
            return None;
        }

        let buf = self.buf[lower_bound..upper_bound].to_vec();

        let checksum = crc16_arr(&buf);
        if checksum != h.crc16 {
            // overan our valid buffer
            let mut copy = self.clone();
            copy.buf = vec![];
            trace!(
                "read_next none (checksum mismatch) at {:?} expected {:?} actual {:?}",
                copy,
                h.crc16,
                checksum
            );
            return None;
        }

        if h.lsn > self.max_encountered_lsn {
            trace!(
                "bumping segment max lsn from {} to {} in read_next",
                self.max_encountered_lsn,
                h.lsn
            );
            self.max_encountered_lsn = h.lsn;
        } else {
            // we've run over the valid LSN's for this segment
            trace!(
                "read_next none (regressive read) h.lsn {} max_encountered_lsn {}",
                h.lsn,
                self.max_encountered_lsn
            );
            return None;
        }

        self.read_offset = upper_bound;

        // trace!("read_next flush lsn {} at lid {} with len {}", h.lsn, lid, len);

        Some(LogRead::Flush(h.lsn, buf, len))
    }
}

pub struct LogIter<'a, L: 'a + Log> {
    min_lsn: Lsn,
    max_encountered_lsn: Lsn,
    log: &'a L,
    segment: Option<SegmentIter>,
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
                    Some(LogRead::Flush(read_lsn, buf, len)) => {
                        // println!("got iter item {} {:?} len: {}", read_lsn, buf, len);

                        if read_lsn < self.max_encountered_lsn {
                            // we've hit a tear, we should cut our scan short
                            error!(
                                "torn segment encountered, cutting log scan short at LSN {}",
                                self.max_encountered_lsn
                            );
                            return None;
                        } else {
                            // println!( "bumping max_encountered_lsn from {} to {} in LogIter", self.max_encountered_lsn, read_lsn);
                            assert!(read_lsn > self.max_encountered_lsn);
                            self.max_encountered_lsn = read_lsn;
                        }

                        if (segment.lsn + (MSG_HEADER_LEN + segment.read_offset) as Lsn) <
                            self.min_lsn
                        {
                            // we have not yet reached our desired offset
                            // println!("bailing out early");
                            continue;
                        }

                        let base = segment.position;
                        let rel_i = segment.read_offset - (len + MSG_HEADER_LEN);

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

                // make sure our segment is stable before reading it
                self.log.make_stable(lsn);

                let next_segment = self.log.config().read_segment(lid);
                if let Err(_e) = next_segment {
                    error!("log read_segment failed: {:?}", _e);
                    return None;
                }

                let next_segment = next_segment.unwrap();
                // println!("got next segment {:?}", next_segment);
                // FIXME this is failing on a c4 stress2 --get=16 --set=16 --key-len=22
                // println!("read segment from lid {}", lid);
                assert_eq!(lsn, next_segment.lsn);

                self.segment = Some(next_segment);
                continue;
            }
            self.segment.take();
        }
    }
}

/// All log messages are prepended with this header
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct MessageHeader {
    pub valid: bool,
    pub lsn: Lsn,
    pub len: usize,
    pub crc16: [u8; 2],
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

/// A segment's header contains the new base LSN and a reference to the previous log segment.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct SegmentHeader {
    pub lsn: Lsn,
    pub prev: LogID,
    pub ok: bool,
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

/// A segment's trailer contains the base Lsn for the segment. It is written
/// after the rest of the segment has been fsync'd, and helps us indicate
/// if a segment has been torn.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct SegmentTrailer {
    pub lsn: Lsn,
    pub ok: bool,
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
