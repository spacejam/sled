use std::sync::Arc;

use self::reader::LogReader;
use super::*;

#[doc(hidden)]
pub const MSG_HEADER_LEN: usize = 15;

#[doc(hidden)]
pub const SEG_HEADER_LEN: usize = 18;

#[doc(hidden)]
pub const SEG_TRAILER_LEN: usize = 10;

/// A sequential store which allows users to create
/// reservations placed at known log offsets, used
/// for writing persistent data structures that need
/// to know where to find persisted bits in the future.
///
/// # Working with `Log`
///
/// ```
/// let conf = pagecache::ConfigBuilder::new()
///     .temporary(true)
///     .segment_mode(pagecache::SegmentMode::Linear)
///     .build();
/// let log = pagecache::Log::start_raw_log(conf).unwrap();
/// let (first_lsn, _first_offset) = log.write(b"1".to_vec()).unwrap();
/// log.write(b"22".to_vec()).unwrap();
/// log.write(b"333".to_vec()).unwrap();
///
/// // stick an abort in the middle, which should not be returned
/// let res = log.reserve(b"never_gonna_hit_disk".to_vec()).unwrap();
/// res.abort().unwrap();
///
/// log.write(b"4444".to_vec());
/// let (last_lsn, _last_offset) = log.write(b"55555".to_vec()).unwrap();
/// log.make_stable(last_lsn).unwrap();
/// let mut iter = log.iter_from(first_lsn);
/// assert_eq!(iter.next().unwrap().2, b"1".to_vec());
/// assert_eq!(iter.next().unwrap().2, b"22".to_vec());
/// assert_eq!(iter.next().unwrap().2, b"333".to_vec());
/// assert_eq!(iter.next().unwrap().2, b"4444".to_vec());
/// assert_eq!(iter.next().unwrap().2, b"55555".to_vec());
/// assert_eq!(iter.next(), None);
/// ```
pub struct Log {
    /// iobufs is the underlying lock-free IO write buffer.
    iobufs: Arc<IoBufs>,
    config: Config,
    /// Preiodically flushes `iobufs`.
    _flusher: periodic::Periodic<Arc<IoBufs>>,
}

unsafe impl Send for Log {}
unsafe impl Sync for Log {}

impl Drop for Log {
    fn drop(&mut self) {
        #[cfg(feature = "cpuprofiler")]
        {
            cpuprofiler::PROFILER.lock().unwrap().stop().unwrap();
        }
    }
}

impl periodic::Callback for Arc<IoBufs> {
    fn call(&self) {
        self.flush().unwrap();
    }
}

impl Log {
    /// Start the log, open or create the configured file,
    /// and optionally start the periodic buffer flush thread.
    pub fn start<R>(
        config: Config,
        snapshot: Snapshot<R>,
    ) -> CacheResult<Log, ()> {
        let iobufs = Arc::new(IoBufs::start(config.clone(), snapshot)?);
        let flusher = periodic::Periodic::new(
            "log flusher".to_owned(),
            iobufs.clone(),
            config.get_flush_every_ms(),
        );

        Ok(Log {
            iobufs: iobufs,
            config: config,
            _flusher: flusher,
        })
    }

    /// Starts a log for use without a materializer.
    pub fn start_raw_log(config: Config) -> CacheResult<Log, ()> {
        assert_eq!(config.get_segment_mode(), SegmentMode::Linear);
        let log_iter = raw_segment_iter_from(0, &config)?;

        let snapshot = advance_snapshot::<NullMaterializer, (), ()>(
            log_iter,
            Snapshot::default(),
            &config,
        )?;

        Log::start::<()>(config, snapshot)
    }

    /// Flushes any pending IO buffers to disk to ensure durability.
    pub fn flush(&self) -> CacheResult<(), ()> {
        for _ in 0..self.config.get_io_bufs() {
            self.iobufs.flush()?;
        }
        Ok(())
    }

    /// Reserve space in the log for a pending linearized operation.
    pub fn reserve(&self, buf: Vec<u8>) -> CacheResult<Reservation, ()> {
        self.iobufs.reserve(buf)
    }

    /// Write a buffer into the log. Returns the log sequence
    /// number and the file offset of the write.
    pub fn write(&self, buf: Vec<u8>) -> CacheResult<(Lsn, LogID), ()> {
        self.iobufs.reserve(buf).and_then(|res| res.complete())
    }

    /// Return an iterator over the log, starting with
    /// a specified offset.
    pub fn iter_from(&self, lsn: Lsn) -> LogIter {
        trace!("iterating from lsn {}", lsn);
        let io_buf_size = self.config.get_io_buf_size();
        let segment_base_lsn = lsn / io_buf_size as Lsn * io_buf_size as Lsn;
        let min_lsn = segment_base_lsn + SEG_HEADER_LEN as Lsn;

        // corrected_lsn accounts for the segment header length
        let corrected_lsn = std::cmp::max(lsn, min_lsn);

        let segment_iter =
            self.with_sa(|sa| sa.segment_snapshot_iter_from(corrected_lsn));

        LogIter {
            config: self.config.clone(),
            max_lsn: self.stable_offset(),
            cur_lsn: corrected_lsn,
            segment_base: None,
            segment_iter: segment_iter,
            segment_len: io_buf_size,
            use_compression: self.config.get_use_compression(),
            trailer: None,
        }
    }

    /// read a buffer from the disk
    pub fn read(&self, lsn: Lsn, lid: LogID) -> CacheResult<LogRead, ()> {
        trace!("reading log lsn {} lid {}", lsn, lid);
        self.make_stable(lsn)?;
        let f = self.config.file()?;

        let read = f.read_message(
            lid,
            self.config.get_io_buf_size(),
            self.config.get_use_compression(),
        );

        read.and_then(|log_read| match log_read {
            LogRead::Flush(read_lsn, _, _) => {
                assert_eq!(lsn, read_lsn);
                Ok(log_read)
            }
            _ => Ok(log_read),
        }).map_err(|e| e.into())
    }

    /// returns the current stable offset written to disk
    pub fn stable_offset(&self) -> Lsn {
        self.iobufs.stable()
    }

    /// blocks until the specified log sequence number has
    /// been made stable on disk
    pub fn make_stable(&self, lsn: Lsn) -> CacheResult<(), ()> {
        let _measure = Measure::new(&M.make_stable);

        // NB before we write the 0th byte of the file, stable  is -1
        while self.iobufs.stable() < lsn {
            self.iobufs.flush()?;

            // block until another thread updates the stable lsn
            let waiter = self.iobufs.intervals.lock().unwrap();

            if self.iobufs.stable() < lsn {
                trace!("waiting on cond var for make_stable({})", lsn);
                let _waiter =
                    self.iobufs.interval_updated.wait(waiter).unwrap();
            } else {
                trace!("make_stable({}) returning", lsn);
                break;
            }
        }

        Ok(())
    }

    // SegmentAccountant access for coordination with the `PageCache`
    pub(in io) fn with_sa<B, F>(&self, f: F) -> B
        where F: FnOnce(&mut SegmentAccountant) -> B
    {
        self.iobufs.with_sa(f)
    }
}

/// All log messages are prepended with this header
#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) struct MessageHeader {
    pub successful_flush: bool,
    pub lsn: Lsn,
    pub len: usize,
    pub crc16: [u8; 2],
}

/// A segment's header contains the new base LSN and a reference
/// to the previous log segment.
#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) struct SegmentHeader {
    pub lsn: Lsn,
    pub prev: LogID,
    pub ok: bool,
}

/// A segment's trailer contains the base Lsn for the segment.
/// It is written after the rest of the segment has been fsync'd,
/// and helps us indicate if a segment has been torn.
#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) struct SegmentTrailer {
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
    pub fn flush(self) -> Option<(Lsn, Vec<u8>, usize)> {
        match self {
            LogRead::Flush(lsn, bytes, len) => Some((lsn, bytes, len)),
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

    /// Retrieves the read bytes from a successful write, or
    /// panics with the provided error message.
    ///
    /// # Panics
    ///
    /// panics if `is_flush()` is false.
    pub fn expect<'a>(self, msg: &'a str) -> (Lsn, Vec<u8>, usize) {
        match self {
            LogRead::Flush(lsn, bytes, len) => (lsn, bytes, len),
            _ => panic!("{}", msg),
        }
    }
}

// NB we use a lot of xors below to differentiate between zeroed out
// data on disk and an lsn or crc16 of 0

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

        let crc16 = [buf[13] ^ 0xFF, buf[14] ^ 0xFF];

        MessageHeader {
            successful_flush: valid,
            lsn: lsn,
            len: len as usize,
            crc16: crc16,
        }
    }
}

impl Into<[u8; MSG_HEADER_LEN]> for MessageHeader {
    fn into(self) -> [u8; MSG_HEADER_LEN] {
        let mut buf = [0u8; MSG_HEADER_LEN];
        if self.successful_flush {
            buf[0] = 1;
        }

        // NB LSN actually gets written after the reservation
        // for the item is claimed, when we actually know the lsn,
        // in PageCache::reserve.
        let lsn_arr: [u8; 8] = unsafe { std::mem::transmute(self.lsn) };
        buf[1..9].copy_from_slice(&lsn_arr);

        let len_arr: [u8; 4] = unsafe { std::mem::transmute(self.len as u32) };
        buf[9..13].copy_from_slice(&len_arr);

        buf[13] = self.crc16[0] ^ 0xFF;
        buf[14] = self.crc16[1] ^ 0xFF;

        buf
    }
}

impl From<[u8; SEG_HEADER_LEN]> for SegmentHeader {
    fn from(buf: [u8; SEG_HEADER_LEN]) -> SegmentHeader {
        let crc16 = [buf[0] ^ 0xFF, buf[1] ^ 0xFF];

        let lsn_buf = &buf[2..10];
        let mut lsn_arr = [0u8; 8];
        lsn_arr.copy_from_slice(&*lsn_buf);
        let xor_lsn: Lsn = unsafe { std::mem::transmute(lsn_arr) };
        let lsn = xor_lsn ^ 0xFFFF_FFFF;

        let prev_lid_buf = &buf[10..18];
        let mut prev_lid_arr = [0u8; 8];
        prev_lid_arr.copy_from_slice(&*prev_lid_buf);
        let xor_prev_lid: LogID = unsafe { std::mem::transmute(prev_lid_arr) };
        let prev_lid = xor_prev_lid ^ 0xFFFF_FFFF_FFFF_FFFF;

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

        let xor_lsn = self.lsn ^ 0xFFFF_FFFF;
        let lsn_arr: [u8; 8] = unsafe { std::mem::transmute(xor_lsn) };
        buf[2..10].copy_from_slice(&lsn_arr);

        let xor_prev_lid = self.prev ^ 0xFFFF_FFFF_FFFF_FFFF;
        let prev_lid_arr: [u8; 8] =
            unsafe { std::mem::transmute(xor_prev_lid) };
        buf[10..18].copy_from_slice(&prev_lid_arr);

        let crc16 = crc16_arr(&prev_lid_arr);

        buf[0] = crc16[0] ^ 0xFF;
        buf[1] = crc16[1] ^ 0xFF;

        buf
    }
}

impl From<[u8; SEG_TRAILER_LEN]> for SegmentTrailer {
    fn from(buf: [u8; SEG_TRAILER_LEN]) -> SegmentTrailer {
        let crc16 = [buf[0] ^ 0xFF, buf[1] ^ 0xFF];

        let lsn_buf = &buf[2..10];
        let mut lsn_arr = [0u8; 8];
        lsn_arr.copy_from_slice(&*lsn_buf);
        let xor_lsn: Lsn = unsafe { std::mem::transmute(lsn_arr) };
        let lsn = xor_lsn ^ 0xFFFF_FFFF;

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

        let xor_lsn = self.lsn ^ 0xFFFF_FFFF;
        let lsn_arr: [u8; 8] = unsafe { std::mem::transmute(xor_lsn) };
        buf[2..10].copy_from_slice(&lsn_arr);

        let crc16 = crc16_arr(&lsn_arr);
        buf[0] = crc16[0] ^ 0xFF;
        buf[1] = crc16[1] ^ 0xFF;

        buf
    }
}
