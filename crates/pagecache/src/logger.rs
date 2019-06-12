//! # Working with `Log`
//!
//! ```
//! let config = pagecache::ConfigBuilder::new()
//!     .temporary(true)
//!     .segment_mode(pagecache::SegmentMode::Linear)
//!     .build();
//! let log = pagecache::Log::start_raw_log(config).unwrap();
//! let (first_lsn, _first_offset) = log.write(b"1").unwrap();
//! log.write(b"22").unwrap();
//! log.write(b"333").unwrap();
//!
//! // stick an abort in the middle, which should not be returned
//! let res = log.reserve(b"never_gonna_hit_disk").unwrap();
//! res.abort().unwrap();
//!
//! log.write(b"4444");
//! let (last_lsn, _last_offset) = log.write(b"55555").unwrap();
//! log.make_stable(last_lsn).unwrap();
//! let mut iter = log.iter_from(first_lsn);
//! assert_eq!(iter.next().unwrap().2, b"1");
//! assert_eq!(iter.next().unwrap().2, b"22");
//! assert_eq!(iter.next().unwrap().2, b"333");
//! assert_eq!(iter.next().unwrap().2, b"4444");
//! assert_eq!(iter.next().unwrap().2, b"55555");
//! assert_eq!(iter.next(), None);
//! ```
use std::sync::Arc;

use super::*;

/// A sequential store which allows users to create
/// reservations placed at known log offsets, used
/// for writing persistent data structures that need
/// to know where to find persisted bits in the future.
#[derive(Debug)]
pub struct Log {
    /// iobufs is the underlying lock-free IO write buffer.
    pub(super) iobufs: Arc<IoBufs>,
    pub(crate) config: Config,
}

unsafe impl Send for Log {}

impl Log {
    /// Start the log, open or create the configured file,
    /// and optionally start the periodic buffer flush thread.
    pub fn start(config: Config, snapshot: Snapshot) -> Result<Log> {
        let iobufs = Arc::new(IoBufs::start(config.clone(), snapshot)?);

        Ok(Log { iobufs, config })
    }

    /// Starts a log for use without a materializer.
    pub fn start_raw_log(config: Config) -> Result<Log> {
        assert_eq!(config.segment_mode, SegmentMode::Linear);
        let log_iter = raw_segment_iter_from(0, &config)?;

        let snapshot = advance_snapshot::<NullMaterializer, ()>(
            log_iter,
            Snapshot::default(),
            &config,
        )?;

        Log::start(config, snapshot)
    }

    /// Flushes any pending IO buffers to disk to ensure durability.
    /// Returns the number of bytes written during this call.
    pub fn flush(&self) -> Result<usize> {
        iobuf::flush(&self.iobufs)
    }

    /// Write a buffer into the log. Returns the log sequence
    /// number and the file offset of the write.
    pub fn write<B>(&self, buf: B) -> Result<(Lsn, DiskPtr)>
    where
        B: AsRef<[u8]>,
    {
        self.reserve(buf.as_ref()).and_then(|res| res.complete())
    }

    /// Return an iterator over the log, starting with
    /// a specified offset.
    pub fn iter_from(&self, lsn: Lsn) -> LogIter {
        self.iobufs.iter_from(lsn)
    }

    /// read a buffer from the disk
    pub fn read(&self, lsn: Lsn, ptr: DiskPtr) -> Result<LogRead> {
        trace!("reading log lsn {} ptr {}", lsn, ptr);

        self.make_stable(lsn)?;

        if ptr.is_inline() {
            let lid = ptr.lid();
            let f = &self.config.file;

            f.read_message(lid, lsn, &self.config)
        } else {
            // we short-circuit the inline read
            // here because it might not still
            // exist in the inline log.
            let (_lid, blob_ptr) = ptr.blob();
            read_blob(blob_ptr, &self.config)
                .map(|buf| LogRead::Blob(lsn, buf, blob_ptr))
        }
    }

    /// returns the current stable offset written to disk
    pub fn stable_offset(&self) -> Lsn {
        self.iobufs.stable()
    }

    /// blocks until the specified log sequence number has
    /// been made stable on disk. Returns the number of
    /// bytes written during this call.
    pub fn make_stable(&self, lsn: Lsn) -> Result<usize> {
        iobuf::make_stable(&self.iobufs, lsn)
    }

    // SegmentAccountant access for coordination with the `PageCache`
    pub(crate) fn with_sa<B, F>(&self, f: F) -> B
    where
        F: FnOnce(&mut SegmentAccountant) -> B,
    {
        self.iobufs.with_sa(f)
    }

    /// Reserve a replacement buffer for a previously written
    /// blob write. This ensures the message header has the
    /// proper blob flag set.
    pub(super) fn rewrite_blob_ptr<'a>(
        &'a self,
        blob_ptr: BlobPointer,
    ) -> Result<Reservation<'a>> {
        let lsn_buf: [u8; std::mem::size_of::<BlobPointer>()] =
            u64_to_arr(blob_ptr as u64);

        self.reserve_inner(&lsn_buf, true)
    }

    /// Tries to claim a reservation for writing a buffer to a
    /// particular location in stable storge, which may either be
    /// completed or aborted later. Useful for maintaining
    /// linearizability across CAS operations that may need to
    /// persist part of their operation.
    #[allow(unused)]
    pub fn reserve<'a>(&'a self, raw_buf: &[u8]) -> Result<Reservation<'a>> {
        let mut _compressed: Option<Vec<u8>> = None;
        let mut buf = raw_buf;

        #[cfg(feature = "compression")]
        {
            if self.config.use_compression {
                use zstd::block::compress;

                let _measure = Measure::new(&M.compress);

                let compressed_buf =
                    compress(buf, self.config.compression_factor).unwrap();
                _compressed = Some(compressed_buf);

                buf = _compressed.as_ref().unwrap();
            }
        }

        self.reserve_inner(buf, false)
    }

    /// Writes a sequence of buffers to stable storge,
    /// leaving a batch marker beforehand to ensure
    /// atomic recovery of the entire batch. If
    /// during recovery the batch is only partially
    /// recoverable, the entire recovery process will
    /// stop before any updates are processed.
    pub fn write_batch<'a, 'b>(
        &'a self,
        bufs: &'b [&'b [u8]],
    ) -> Result<Vec<(Lsn, DiskPtr)>> {
        // reserve space for pointer
        let mut batch_res = self.reserve(&[0; std::mem::size_of::<Lsn>()])?;
        let mut pointers = Vec::with_capacity(bufs.len());
        for buf in bufs {
            pointers.push(self.write(buf)?);
        }
        if let Some((last_lsn, _last_ptr)) = pointers.last() {
            batch_res.mark_writebatch(*last_lsn);
            batch_res.complete()?;
        } else {
            batch_res.abort()?;
        }

        Ok(pointers)
    }

    fn reserve_inner<'a>(
        &'a self,
        buf: &[u8],
        is_blob_rewrite: bool,
    ) -> Result<Reservation<'a>> {
        let _measure = Measure::new(&M.reserve_lat);

        let n_io_bufs = self.config.io_bufs;

        let total_buf_len = MSG_HEADER_LEN + buf.len();

        M.reserve_sz.measure(total_buf_len as f64);

        let max_buf_size = (self.config.io_buf_size
            / MINIMUM_ITEMS_PER_SEGMENT)
            - SEG_HEADER_LEN;

        let over_blob_threshold = total_buf_len > max_buf_size;

        assert!(!(over_blob_threshold && is_blob_rewrite));

        let inline_buf_len = if over_blob_threshold {
            MSG_HEADER_LEN + std::mem::size_of::<Lsn>()
        } else {
            total_buf_len
        };

        trace!("reserving buf of len {}", inline_buf_len);

        let mut printed = false;
        macro_rules! trace_once {
            ($($msg:expr),*) => {
                if !printed {
                    trace!($($msg),*);
                    printed = true;
                }
            };
        }

        let backoff = Backoff::new();

        loop {
            M.log_reservation_attempted();

            // don't continue if the system
            // has encountered an issue.
            if let Err(e) = self.config.global_error() {
                self.iobufs.interval_updated.notify_all();
                return Err(e);
            }

            debug_delay();
            let written_bufs = self.iobufs.written_bufs.load(SeqCst);
            debug_delay();
            let current_buf = self.iobufs.current_buf.load(SeqCst);
            let idx = assert_usize(current_buf % n_io_bufs as u64);

            if written_bufs > current_buf {
                // This can happen because a reservation can finish up
                // before the sealing thread gets around to bumping
                // current_buf.
                trace_once!("written ahead of sealed, spinning");
                backoff.spin();
                continue;
            }

            if assert_usize(current_buf - written_bufs) >= n_io_bufs {
                // if written is too far behind, we need to
                // spin while it catches up to avoid overlap
                trace_once!("old io buffer not written yet, spinning");
                if backoff.is_completed() {
                    // use a condition variable to wait until
                    // we've updated the written_bufs counter.
                    let _measure =
                        Measure::new(&M.reserve_written_condvar_wait);

                    let mut buf_mu = self.iobufs.buf_mu.lock().unwrap();
                    while written_bufs == self.iobufs.written_bufs.load(SeqCst)
                    {
                        buf_mu = self.iobufs.buf_updated.wait(buf_mu).unwrap();
                    }
                } else {
                    backoff.snooze();
                }

                continue;
            }

            // load current header value
            let iobuf = &self.iobufs.bufs[idx];
            let header = iobuf.get_header();

            // skip if already sealed
            if iobuf::is_sealed(header) {
                // already sealed, start over and hope cur
                // has already been bumped by sealer.
                trace_once!("io buffer already sealed, spinning");

                if backoff.is_completed() {
                    // use a condition variable to wait until
                    // we've updated the current_buf counter.
                    let _measure =
                        Measure::new(&M.reserve_current_condvar_wait);
                    let mut buf_mu = self.iobufs.buf_mu.lock().unwrap();
                    while current_buf == self.iobufs.current_buf.load(SeqCst) {
                        buf_mu = self.iobufs.buf_updated.wait(buf_mu).unwrap();
                    }
                } else {
                    backoff.snooze();
                }

                continue;
            }

            // try to claim space
            let buf_offset = iobuf::offset(header);
            let prospective_size = buf_offset + inline_buf_len;
            let would_overflow = prospective_size > iobuf.get_capacity();
            if would_overflow {
                // This buffer is too full to accept our write!
                // Try to seal the buffer, and maybe write it if
                // there are zero writers.
                trace_once!("io buffer too full, spinning");
                iobuf::maybe_seal_and_write_iobuf(
                    &self.iobufs,
                    idx,
                    header,
                    true,
                )?;
                backoff.spin();
                continue;
            }

            // attempt to claim by incrementing an unsealed header
            let bumped_offset = iobuf::bump_offset(header, inline_buf_len);

            // check for maxed out IO buffer writers
            if iobuf::n_writers(bumped_offset) == iobuf::MAX_WRITERS {
                trace_once!(
                    "spinning because our buffer has {} writers already",
                    iobuf::MAX_WRITERS
                );
                backoff.snooze();
                continue;
            }

            let claimed = iobuf::incr_writers(bumped_offset);

            if iobuf.cas_header(header, claimed).is_err() {
                // CAS failed, start over
                trace_once!("CAS failed while claiming buffer slot, spinning");
                backoff.spin();
                continue;
            }

            let lid = iobuf.get_lid();

            // if we're giving out a reservation,
            // the writer count should be positive
            assert_ne!(iobuf::n_writers(claimed), 0);

            // should never have claimed a sealed buffer
            assert!(!iobuf::is_sealed(claimed));

            // MAX is used to signify unreadiness of
            // the underlying IO buffer, and if it's
            // still set here, the buffer counters
            // used to choose this IO buffer
            // were incremented in a racy way.
            assert_ne!(
                lid,
                LogId::max_value(),
                "fucked up on idx {}\n{:?}",
                idx,
                self
            );

            let out_buf = unsafe { (*iobuf.buf.get()).as_mut_slice() };

            let res_start = buf_offset;
            let res_end = res_start + inline_buf_len;

            let destination = &mut (out_buf)[res_start..res_end];
            let reservation_offset = lid + buf_offset as LogId;

            let reservation_lsn = iobuf.get_lsn() + buf_offset as Lsn;

            trace!(
                "reserved {} bytes at lsn {} lid {}",
                inline_buf_len,
                reservation_lsn,
                reservation_offset,
            );

            self.iobufs.bump_max_reserved_lsn(reservation_lsn);

            self.iobufs.encapsulate(
                &*buf,
                destination,
                reservation_lsn,
                over_blob_threshold,
                is_blob_rewrite,
            )?;

            M.log_reservation_success();

            let ptr = if over_blob_threshold {
                DiskPtr::new_blob(reservation_offset, reservation_lsn)
            } else if is_blob_rewrite {
                let blob_ptr = arr_to_u64(&*buf) as BlobPointer;
                DiskPtr::new_blob(reservation_offset, blob_ptr)
            } else {
                DiskPtr::new_inline(reservation_offset)
            };

            return Ok(Reservation {
                idx,
                log: &self,
                buf: destination,
                flushed: false,
                lsn: reservation_lsn,
                ptr,
                is_blob_rewrite,
            });
        }
    }

    /// Called by Reservation on termination (completion or abort).
    /// Handles departure from shared state, and possibly writing
    /// the buffer to stable storage if necessary.
    pub(super) fn exit_reservation(&self, idx: usize) -> Result<()> {
        let iobuf = &self.iobufs.bufs[idx];
        let mut header = iobuf.get_header();

        // Decrement writer count, retrying until successful.
        loop {
            let new_hv = iobuf::decr_writers(header);
            match iobuf.cas_header(header, new_hv) {
                Ok(new) => {
                    header = new;
                    break;
                }
                Err(new) => {
                    // we failed to decr, retry
                    header = new;
                }
            }
        }

        // Succeeded in decrementing writers, if we decremented writn
        // to 0 and it's sealed then we should write it to storage.
        if iobuf::n_writers(header) == 0 && iobuf::is_sealed(header) {
            if let Err(e) = self.config.global_error() {
                self.iobufs.interval_updated.notify_all();
                return Err(e);
            }

            if let Some(ref thread_pool) = self.config.thread_pool {
                trace!("asynchronously writing index {} to log from exit_reservation", idx);
                let iobufs = self.iobufs.clone();
                thread_pool.spawn(move || {
                    if let Err(e) = iobufs.write_to_log(idx) {
                        error!(
                            "hit error while writing segment {}: {:?}",
                            idx, e
                        );
                        iobufs.config.set_global_error(e);
                    }
                });
                Ok(())
            } else {
                trace!("synchronously writing index {} to log from exit_reservation", idx);
                self.iobufs.write_to_log(idx)
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for Log {
    fn drop(&mut self) {
        {
            // this is a hack to let TSAN know
            // that it's safe to destroy our inner
            // members, since it doesn't pick up on
            // the implicit barriers used elsewhere.
            let _ = self.iobufs.buf_mu.lock().unwrap();
        }

        // don't do any more IO if we're crashing
        if self.config.global_error().is_err() {
            return;
        }

        if let Err(e) = iobuf::flush(&self.iobufs) {
            error!("failed to flush from IoBufs::drop: {}", e);
        }

        self.config.file.sync_all().unwrap();

        debug!("IoBufs dropped");
    }
}

/// Represents the kind of message written to the log
#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) enum MessageKind {
    Inline,
    Blob,
    Failed,
    Pad,
    Corrupted,
    BatchManifest,
}

/// All log messages are prepended with this header
#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) struct MessageHeader {
    pub(crate) kind: MessageKind,
    pub(crate) lsn: Lsn,
    pub(crate) len: usize,
    pub(crate) crc32: u32,
}

/// A segment's header contains the new base LSN and a reference
/// to the previous log segment.
#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) struct SegmentHeader {
    pub(crate) lsn: Lsn,
    pub(crate) highest_known_stable_lsn: Lsn,
    pub(crate) ok: bool,
}

#[doc(hidden)]
#[derive(Debug)]
pub enum LogRead {
    Inline(Lsn, Vec<u8>, usize),
    Blob(Lsn, Vec<u8>, BlobPointer),
    Failed(Lsn, usize),
    Pad(Lsn),
    Corrupted(usize),
    DanglingBlob(Lsn, BlobPointer),
    BatchManifest(Lsn),
}

impl LogRead {
    /// Optionally return successfully read inline bytes, or
    /// None if the data was corrupt or this log entry was aborted.
    pub fn inline(self) -> Option<(Lsn, Vec<u8>, usize)> {
        match self {
            LogRead::Inline(lsn, bytes, len) => Some((lsn, bytes, len)),
            _ => None,
        }
    }

    /// Return true if this is an Inline value..
    pub fn is_inline(&self) -> bool {
        match *self {
            LogRead::Inline(_, _, _) => true,
            _ => false,
        }
    }

    /// Optionally return a successfully read pointer to an
    /// blob value, or None if the data was corrupt or
    /// this log entry was aborted.
    pub fn blob(self) -> Option<(Lsn, Vec<u8>, BlobPointer)> {
        match self {
            LogRead::Blob(lsn, buf, ptr) => Some((lsn, buf, ptr)),
            _ => None,
        }
    }

    /// Return true if we read a completed blob write successfully.
    pub fn is_blob(&self) -> bool {
        match self {
            LogRead::Blob(..) => true,
            _ => false,
        }
    }

    /// Return true if we read an aborted flush.
    pub fn is_failed(&self) -> bool {
        match *self {
            LogRead::Failed(_, _) => true,
            _ => false,
        }
    }

    /// Return true if we read a successful Inline or Blob value.
    pub fn is_successful(&self) -> bool {
        match *self {
            LogRead::Inline(_, _, _) | LogRead::Blob(_, _, _) => true,
            _ => false,
        }
    }

    /// Return true if we read a segment pad.
    pub fn is_pad(&self) -> bool {
        match *self {
            LogRead::Pad(_) => true,
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

    /// Return the underlying data read from a log read, if successful.
    pub fn into_data(self) -> Option<Vec<u8>> {
        match self {
            LogRead::Blob(_, buf, _) | LogRead::Inline(_, buf, _) => Some(buf),
            _ => None,
        }
    }
}

// NB we use a lot of xors below to differentiate between zeroed out
// data on disk and an lsn or crc32 of 0

impl From<[u8; MSG_HEADER_LEN]> for MessageHeader {
    fn from(buf: [u8; MSG_HEADER_LEN]) -> MessageHeader {
        let kind = match buf[0] {
            INLINE_FLUSH => MessageKind::Inline,
            BLOB_FLUSH => MessageKind::Blob,
            FAILED_FLUSH => MessageKind::Failed,
            SEGMENT_PAD => MessageKind::Pad,
            BATCH_MANIFEST => MessageKind::BatchManifest,
            _ => MessageKind::Corrupted,
        };

        unsafe {
            let lsn = arr_to_u64(buf.get_unchecked(1..9)) as Lsn;
            let len = assert_usize(arr_to_u32(buf.get_unchecked(9..13)));
            let crc32 = arr_to_u32(buf.get_unchecked(13..)) ^ 0xFFFF_FFFF;

            MessageHeader {
                kind,
                lsn,
                len,
                crc32,
            }
        }
    }
}

impl Into<[u8; MSG_HEADER_LEN]> for MessageHeader {
    fn into(self) -> [u8; MSG_HEADER_LEN] {
        let mut buf = [0u8; MSG_HEADER_LEN];
        buf[0] = match self.kind {
            MessageKind::Inline => INLINE_FLUSH,
            MessageKind::Blob => BLOB_FLUSH,
            MessageKind::Failed => FAILED_FLUSH,
            MessageKind::Pad => SEGMENT_PAD,
            MessageKind::BatchManifest => BATCH_MANIFEST,
            MessageKind::Corrupted => EVIL_BYTE,
        };

        assert!(self.len <= assert_usize(std::u32::MAX));
        let lsn_arr = u64_to_arr(self.lsn as u64);
        let len_arr = u32_to_arr(self.len as u32);
        let crc32_arr = u32_to_arr(self.crc32 ^ 0xFFFF_FFFF);

        unsafe {
            std::ptr::copy_nonoverlapping(
                lsn_arr.as_ptr(),
                buf.as_mut_ptr().add(1),
                std::mem::size_of::<u64>(),
            );
            std::ptr::copy_nonoverlapping(
                len_arr.as_ptr(),
                buf.as_mut_ptr().add(9),
                std::mem::size_of::<u32>(),
            );
            std::ptr::copy_nonoverlapping(
                crc32_arr.as_ptr(),
                buf.as_mut_ptr().add(13),
                std::mem::size_of::<u32>(),
            );
        }

        buf
    }
}

impl From<[u8; SEG_HEADER_LEN]> for SegmentHeader {
    fn from(buf: [u8; SEG_HEADER_LEN]) -> SegmentHeader {
        unsafe {
            let crc32_header =
                arr_to_u32(buf.get_unchecked(0..4)) ^ 0xFFFF_FFFF;

            let xor_lsn = arr_to_u64(buf.get_unchecked(4..12)) as Lsn;
            let lsn = xor_lsn ^ 0x7FFF_FFFF_FFFF_FFFF;

            let xor_highest_stable_lsn =
                arr_to_u64(buf.get_unchecked(12..20)) as Lsn;
            let highest_known_stable_lsn =
                xor_highest_stable_lsn ^ 0x7FFF_FFFF_FFFF_FFFF;

            let crc32_tested = crc32(&buf[4..20]);

            SegmentHeader {
                lsn,
                highest_known_stable_lsn,
                ok: crc32_tested == crc32_header,
            }
        }
    }
}

impl Into<[u8; SEG_HEADER_LEN]> for SegmentHeader {
    fn into(self) -> [u8; SEG_HEADER_LEN] {
        let mut buf = [0u8; SEG_HEADER_LEN];

        let xor_lsn = self.lsn ^ 0x7FFF_FFFF_FFFF_FFFF;
        let xor_highest_stable_lsn =
            self.highest_known_stable_lsn ^ 0x7FFF_FFFF_FFFF_FFFF;
        let lsn_arr = u64_to_arr(xor_lsn as u64);
        let highest_stable_lsn_arr = u64_to_arr(xor_highest_stable_lsn as u64);

        unsafe {
            std::ptr::copy_nonoverlapping(
                lsn_arr.as_ptr(),
                buf.as_mut_ptr().add(4),
                std::mem::size_of::<u64>(),
            );
            std::ptr::copy_nonoverlapping(
                highest_stable_lsn_arr.as_ptr(),
                buf.as_mut_ptr().add(12),
                std::mem::size_of::<u64>(),
            );
        }

        let crc32 = u32_to_arr(crc32(&buf[4..20]) ^ 0xFFFF_FFFF);

        unsafe {
            std::ptr::copy_nonoverlapping(
                crc32.as_ptr(),
                buf.as_mut_ptr(),
                std::mem::size_of::<u32>(),
            );
        }

        buf
    }
}
