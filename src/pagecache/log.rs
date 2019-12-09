use std::sync::Arc;

use super::{
    arr_to_lsn, arr_to_u32, arr_to_u64, bump_atomic_lsn, iobuf, lsn_to_arr,
    read_blob, read_message, u32_to_arr, u64_to_arr, BlobPointer, DiskPtr,
    IoBuf, IoBufs, LogKind, LogOffset, Lsn, MessageKind, Reservation,
    SegmentAccountant, Snapshot, BATCH_MANIFEST_PID, BLOB_INLINE_LEN,
    COUNTER_PID, META_PID, MINIMUM_ITEMS_PER_SEGMENT, MSG_HEADER_LEN,
    SEG_HEADER_LEN,
};

use crate::*;

/// A sequential store which allows users to create
/// reservations placed at known log offsets, used
/// for writing persistent data structures that need
/// to know where to find persisted bits in the future.
#[derive(Debug)]
pub struct Log {
    /// iobufs is the underlying lock-free IO write buffer.
    pub(super) iobufs: Arc<IoBufs>,
    pub(crate) config: RunningConfig,
}

impl Log {
    /// Start the log, open or create the configured file,
    /// and optionally start the periodic buffer flush thread.
    pub fn start(config: RunningConfig, snapshot: &Snapshot) -> Result<Self> {
        let iobufs = Arc::new(IoBufs::start(config.clone(), snapshot)?);

        Ok(Self { iobufs, config })
    }

    /// Starts a log for use without a materializer.
    pub fn start_raw_log(config: RunningConfig) -> Result<Self> {
        assert_eq!(config.segment_mode, super::SegmentMode::Linear);
        let (log_iter, _, _) = super::raw_segment_iter_from(0, &config)?;

        let snapshot =
            super::advance_snapshot(log_iter, Snapshot::default(), &config)?;

        Self::start(config, &snapshot)
    }

    /// Flushes any pending IO buffers to disk to ensure durability.
    /// Returns the number of bytes written during this call.
    pub fn flush(&self) -> Result<usize> {
        iobuf::flush(&self.iobufs)
    }

    /// Return an iterator over the log, starting with
    /// a specified offset.
    pub fn iter_from(&self, lsn: Lsn) -> super::LogIter {
        self.iobufs.iter_from(lsn)
    }

    /// read a buffer from the disk
    pub fn read(&self, pid: PageId, lsn: Lsn, ptr: DiskPtr) -> Result<LogRead> {
        trace!("reading log lsn {} ptr {}", lsn, ptr);

        let _wrote = self.make_stable(lsn)?;

        if ptr.is_inline() {
            let f = &self.config.file;
            read_message(f, ptr.lid(), lsn, &self.config)
        } else {
            // we short-circuit the inline read
            // here because it might not still
            // exist in the inline log.
            let (_, blob_ptr) = ptr.blob();
            read_blob(blob_ptr, &self.config).map(|(kind, buf)| {
                let sz = MSG_HEADER_LEN + BLOB_INLINE_LEN;
                let header = MessageHeader {
                    kind,
                    pid,
                    lsn,
                    crc32: 0,
                    len: u32::try_from(sz).unwrap(),
                };
                LogRead::Blob(header, buf, blob_ptr)
            })
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
    pub(in crate::pagecache) fn with_sa<B, F>(&self, f: F) -> B
    where
        F: FnOnce(&mut SegmentAccountant) -> B,
    {
        self.iobufs.with_sa(f)
    }

    /// Reserve a replacement buffer for a previously written
    /// blob write. This ensures the message header has the
    /// proper blob flag set.
    pub(super) fn rewrite_blob_pointer(
        &self,
        pid: PageId,
        blob_pointer: BlobPointer,
    ) -> Result<Reservation<'_>> {
        let lsn_buf: [u8; std::mem::size_of::<BlobPointer>()] =
            lsn_to_arr(blob_pointer);

        self.reserve_inner(LogKind::Replace, pid, &lsn_buf, true)
    }

    /// Tries to claim a reservation for writing a buffer to a
    /// particular location in stable storge, which may either be
    /// completed or aborted later. Useful for maintaining
    /// linearizability across CAS operations that may need to
    /// persist part of their operation.
    #[allow(unused)]
    pub fn reserve(
        &self,
        log_kind: LogKind,
        pid: PageId,
        raw_buf: &[u8],
    ) -> Result<Reservation<'_>> {
        #[cfg(feature = "compression")]
        let mut compressed: Option<Vec<u8>> = None;
        let mut buf = raw_buf;

        #[cfg(feature = "compression")]
        {
            if self.config.use_compression {
                use zstd::block::compress;

                let _measure = Measure::new(&M.compress);

                let compressed_buf =
                    compress(buf, self.config.compression_factor).unwrap();
                compressed = Some(compressed_buf);

                buf = compressed.as_ref().unwrap();
            }
        }

        self.reserve_inner(log_kind, pid, buf, false)
    }

    fn reserve_inner(
        &self,
        log_kind: LogKind,
        pid: PageId,
        buf: &[u8],
        is_blob_rewrite: bool,
    ) -> Result<Reservation<'_>> {
        let _measure = Measure::new(&M.reserve_lat);

        let total_buf_len = MSG_HEADER_LEN + buf.len();

        #[allow(clippy::cast_precision_loss)]
        M.reserve_sz.measure(total_buf_len as f64);

        let max_buf_size = (self.config.segment_size
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

        let kind = match (pid, log_kind, over_blob_threshold || is_blob_rewrite)
        {
            (COUNTER_PID, LogKind::Replace, false) => MessageKind::Counter,
            (META_PID, LogKind::Replace, true) => MessageKind::BlobMeta,
            (META_PID, LogKind::Replace, false) => MessageKind::InlineMeta,
            (BATCH_MANIFEST_PID, LogKind::Skip, false) => {
                MessageKind::BatchManifest
            }
            (_, LogKind::Free, false) => MessageKind::Free,
            (_, LogKind::Replace, true) => MessageKind::BlobNode,
            (_, LogKind::Replace, false) => MessageKind::InlineNode,
            (_, LogKind::Link, true) => MessageKind::BlobLink,
            (_, LogKind::Link, false) => MessageKind::InlineLink,
            other => panic!(
                "unexpected combination of PageId, \
                 LogKind, and blob status: {:?}",
                other
            ),
        };

        loop {
            M.log_reservation_attempted();

            // don't continue if the system
            // has encountered an issue.
            if let Err(e) = self.config.global_error() {
                let _ = self.iobufs.intervals.lock();
                let _notified = self.iobufs.interval_updated.notify_all();
                return Err(e);
            }

            // load current header value
            let iobuf = self.iobufs.current_iobuf();
            let header = iobuf.get_header();

            // skip if already sealed
            if iobuf::is_sealed(header) {
                // already sealed, start over and hope cur
                // has already been bumped by sealer.
                trace_once!("io buffer already sealed, spinning");

                backoff.snooze();

                continue;
            }

            // try to claim space
            let buf_offset = iobuf::offset(header);
            let prospective_size = buf_offset + inline_buf_len;
            let would_overflow = prospective_size > iobuf.capacity;
            if would_overflow {
                // This buffer is too full to accept our write!
                // Try to seal the buffer, and maybe write it if
                // there are zero writers.
                trace_once!("io buffer too full, spinning");
                iobuf::maybe_seal_and_write_iobuf(
                    &self.iobufs,
                    &iobuf,
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

            let log_offset = iobuf.offset;

            // if we're giving out a reservation,
            // the writer count should be positive
            assert_ne!(iobuf::n_writers(claimed), 0);

            // should never have claimed a sealed buffer
            assert!(!iobuf::is_sealed(claimed));

            let reservation_lsn =
                iobuf.lsn + Lsn::try_from(buf_offset).unwrap();

            // MAX is used to signify unreadiness of
            // the underlying IO buffer, and if it's
            // still set here, the buffer counters
            // used to choose this IO buffer
            // were incremented in a racy way.
            assert_ne!(
                log_offset,
                LogOffset::max_value(),
                "fucked up on iobuf with lsn {}\n{:?}",
                reservation_lsn,
                self
            );

            #[allow(unsafe_code)]
            let out_buf = unsafe { (*iobuf.buf.get()).as_mut_slice() };

            let res_start = buf_offset;
            let res_end = res_start + inline_buf_len;

            let destination = &mut (out_buf)[res_start..res_end];
            let reservation_offset = log_offset + buf_offset as LogOffset;

            trace!(
                "reserved {} bytes at lsn {} lid {}",
                inline_buf_len,
                reservation_lsn,
                reservation_offset,
            );

            bump_atomic_lsn(&self.iobufs.max_reserved_lsn, reservation_lsn);

            self.iobufs.encapsulate(
                &*buf,
                destination,
                kind,
                pid,
                reservation_lsn,
                over_blob_threshold,
            )?;

            M.log_reservation_success();

            let pointer = if over_blob_threshold {
                DiskPtr::new_blob(reservation_offset, reservation_lsn)
            } else if is_blob_rewrite {
                let blob_ptr =
                    BlobPointer::try_from(arr_to_u64(&*buf)).unwrap();
                DiskPtr::new_blob(reservation_offset, blob_ptr)
            } else {
                DiskPtr::new_inline(reservation_offset)
            };

            return Ok(Reservation {
                iobuf,
                log: self,
                buf: destination,
                flushed: false,
                lsn: reservation_lsn,
                pointer,
                is_blob_rewrite,
            });
        }
    }

    /// Called by Reservation on termination (completion or abort).
    /// Handles departure from shared state, and possibly writing
    /// the buffer to stable storage if necessary.
    pub(super) fn exit_reservation(&self, iobuf: &Arc<IoBuf>) -> Result<()> {
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
                let _ = self.iobufs.intervals.lock();
                let _notified = self.iobufs.interval_updated.notify_all();
                return Err(e);
            }

            let lsn = iobuf.lsn;
            trace!(
                "asynchronously writing iobuf with lsn {} \
                 to log from exit_reservation",
                lsn
            );
            let iobufs = self.iobufs.clone();
            let iobuf = iobuf.clone();
            let _result = threadpool::spawn(move || {
                if let Err(e) = iobufs.write_to_log(iobuf) {
                    error!(
                        "hit error while writing iobuf with lsn {}: {:?}",
                        lsn, e
                    );
                    iobufs.config.set_global_error(e);
                }
            });

            #[cfg(test)]
            _result.unwrap();

            Ok(())
        } else {
            Ok(())
        }
    }
}

impl Drop for Log {
    fn drop(&mut self) {
        // don't do any more IO if we're crashing
        if self.config.global_error().is_err() {
            return;
        }

        if let Err(e) = iobuf::flush(&self.iobufs) {
            error!("failed to flush from IoBufs::drop: {}", e);
        }

        if !self.config.temporary {
            self.config.file.sync_all().unwrap();
        }

        debug!("IoBufs dropped");
    }
}

/// All log messages are prepended with this header
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct MessageHeader {
    pub kind: MessageKind,
    pub lsn: Lsn,
    pub pid: PageId,
    pub len: u32,
    pub crc32: u32,
}

/// A segment's header contains the new base LSN and a reference
/// to the previous log segment.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct SegmentHeader {
    pub lsn: Lsn,
    pub max_stable_lsn: Lsn,
    pub ok: bool,
}

/// The result of a read of a log message
#[derive(Debug)]
pub enum LogRead {
    /// Successful read, entirely on-log
    Inline(MessageHeader, Vec<u8>, u32),
    /// Successful read, spilled to its own blob file
    Blob(MessageHeader, Vec<u8>, BlobPointer),
    /// A cancelled message was encountered
    Failed(Lsn, u32),
    /// A padding message used to show that a segment was filled
    Pad(Lsn),
    /// This log message was not readable due to corruption
    Corrupted(u32),
    /// This blob file is no longer available
    DanglingBlob(MessageHeader, BlobPointer),
    /// This data may only be read if at least this future location is stable
    BatchManifest(Lsn),
}

impl LogRead {
    /// Return true if we read a successful Inline or Blob value.
    pub fn is_successful(&self) -> bool {
        match *self {
            LogRead::Inline(..) | LogRead::Blob(..) => true,
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
    fn from(buf: [u8; MSG_HEADER_LEN]) -> Self {
        let kind = MessageKind::from(buf[0]);

        #[allow(unsafe_code)]
        unsafe {
            let page_id = arr_to_u64(buf.get_unchecked(1..9));
            let lsn = arr_to_lsn(buf.get_unchecked(9..17));
            let length = arr_to_u32(buf.get_unchecked(17..21));
            let crc32 = arr_to_u32(buf.get_unchecked(21..)) ^ 0xFFFF_FFFF;

            Self { kind, pid: page_id, lsn, len: length, crc32 }
        }
    }
}

impl Into<[u8; MSG_HEADER_LEN]> for MessageHeader {
    fn into(self) -> [u8; MSG_HEADER_LEN] {
        let mut buf = [0; MSG_HEADER_LEN];
        buf[0] = self.kind.into();

        let pid_arr = u64_to_arr(self.pid);
        let lsn_arr = lsn_to_arr(self.lsn);
        let length_arr = u32_to_arr(self.len);
        let crc32_arr = u32_to_arr(self.crc32 ^ 0xFFFF_FFFF);

        #[allow(unsafe_code)]
        unsafe {
            std::ptr::copy_nonoverlapping(
                pid_arr.as_ptr(),
                buf.as_mut_ptr().add(1),
                std::mem::size_of::<u64>(),
            );
            std::ptr::copy_nonoverlapping(
                lsn_arr.as_ptr(),
                buf.as_mut_ptr().add(9),
                std::mem::size_of::<u64>(),
            );
            std::ptr::copy_nonoverlapping(
                length_arr.as_ptr(),
                buf.as_mut_ptr().add(17),
                std::mem::size_of::<u32>(),
            );
            std::ptr::copy_nonoverlapping(
                crc32_arr.as_ptr(),
                buf.as_mut_ptr().add(21),
                std::mem::size_of::<u32>(),
            );
        }

        buf
    }
}

impl From<[u8; SEG_HEADER_LEN]> for SegmentHeader {
    fn from(buf: [u8; SEG_HEADER_LEN]) -> Self {
        #[allow(unsafe_code)]
        unsafe {
            let crc32_header =
                arr_to_u32(buf.get_unchecked(0..4)) ^ 0xFFFF_FFFF;

            let xor_lsn = arr_to_lsn(buf.get_unchecked(4..12));
            let lsn = xor_lsn ^ 0x7FFF_FFFF_FFFF_FFFF;

            let xor_max_stable_lsn = arr_to_lsn(buf.get_unchecked(12..20));
            let max_stable_lsn = xor_max_stable_lsn ^ 0x7FFF_FFFF_FFFF_FFFF;

            let crc32_tested = crc32(&buf[4..20]);

            let ok = crc32_tested == crc32_header;

            if !ok {
                debug!(
                    "segment with lsn {} had computed crc {}, \
                     but stored crc {}",
                    lsn, crc32_tested, crc32_header
                );
            }

            Self { lsn, max_stable_lsn, ok }
        }
    }
}

impl Into<[u8; SEG_HEADER_LEN]> for SegmentHeader {
    fn into(self) -> [u8; SEG_HEADER_LEN] {
        let mut buf = [0; SEG_HEADER_LEN];

        let xor_lsn = self.lsn ^ 0x7FFF_FFFF_FFFF_FFFF;
        let lsn_arr = lsn_to_arr(xor_lsn);

        let xor_max_stable_lsn = self.max_stable_lsn ^ 0x7FFF_FFFF_FFFF_FFFF;
        let highest_stable_lsn_arr = lsn_to_arr(xor_max_stable_lsn);

        #[allow(unsafe_code)]
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

        #[allow(unsafe_code)]
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
