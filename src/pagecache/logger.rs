use std::fs::File;

use super::{
    arr_to_lsn, arr_to_u32, assert_usize, bump_atomic_lsn, decompress, header,
    iobuf, lsn_to_arr, pread_exact, pread_exact_or_eof, roll_iobuf, u32_to_arr,
    Arc, BasedBuf, DiskPtr, HeapId, IoBuf, IoBufs, LogKind, LogOffset, Lsn,
    MessageKind, Reservation, Serialize, Snapshot, BATCH_MANIFEST_PID,
    COUNTER_PID, MAX_MSG_HEADER_LEN, META_PID, SEG_HEADER_LEN,
};

use crate::*;

/// A sequential store which allows users to create
/// reservations placed at known log offsets, used
/// for writing persistent data structures that need
/// to know where to find persisted bits in the future.
#[derive(Debug)]
pub struct Log {
    /// iobufs is the underlying lock-free IO write buffer.
    pub(crate) iobufs: Arc<IoBufs>,
    pub(crate) config: RunningConfig,
}

impl Log {
    /// Start the log, open or create the configured file,
    /// and optionally start the periodic buffer flush thread.
    pub fn start(config: RunningConfig, snapshot: &Snapshot) -> Result<Self> {
        let iobufs = Arc::new(IoBufs::start(config.clone(), snapshot)?);

        Ok(Self { iobufs, config })
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

    pub(crate) fn roll_iobuf(&self) -> Result<usize> {
        roll_iobuf(&self.iobufs)
    }

    /// read a buffer from the disk
    pub fn read(&self, pid: PageId, lsn: Lsn, ptr: DiskPtr) -> Result<LogRead> {
        trace!("reading log lsn {} ptr {}", lsn, ptr);

        let expected_segment_number = SegmentNumber(
            u64::try_from(lsn).unwrap()
                / u64::try_from(self.config.segment_size).unwrap(),
        );

        iobuf::make_durable(&self.iobufs, lsn)?;

        if ptr.is_inline() {
            let f = &self.config.file;
            read_message(
                &**f,
                ptr.lid().unwrap(),
                expected_segment_number,
                &self.config,
            )
        } else {
            // we short-circuit the inline read
            // here because it might not still
            // exist in the inline log.
            let heap_id = ptr.heap_id().unwrap();
            self.config.heap.read(heap_id, self.config.use_compression).map(
                |(kind, buf)| {
                    let header = MessageHeader {
                        kind,
                        pid,
                        segment_number: expected_segment_number,
                        crc32: 0,
                        len: 0,
                    };
                    LogRead::Heap(header, buf, heap_id, 0)
                },
            )
        }
    }

    /// returns the current stable offset written to disk
    pub fn stable_offset(&self) -> Lsn {
        self.iobufs.stable()
    }

    /// blocks until the specified log sequence number has
    /// been made stable on disk. Returns the number of
    /// bytes written during this call. this is appropriate
    /// as a full consistency-barrier for all data written
    /// up until this point.
    pub fn make_stable(&self, lsn: Lsn) -> Result<usize> {
        iobuf::make_stable(&self.iobufs, lsn)
    }

    /// Reserve a replacement buffer for a previously written
    /// heap write. This allows the tiny pointer in the log
    /// to be migrated to a new segment without copying the
    /// massive slab in the heap that the pointer references.
    pub(super) fn rewrite_heap_pointer(
        &self,
        pid: PageId,
        heap_pointer: HeapId,
        guard: &Guard,
    ) -> Result<Reservation<'_>> {
        let ret = self.reserve_inner(
            LogKind::Replace,
            pid,
            &heap_pointer,
            Some(heap_pointer),
            guard,
        );

        if let Err(e) = &ret {
            self.iobufs.set_global_error(*e);
        }

        ret
    }

    /// Tries to claim a reservation for writing a buffer to a
    /// particular location in stable storge, which may either be
    /// completed or aborted later. Useful for maintaining
    /// linearizability across CAS operations that may need to
    /// persist part of their operation.
    #[allow(unused)]
    pub fn reserve<T: Serialize + Debug>(
        &self,
        log_kind: LogKind,
        pid: PageId,
        item: &T,
        guard: &Guard,
    ) -> Result<Reservation<'_>> {
        #[cfg(feature = "compression")]
        {
            if self.config.use_compression && pid != BATCH_MANIFEST_PID {
                use zstd::block::compress;

                let buf = item.serialize();

                #[cfg(feature = "metrics")]
                let _measure = Measure::new(&M.compress);

                let compressed_buf =
                    compress(&buf, self.config.compression_factor).unwrap();

                let ret = self.reserve_inner(
                    log_kind,
                    pid,
                    &IVec::from(compressed_buf),
                    None,
                    guard,
                );

                if let Err(e) = &ret {
                    self.iobufs.set_global_error(*e);
                }

                return ret;
            }
        }

        let ret = self.reserve_inner(log_kind, pid, item, None, guard);

        if let Err(e) = &ret {
            self.iobufs.set_global_error(*e);
        }

        ret
    }

    fn reserve_inner<T: Serialize + Debug>(
        &self,
        log_kind: LogKind,
        pid: PageId,
        item: &T,
        heap_rewrite: Option<HeapId>,
        _: &Guard,
    ) -> Result<Reservation<'_>> {
        #[cfg(feature = "metrics")]
        let _measure = Measure::new(&M.reserve_lat);

        let serialized_len = item.serialized_size();
        let max_buf_len =
            u64::try_from(MAX_MSG_HEADER_LEN).unwrap() + serialized_len;

        #[cfg(feature = "metrics")]
        M.reserve_sz.measure(max_buf_len);

        let max_buf_size = usize::try_from(super::heap::MIN_SZ * 15 / 16)
            .unwrap()
            .min(self.config.segment_size - SEG_HEADER_LEN);

        let over_heap_threshold =
            max_buf_len > u64::try_from(max_buf_size).unwrap();

        assert!(!(over_heap_threshold && heap_rewrite.is_some()));

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

        let kind = match (
            pid,
            log_kind,
            over_heap_threshold || heap_rewrite.is_some(),
        ) {
            (COUNTER_PID, LogKind::Replace, false) => MessageKind::Counter,
            (META_PID, LogKind::Replace, true) => MessageKind::HeapMeta,
            (META_PID, LogKind::Replace, false) => MessageKind::InlineMeta,
            (BATCH_MANIFEST_PID, LogKind::Skip, false) => {
                MessageKind::BatchManifest
            }
            (_, LogKind::Free, false) => MessageKind::Free,
            (_, LogKind::Replace, true) => MessageKind::HeapNode,
            (_, LogKind::Replace, false) => MessageKind::InlineNode,
            (_, LogKind::Link, true) => MessageKind::HeapLink,
            (_, LogKind::Link, false) => MessageKind::InlineLink,
            other => unreachable!(
                "unexpected combination of PageId, \
                 LogKind, and heap status: {:?}",
                other
            ),
        };

        #[cfg(feature = "metrics")]
        match kind {
            MessageKind::HeapLink | MessageKind::HeapNode => {
                M.bytes_written_heap_item.fetch_add(
                    usize::try_from(serialized_len).unwrap(),
                    Relaxed,
                );
                M.bytes_written_heap_ptr.fetch_add(16, Relaxed);
            }
            MessageKind::InlineNode => {
                M.bytes_written_replace.fetch_add(
                    usize::try_from(serialized_len).unwrap(),
                    Relaxed,
                );
            }
            MessageKind::InlineLink => {
                M.bytes_written_link.fetch_add(
                    usize::try_from(serialized_len).unwrap(),
                    Relaxed,
                );
            }
            _ => {
                M.bytes_written_other.fetch_add(
                    usize::try_from(serialized_len).unwrap(),
                    Relaxed,
                );
            }
        }

        loop {
            #[cfg(feature = "metrics")]
            M.log_reservation_attempted();

            // don't continue if the system
            // has encountered an issue.
            if let Err(e) = self.config.global_error() {
                let intervals = self.iobufs.intervals.lock();

                // having held the mutex makes this linearized
                // with the notify below.
                drop(intervals);

                let _notified = self.iobufs.interval_updated.notify_all();
                return Err(e);
            }

            // load current header value
            let iobuf = self.iobufs.current_iobuf();
            let header = iobuf.get_header();
            let buf_offset = header::offset(header);
            let reservation_lsn =
                iobuf.lsn + Lsn::try_from(buf_offset).unwrap();

            // skip if already sealed
            if header::is_sealed(header) {
                // already sealed, start over and hope cur
                // has already been bumped by sealer.
                trace_once!("io buffer already sealed, spinning");

                backoff.spin();

                continue;
            }

            // figure out how big the header + buf will be.
            // this is variable because of varints used
            // in the header.
            let message_header = MessageHeader {
                crc32: 0,
                kind,
                segment_number: SegmentNumber(
                    u64::try_from(iobuf.lsn).unwrap()
                        / u64::try_from(self.config.segment_size).unwrap(),
                ),
                pid,
                len: if over_heap_threshold {
                    // a HeapId is always 16 bytes
                    16
                } else {
                    serialized_len
                },
            };

            let inline_buf_len = if over_heap_threshold {
                usize::try_from(
                    // a HeapId is always 16 bytes
                    message_header.serialized_size() + 16,
                )
                .unwrap()
            } else {
                usize::try_from(
                    message_header.serialized_size() + serialized_len,
                )
                .unwrap()
            };

            trace!(
                "reserving buf of len {} for pid {} with kind {:?}",
                inline_buf_len,
                pid,
                kind
            );

            // try to claim space
            let prospective_size = buf_offset + inline_buf_len;
            // we don't reserve anything if we're within the last
            // MAX_MSG_HEADER_LEN bytes of the buffer. during
            // recovery, we assume that nothing can begin here,
            // because headers are dynamically sized.
            let red_zone = iobuf.capacity - buf_offset < MAX_MSG_HEADER_LEN;
            let would_overflow = red_zone || prospective_size > iobuf.capacity;
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
            let bumped_offset = header::bump_offset(header, inline_buf_len);

            // check for maxed out IO buffer writers
            if header::n_writers(bumped_offset) == header::MAX_WRITERS {
                trace_once!(
                    "spinning because our buffer has {} writers already",
                    header::MAX_WRITERS
                );
                backoff.spin();
                continue;
            }

            let claimed = header::incr_writers(bumped_offset);

            if iobuf.cas_header(header, claimed).is_err() {
                // CAS failed, start over
                trace_once!("CAS failed while claiming buffer slot, spinning");
                backoff.spin();
                continue;
            }

            let log_offset = iobuf.offset;

            // if we're giving out a reservation,
            // the writer count should be positive
            assert_ne!(header::n_writers(claimed), 0);

            // should never have claimed a sealed buffer
            assert!(!header::is_sealed(claimed));

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

            let destination = iobuf.get_mut_range(buf_offset, inline_buf_len);
            let reservation_lid = log_offset + buf_offset as LogOffset;

            trace!(
                "reserved {} bytes at lsn {} lid {}",
                inline_buf_len,
                reservation_lsn,
                reservation_lid,
            );

            bump_atomic_lsn(
                &self.iobufs.max_reserved_lsn,
                reservation_lsn + inline_buf_len as Lsn - 1,
            );

            let (heap_reservation, heap_id) = if over_heap_threshold {
                let heap_reservation = self
                    .config
                    .heap
                    .reserve(serialized_len + 13, reservation_lsn);
                let heap_id = heap_reservation.heap_id;
                (Some(heap_reservation), Some(heap_id))
            } else {
                (None, None)
            };

            self.iobufs.encapsulate(
                item,
                message_header,
                destination,
                heap_reservation,
            )?;

            #[cfg(feature = "metrics")]
            M.log_reservation_success();

            let pointer = if let Some(heap_id) = heap_id {
                DiskPtr::new_heap_item(reservation_lid, heap_id)
            } else if let Some(heap_id) = heap_rewrite {
                DiskPtr::new_heap_item(reservation_lid, heap_id)
            } else {
                DiskPtr::new_inline(reservation_lid)
            };

            return Ok(Reservation {
                iobuf,
                log: self,
                buf: destination,
                flushed: false,
                lsn: reservation_lsn,
                pointer,
                is_heap_item_rewrite: heap_rewrite.is_some(),
                header_len: usize::try_from(message_header.serialized_size())
                    .unwrap(),
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
            let new_hv = header::decr_writers(header);
            if let Err(current) = iobuf.cas_header(header, new_hv) {
                // we failed to decr, retry
                header = current;
            } else {
                // success
                header = new_hv;
                break;
            }
        }

        // Succeeded in decrementing writers, if we decremented writn
        // to 0 and it's sealed then we should write it to storage.
        if header::n_writers(header) == 0 && header::is_sealed(header) {
            if let Err(e) = self.config.global_error() {
                let intervals = self.iobufs.intervals.lock();

                // having held the mutex makes this linearized
                // with the notify below.
                drop(intervals);

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
            threadpool::write_to_log(iobuf, iobufs);

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
    pub crc32: u32,
    pub kind: MessageKind,
    pub segment_number: SegmentNumber,
    pub pid: PageId,
    pub len: u64,
}

/// A number representing a segment number.
#[derive(Copy, Clone, PartialEq, Debug)]
#[repr(transparent)]
pub struct SegmentNumber(pub u64);

impl std::ops::Deref for SegmentNumber {
    type Target = u64;

    fn deref(&self) -> &u64 {
        &self.0
    }
}

/// A segment's header contains the new base LSN and a reference
/// to the previous log segment.
#[derive(Debug, Copy, Clone)]
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
    /// Successful read, spilled to a slot in the heap
    Heap(MessageHeader, Vec<u8>, HeapId, u32),
    /// A cancelled message was encountered
    Canceled(u32),
    /// A padding message used to show that a segment was filled
    Cap(SegmentNumber),
    /// This log message was not readable due to corruption
    Corrupted,
    /// This heap slot has been replaced
    DanglingHeap(MessageHeader, HeapId, u32),
    /// This data may only be read if at least this future location is stable
    BatchManifest(Lsn, u32),
}

impl LogRead {
    /// Return true if we read a successful Inline or Heap value.
    pub const fn is_successful(&self) -> bool {
        matches!(self, LogRead::Inline(..) | LogRead::Heap(..))
    }

    /// Return the underlying data read from a log read, if successful.
    pub fn into_data(self) -> Option<Vec<u8>> {
        match self {
            LogRead::Heap(_, buf, _, _) | LogRead::Inline(_, buf, _) => {
                Some(buf)
            }
            _ => None,
        }
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

impl From<SegmentHeader> for [u8; SEG_HEADER_LEN] {
    fn from(header: SegmentHeader) -> [u8; SEG_HEADER_LEN] {
        let mut buf = [0; SEG_HEADER_LEN];

        let xor_lsn = header.lsn ^ 0x7FFF_FFFF_FFFF_FFFF;
        let lsn_arr = lsn_to_arr(xor_lsn);

        let xor_max_stable_lsn = header.max_stable_lsn ^ 0x7FFF_FFFF_FFFF_FFFF;
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

pub(crate) fn read_segment_header(
    file: &File,
    lid: LogOffset,
) -> Result<SegmentHeader> {
    trace!("reading segment header at {}", lid);

    let mut seg_header_buf = [0; SEG_HEADER_LEN];
    pread_exact(file, &mut seg_header_buf, lid)?;
    let segment_header = SegmentHeader::from(seg_header_buf);

    if segment_header.lsn < Lsn::try_from(lid).unwrap() {
        debug!(
            "segment had lsn {} but we expected something \
             greater, as the base lid is {}",
            segment_header.lsn, lid
        );
    }

    Ok(segment_header)
}

pub(crate) trait ReadAt {
    fn pread_exact(&self, dst: &mut [u8], at: u64) -> Result<()>;

    fn pread_exact_or_eof(&self, dst: &mut [u8], at: u64) -> Result<usize>;
}

impl ReadAt for File {
    fn pread_exact(&self, dst: &mut [u8], at: u64) -> Result<()> {
        pread_exact(self, dst, at)
    }

    fn pread_exact_or_eof(&self, dst: &mut [u8], at: u64) -> Result<usize> {
        pread_exact_or_eof(self, dst, at)
    }
}

impl ReadAt for BasedBuf {
    fn pread_exact(&self, dst: &mut [u8], mut at: u64) -> Result<()> {
        if at < self.offset
            || u64::try_from(dst.len()).unwrap() + at
                > u64::try_from(self.buf.len()).unwrap() + self.offset
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to fill buffer",
            )
            .into());
        }
        at -= self.offset;
        let at_usize = usize::try_from(at).unwrap();
        let to_usize = at_usize + dst.len();
        dst.copy_from_slice(self.buf[at_usize..to_usize].as_ref());
        Ok(())
    }

    fn pread_exact_or_eof(&self, dst: &mut [u8], mut at: u64) -> Result<usize> {
        if at < self.offset
            || u64::try_from(self.buf.len()).unwrap() < at - self.offset
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to fill buffer",
            )
            .into());
        }
        at -= self.offset;

        let at_usize = usize::try_from(at).unwrap();

        let len = std::cmp::min(dst.len(), self.buf.len() - at_usize);

        let start = at_usize;
        let end = start + len;
        dst[..len].copy_from_slice(self.buf[start..end].as_ref());
        Ok(len)
    }
}

/// read a buffer from the disk
pub(crate) fn read_message<R: ReadAt>(
    file: &R,
    lid: LogOffset,
    expected_segment_number: SegmentNumber,
    config: &RunningConfig,
) -> Result<LogRead> {
    #[cfg(feature = "metrics")]
    let _measure = Measure::new(&M.read);
    let segment_len = config.segment_size;
    let seg_start = lid / segment_len as LogOffset * segment_len as LogOffset;
    trace!("reading message from segment: {} at lid: {}", seg_start, lid);
    assert!(seg_start + SEG_HEADER_LEN as LogOffset <= lid);
    assert!(
        (seg_start + segment_len as LogOffset) - lid
            >= MAX_MSG_HEADER_LEN as LogOffset,
        "tried to read a message from the red zone"
    );

    let msg_header_buf = &mut [0; 128];
    let _read_bytes = file.pread_exact_or_eof(msg_header_buf, lid)?;
    let header_cursor = &mut msg_header_buf.as_ref();
    let len_before = header_cursor.len();
    let header = MessageHeader::deserialize(header_cursor)?;
    let len_after = header_cursor.len();
    let message_offset = len_before - len_after;
    trace!(
        "read message header at lid {} with header length {}: {:?}",
        lid,
        message_offset,
        header
    );

    let ceiling = seg_start + segment_len as LogOffset;

    assert!(lid + message_offset as LogOffset <= ceiling);

    let max_possible_len =
        assert_usize(ceiling - lid - message_offset as LogOffset);

    if header.len > max_possible_len as u64 {
        trace!(
            "read a corrupted message with impossibly long length {:?}",
            header
        );
        return Ok(LogRead::Corrupted);
    }

    let header_len = usize::try_from(header.len).unwrap();

    if header.kind == MessageKind::Corrupted {
        trace!(
            "read a corrupted message with Corrupted MessageKind: {:?}",
            header
        );
        return Ok(LogRead::Corrupted);
    }

    // perform crc check on everything that isn't Corrupted
    let mut buf = vec![0; header_len];

    if header_len > len_after {
        // we have to read more data from disk
        file.pread_exact(&mut buf, lid + message_offset as LogOffset)?;
    } else {
        // we already read this data in the initial read
        buf.copy_from_slice(header_cursor[..header_len].as_ref());
    }

    let crc32 = calculate_message_crc32(
        msg_header_buf[..message_offset].as_ref(),
        &buf,
    );

    if crc32 != header.crc32 {
        trace!(
            "read a message with a bad checksum with header {:?} msg len: {} expected: {} actual: {}",
            header,
            header_len,
            header.crc32,
            crc32
        );
        return Ok(LogRead::Corrupted);
    }

    let inline_len = u32::try_from(message_offset).unwrap()
        + u32::try_from(header.len).unwrap();

    if header.segment_number != expected_segment_number {
        debug!(
            "header {:?} does not contain expected segment_number {:?}",
            header, expected_segment_number
        );
        return Ok(LogRead::Corrupted);
    }

    match header.kind {
        MessageKind::Canceled => {
            trace!("read failed of len {}", header.len);
            Ok(LogRead::Canceled(inline_len))
        }
        MessageKind::Cap => {
            trace!("read pad in segment number {:?}", header.segment_number);
            Ok(LogRead::Cap(header.segment_number))
        }
        MessageKind::HeapLink
        | MessageKind::HeapNode
        | MessageKind::HeapMeta => {
            assert_eq!(buf.len(), 16);
            let heap_id = HeapId::deserialize(&mut &buf[..]).unwrap();

            match config.heap.read(heap_id, config.use_compression) {
                Ok((kind, buf)) => {
                    assert_eq!(header.kind, kind);
                    trace!(
                        "read a successful heap message for heap {:?} in segment number {:?}",
                        heap_id,
                        header.segment_number,
                    );

                    Ok(LogRead::Heap(header, buf, heap_id, inline_len))
                }
                Err(e) => {
                    debug!("failed to read heap: {:?}", e);
                    Ok(LogRead::DanglingHeap(header, heap_id, inline_len))
                }
            }
        }
        MessageKind::InlineLink
        | MessageKind::InlineNode
        | MessageKind::InlineMeta
        | MessageKind::Free
        | MessageKind::Counter => {
            trace!("read a successful inline message");
            let buf =
                if config.use_compression { decompress(buf) } else { buf };

            Ok(LogRead::Inline(header, buf, inline_len))
        }
        MessageKind::BatchManifest => {
            assert_eq!(buf.len(), std::mem::size_of::<Lsn>());
            let max_lsn = arr_to_lsn(&buf);
            Ok(LogRead::BatchManifest(max_lsn, inline_len))
        }
        MessageKind::Corrupted => unreachable!(
            "corrupted should have been handled \
             before reading message length above"
        ),
    }
}
