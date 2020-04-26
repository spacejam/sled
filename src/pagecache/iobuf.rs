use std::{
    alloc::{alloc, dealloc, Layout},
    cell::UnsafeCell,
    sync::atomic::{AtomicBool, AtomicPtr},
};

use crate::{pagecache::*, *};

// This is the most writers in a single IO buffer
// that we have space to accommodate in the counter
// for writers in the IO buffer header.
pub(in crate::pagecache) const MAX_WRITERS: Header = 127;

pub(in crate::pagecache) type Header = u64;

macro_rules! io_fail {
    ($self:expr, $e:expr) => {
        #[cfg(feature = "failpoints")]
        {
            if crate::fail::is_active($e) {
                $self.config.set_global_error(Error::FailPoint);
                // wake up any waiting threads so they don't stall forever
                let _mu = $self.intervals.lock();

                // having held the mutex makes this linearized
                // with the notify below.
                drop(_mu);

                let _notified = $self.interval_updated.notify_all();
                return Err(Error::FailPoint);
            }
        };
    };
}

struct AlignedBuf(*mut u8, usize);

#[allow(unsafe_code)]
unsafe impl Send for AlignedBuf {}

#[allow(unsafe_code)]
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    fn new(len: usize) -> AlignedBuf {
        let layout = Layout::from_size_align(len, 8192).unwrap();
        let ptr = unsafe { alloc(layout) };

        assert!(!ptr.is_null(), "failed to allocate critical IO buffer");

        AlignedBuf(ptr, len)
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.1, 8192).unwrap();
        unsafe {
            dealloc(self.0, layout);
        }
    }
}

pub(crate) struct IoBuf {
    buf: Arc<UnsafeCell<AlignedBuf>>,
    header: CachePadded<AtomicU64>,
    base: usize,
    pub offset: LogOffset,
    pub lsn: Lsn,
    pub capacity: usize,
    maxed: AtomicBool,
    linearizer: Mutex<()>,
    stored_max_stable_lsn: Lsn,
}

#[allow(unsafe_code)]
unsafe impl Sync for IoBuf {}

#[allow(unsafe_code)]
unsafe impl Send for IoBuf {}

impl IoBuf {
    /// # Safety
    ///
    /// This operation provides access to a mutable buffer of
    /// uninitialized memory. For this to be correct, we must
    /// ensure that:
    /// 1. overlapping mutable slices are never created.
    /// 2. a read to any subslice of this slice only happens
    ///    after a write has initialized that memory
    ///
    /// It is intended that the log reservation code guarantees
    /// that no two `Reservation` objects will hold overlapping
    /// mutable slices to our io buffer.
    ///
    /// It is intended that the `write_to_log` function only
    /// tries to write initialized bytes to the underlying storage.
    ///
    /// It is intended that the `write_to_log` function will
    /// initialize any yet-to-be-initialized bytes before writing
    /// the buffer to storage. #1040 added logic that was intended
    /// to meet this requirement.
    ///
    /// The safety of this method was discussed in #1044.
    pub(crate) fn get_mut_range(
        &self,
        at: usize,
        len: usize,
    ) -> &'static mut [u8] {
        let buf_ptr = self.buf.get();

        unsafe {
            assert!((*buf_ptr).1 >= at + len);
            std::slice::from_raw_parts_mut(
                (*buf_ptr).0.add(self.base + at),
                len,
            )
        }
    }

    // use this for operations on an `IoBuf` that must be
    // linearized together, and can't fit in the header!
    pub(crate) fn linearized<F, B>(&self, f: F) -> B
    where
        F: FnOnce() -> B,
    {
        let _l = self.linearizer.lock();
        f()
    }

    // This is called upon the initialization of a fresh segment.
    // We write a new segment header to the beginning of the buffer
    // for assistance during recovery. The caller is responsible
    // for ensuring that the IoBuf's capacity has been set properly.
    fn store_segment_header(
        &mut self,
        last: Header,
        lsn: Lsn,
        max_stable_lsn: Lsn,
    ) {
        debug!("storing lsn {} in beginning of buffer", lsn);
        assert!(self.capacity >= SEG_HEADER_LEN);

        self.stored_max_stable_lsn = max_stable_lsn;

        self.lsn = lsn;

        let header = SegmentHeader { lsn, max_stable_lsn, ok: true };
        let header_bytes: [u8; SEG_HEADER_LEN] = header.into();

        #[allow(unsafe_code)]
        unsafe {
            std::ptr::copy_nonoverlapping(
                header_bytes.as_ptr(),
                (*self.buf.get()).0,
                SEG_HEADER_LEN,
            );
        }

        // ensure writes to the buffer land after our header.
        let last_salt = salt(last);
        let new_salt = bump_salt(last_salt);
        let bumped = bump_offset(new_salt, SEG_HEADER_LEN);
        self.set_header(bumped);
    }

    pub(crate) fn set_maxed(&self, maxed: bool) {
        debug_delay();
        self.maxed.store(maxed, SeqCst);
    }

    pub(crate) fn get_maxed(&self) -> bool {
        debug_delay();
        self.maxed.load(SeqCst)
    }

    pub(crate) fn get_header(&self) -> Header {
        debug_delay();
        self.header.load(SeqCst)
    }

    pub(crate) fn set_header(&self, new: Header) {
        debug_delay();
        self.header.store(new, SeqCst);
    }

    pub(crate) fn cas_header(
        &self,
        old: Header,
        new: Header,
    ) -> std::result::Result<Header, Header> {
        debug_delay();
        let res = self.header.compare_and_swap(old, new, SeqCst);
        if res == old {
            Ok(new)
        } else {
            Err(res)
        }
    }
}

pub(crate) struct IoBufs {
    pub config: RunningConfig,

    // A pointer to the current IoBuf. This relies on crossbeam-epoch
    // for garbage collection when it gets swapped out, to ensure that
    // no witnessing threads experience use-after-free.
    // mutated from the maybe_seal_and_write_iobuf method.
    // finally dropped in the Drop impl, without using crossbeam-epoch,
    // because if this drops, all witnessing threads should be done.
    pub iobuf: AtomicPtr<IoBuf>,

    // Pending intervals that have been written to stable storage, but may be
    // higher than the current value of `stable` due to interesting thread
    // interleavings.
    pub intervals: Mutex<Vec<(Lsn, Lsn)>>,
    pub interval_updated: Condvar,

    // The highest CONTIGUOUS log sequence number that has been written to
    // stable storage. This may be lower than the length of the underlying
    // file, and there may be buffers that have been written out-of-order
    // to stable storage due to interesting thread interleavings.
    pub stable_lsn: AtomicLsn,
    pub max_reserved_lsn: AtomicLsn,
    pub max_header_stable_lsn: Arc<AtomicLsn>,
    pub segment_accountant: Mutex<SegmentAccountant>,
    pub segment_cleaner: SegmentCleaner,
    deferred_segment_ops: stack::Stack<SegmentOp>,
    #[cfg(feature = "io_uring")]
    pub submission_mutex: Mutex<()>,
    #[cfg(feature = "io_uring")]
    pub io_uring: rio::Rio,
}

impl Drop for IoBufs {
    fn drop(&mut self) {
        let ptr = self.iobuf.swap(std::ptr::null_mut(), SeqCst);
        assert!(!ptr.is_null());
        unsafe {
            Arc::from_raw(ptr);
        }
    }
}

/// `IoBufs` is a set of lock-free buffers for coordinating
/// writes to underlying storage.
impl IoBufs {
    pub fn start(config: RunningConfig, snapshot: &Snapshot) -> Result<IoBufs> {
        // open file for writing
        let file = &config.file;

        let segment_size = config.segment_size;

        let snapshot_last_lsn = snapshot.last_lsn;
        let snapshot_last_lid = snapshot.last_lid;
        let snapshot_max_header_stable_lsn = snapshot.max_header_stable_lsn;

        let segment_cleaner = SegmentCleaner::default();

        let mut segment_accountant: SegmentAccountant =
            SegmentAccountant::start(
                config.clone(),
                snapshot,
                segment_cleaner.clone(),
            )?;

        let (next_lsn, next_lid) =
            if snapshot_last_lsn % segment_size as Lsn == 0 {
                (snapshot_last_lsn, snapshot_last_lid)
            } else {
                let width = match read_message(
                    &**file,
                    snapshot_last_lid,
                    SegmentNumber(
                        u64::try_from(snapshot_last_lsn).unwrap()
                            / u64::try_from(config.segment_size).unwrap(),
                    ),
                    &config,
                ) {
                    Ok(LogRead::Canceled(inline_len))
                    | Ok(LogRead::Inline(_, _, inline_len)) => inline_len,
                    Ok(LogRead::Blob(_header, _buf, _blob_ptr, inline_len)) => {
                        inline_len
                    }
                    other => {
                        // we can overwrite this non-flush
                        debug!(
                            "got non-flush tip while recovering at {}: {:?}",
                            snapshot_last_lid, other
                        );
                        0
                    }
                };

                (
                    snapshot_last_lsn + Lsn::from(width),
                    snapshot_last_lid + LogOffset::from(width),
                )
            };

        trace!(
            "starting IoBufs with next_lsn: {} \
             next_lid: {}",
            next_lsn,
            next_lid
        );

        // we want stable to begin at -1 if the 0th byte
        // of our file has not yet been written.
        let stable = next_lsn - 1;

        let iobuf = if next_lsn % config.segment_size as Lsn == 0 {
            // allocate new segment for data

            if next_lsn == 0 {
                assert_eq!(next_lid, 0);
            }
            let lid = segment_accountant.next(next_lsn)?;
            if next_lsn == 0 {
                assert_eq!(0, lid);
            }

            debug!(
                "starting log at clean offset {}, recovered lsn {}",
                next_lid, next_lsn
            );

            let mut iobuf = IoBuf {
                buf: Arc::new(UnsafeCell::new(AlignedBuf::new(segment_size))),
                header: CachePadded::new(AtomicU64::new(0)),
                base: 0,
                offset: lid,
                lsn: 0,
                capacity: segment_size,
                maxed: AtomicBool::new(false),
                linearizer: Mutex::new(()),
                stored_max_stable_lsn: -1,
            };

            iobuf.store_segment_header(0, next_lsn, stable);

            iobuf
        } else {
            // the tip offset is not completely full yet, reuse it
            let base = assert_usize(next_lid % segment_size as LogOffset);

            debug!(
                "starting log at split offset {}, recovered lsn {}",
                next_lid, next_lsn
            );

            IoBuf {
                buf: Arc::new(UnsafeCell::new(AlignedBuf::new(segment_size))),
                header: CachePadded::new(AtomicU64::new(0)),
                base,
                offset: next_lid,
                lsn: next_lsn,
                capacity: segment_size - base,
                maxed: AtomicBool::new(false),
                linearizer: Mutex::new(()),
                stored_max_stable_lsn: -1,
            }
        };

        // remove all blob files larger than our stable offset
        gc_blobs(&config, stable)?;

        Ok(IoBufs {
            config,

            iobuf: AtomicPtr::new(Arc::into_raw(Arc::new(iobuf)) as *mut IoBuf),

            intervals: Mutex::new(vec![]),
            interval_updated: Condvar::new(),

            stable_lsn: AtomicLsn::new(stable),
            max_reserved_lsn: AtomicLsn::new(stable),
            max_header_stable_lsn: Arc::new(AtomicLsn::new(
                snapshot_max_header_stable_lsn,
            )),
            segment_accountant: Mutex::new(segment_accountant),
            segment_cleaner,
            deferred_segment_ops: stack::Stack::default(),
            #[cfg(feature = "io_uring")]
            submission_mutex: Mutex::new(()),
            #[cfg(feature = "io_uring")]
            io_uring: rio::new()?,
        })
    }

    pub(in crate::pagecache) fn sa_mark_peg(
        &self,
        peg_start_lsn: Lsn,
        peg_end_lsn: Lsn,
        guard: &Guard,
    ) {
        let op = SegmentOp::Peg { peg_start_lsn, peg_end_lsn };
        self.deferred_segment_ops.push(op, guard);
    }

    pub(in crate::pagecache) fn sa_mark_link(
        &self,
        pid: PageId,
        cache_info: CacheInfo,
        guard: &Guard,
    ) {
        let op = SegmentOp::Link { pid, cache_info };
        self.deferred_segment_ops.push(op, guard);
    }

    pub(in crate::pagecache) fn sa_mark_replace(
        &self,
        pid: PageId,
        lsn: Lsn,
        old_cache_infos: &StackVec,
        new_cache_info: CacheInfo,
        guard: &Guard,
    ) -> Result<()> {
        debug_delay();
        if let Some(mut sa) = self.segment_accountant.try_lock() {
            let start = clock();
            sa.mark_replace(pid, lsn, old_cache_infos, new_cache_info)?;
            for op in self.deferred_segment_ops.take_iter(guard) {
                sa.apply_op(op)?;
            }
            M.accountant_hold.measure(clock() - start);
        } else {
            let op = SegmentOp::Replace {
                pid,
                lsn,
                old_cache_infos: *old_cache_infos,
                new_cache_info,
            };
            self.deferred_segment_ops.push(op, guard);
        }
        Ok(())
    }

    pub(in crate::pagecache) fn sa_stabilize(
        &self,
        lsn: Lsn,
        guard: &Guard,
    ) -> Result<()> {
        self.with_sa(|sa| {
            for op in self.deferred_segment_ops.take_iter(guard) {
                sa.apply_op(op)?;
            }
            sa.stabilize(lsn)?;
            Ok(())
        })
    }

    /// `SegmentAccountant` access for coordination with the `PageCache`
    pub(in crate::pagecache) fn with_sa<B, F>(&self, f: F) -> B
    where
        F: FnOnce(&mut SegmentAccountant) -> B,
    {
        let start = clock();

        debug_delay();
        let mut sa = self.segment_accountant.lock();

        let locked_at = clock();

        M.accountant_lock.measure(locked_at - start);

        let ret = f(&mut sa);

        drop(sa);

        M.accountant_hold.measure(clock() - locked_at);

        ret
    }

    /// Return an iterator over the log, starting with
    /// a specified offset.
    pub(crate) fn iter_from(&self, lsn: Lsn) -> LogIter {
        trace!("iterating from lsn {}", lsn);
        let segment_size = self.config.segment_size;
        let segment_base_lsn = lsn / segment_size as Lsn * segment_size as Lsn;
        let min_lsn = segment_base_lsn + SEG_HEADER_LEN as Lsn;

        // corrected_lsn accounts for the segment header length
        let corrected_lsn = std::cmp::max(lsn, min_lsn);

        let segment_iter =
            self.with_sa(|sa| sa.segment_snapshot_iter_from(corrected_lsn));

        LogIter {
            config: self.config.clone(),
            max_lsn: self.stable(),
            cur_lsn: corrected_lsn,
            segment_base: None,
            segment_iter,
        }
    }

    /// Returns the last stable offset in storage.
    pub(in crate::pagecache) fn stable(&self) -> Lsn {
        debug_delay();
        self.stable_lsn.load(SeqCst)
    }

    // Adds a header to the front of the buffer
    #[allow(clippy::mut_mut)]
    pub(crate) fn encapsulate<T: Serialize + Debug>(
        &self,
        item: &T,
        header: MessageHeader,
        mut out_buf: &mut [u8],
        blob_id: Option<Lsn>,
    ) -> Result<()> {
        // we create this double ref to allow scooting
        // the slice forward without doing anything
        // to the argument
        let out_buf_ref: &mut &mut [u8] = &mut out_buf;
        {
            let _ = Measure::new(&M.serialize);
            header.serialize_into(out_buf_ref);
        }

        if let Some(blob_id) = blob_id {
            // write blob to file
            io_fail!(self, "blob blob write");
            write_blob(&self.config, header.kind, blob_id, item)?;

            let _ = Measure::new(&M.serialize);
            blob_id.serialize_into(out_buf_ref);
        } else {
            let _ = Measure::new(&M.serialize);
            item.serialize_into(out_buf_ref);
        };

        assert_eq!(
            out_buf_ref.len(),
            0,
            "trying to serialize header {:?} \
             and item {:?} but there were \
             buffer leftovers at the end",
            header,
            item
        );

        Ok(())
    }

    // Write an IO buffer's data to stable storage and set up the
    // next IO buffer for writing.
    pub(crate) fn write_to_log(&self, iobuf: &IoBuf) -> Result<()> {
        let _measure = Measure::new(&M.write_to_log);
        let header = iobuf.get_header();
        let log_offset = iobuf.offset;
        let base_lsn = iobuf.lsn;
        let capacity = iobuf.capacity;

        let segment_size = self.config.segment_size;

        assert_eq!(
            Lsn::try_from(log_offset % segment_size as LogOffset).unwrap(),
            base_lsn % segment_size as Lsn
        );

        assert_ne!(
            log_offset,
            LogOffset::max_value(),
            "created reservation for uninitialized slot",
        );

        assert!(is_sealed(header));

        let bytes_to_write = offset(header);

        trace!(
            "write_to_log log_offset {} lsn {} len {}",
            log_offset,
            base_lsn,
            bytes_to_write
        );

        let maxed = iobuf.linearized(|| iobuf.get_maxed());
        let unused_space = capacity - bytes_to_write;
        let should_pad = maxed && unused_space >= MAX_MSG_HEADER_LEN;

        // a pad is a null message written to the end of a buffer
        // to signify that nothing else will be written into it
        if should_pad {
            let pad_len = unused_space - MAX_MSG_HEADER_LEN;
            let data = iobuf.get_mut_range(bytes_to_write, unused_space);

            let segment_number = SegmentNumber(
                u64::try_from(base_lsn).unwrap()
                    / u64::try_from(self.config.segment_size).unwrap(),
            );

            let header = MessageHeader {
                kind: MessageKind::Cap,
                pid: PageId::max_value(),
                segment_number,
                len: u64::try_from(pad_len).unwrap(),
                crc32: 0,
            };

            let header_bytes = header.serialize();

            // initialize the remainder of this buffer (only pad_len of this will be part of the Cap message)
            let padding_bytes = vec![
                MessageKind::Corrupted.into();
                unused_space - header_bytes.len()
            ];

            #[allow(unsafe_code)]
            unsafe {
                std::ptr::copy_nonoverlapping(
                    header_bytes.as_ptr(),
                    data.as_mut_ptr(),
                    header_bytes.len(),
                );
                std::ptr::copy_nonoverlapping(
                    padding_bytes.as_ptr(),
                    data.as_mut_ptr().add(header_bytes.len()),
                    padding_bytes.len(),
                );
            }

            // this as to stay aligned with the hashing
            let crc32_arr = u32_to_arr(calculate_message_crc32(
                &header_bytes,
                &padding_bytes[..pad_len],
            ));

            #[allow(unsafe_code)]
            unsafe {
                std::ptr::copy_nonoverlapping(
                    crc32_arr.as_ptr(),
                    // the crc32 is the first part of the buffer
                    data.as_mut_ptr(),
                    std::mem::size_of::<u32>(),
                );
            }
        } else if maxed {
            // initialize the remainder of this buffer's red zone
            let data = iobuf.get_mut_range(bytes_to_write, unused_space);

            #[allow(unsafe_code)]
            unsafe {
                // note: this could use slice::fill() if it stabilizes
                std::ptr::write_bytes(
                    data.as_mut_ptr(),
                    MessageKind::Corrupted.into(),
                    unused_space,
                );
            }
        }

        let total_len = if maxed { capacity } else { bytes_to_write };

        let data = iobuf.get_mut_range(0, total_len);
        let stored_max_stable_lsn = iobuf.stored_max_stable_lsn;

        io_fail!(self, "buffer write");
        #[cfg(feature = "io_uring")]
        {
            let mut wrote = 0;
            while wrote < total_len {
                let to_write = &data[wrote..];
                let offset = log_offset + wrote as u64;

                // we take out this mutex to guarantee
                // that our `Link` write operation below
                // is serialized with the following sync.
                // we don't put the `Rio` instance into
                // the `Mutex` because we want to drop the
                // `Mutex` right after beginning the async
                // submission.
                let link_mu = self.submission_mutex.lock();

                // using the `Link` ordering, we specify
                // that `io_uring` should not begin
                // the following `sync_file_range`
                // until the previous write is
                // complete.
                let wrote_completion = self.io_uring.write_at_ordered(
                    &*self.config.file,
                    &to_write,
                    offset,
                    rio::Ordering::Link,
                );

                let sync_completion = self.io_uring.sync_file_range(
                    &*self.config.file,
                    offset,
                    to_write.len(),
                );

                sync_completion.wait()?;

                // TODO we want to move this above the previous `wait`
                // but there seems to be an issue in `rio` that is
                // triggered when multiple threads are submitting
                // events while events from other threads are in play.
                drop(link_mu);

                wrote += wrote_completion.wait()?;
            }
        }
        #[cfg(not(feature = "io_uring"))]
        {
            let f = &self.config.file;
            pwrite_all(f, data, log_offset)?;
            if !self.config.temporary {
                #[cfg(target_os = "linux")]
                {
                    use std::os::unix::io::AsRawFd;
                    let ret = unsafe {
                        libc::sync_file_range(
                            f.as_raw_fd(),
                            i64::try_from(log_offset).unwrap(),
                            i64::try_from(total_len).unwrap(),
                            libc::SYNC_FILE_RANGE_WAIT_BEFORE
                                | libc::SYNC_FILE_RANGE_WRITE
                                | libc::SYNC_FILE_RANGE_WAIT_AFTER,
                        )
                    };
                    if ret < 0 {
                        let err = std::io::Error::last_os_error();
                        if let Some(libc::ENOSYS) = err.raw_os_error() {
                            f.sync_all()?;
                        } else {
                            return Err(err.into());
                        }
                    }
                }

                #[cfg(not(target_os = "linux"))]
                f.sync_all()?;
            }
        }
        io_fail!(self, "buffer write post");

        if total_len > 0 {
            let complete_len = if maxed {
                let lsn_idx = base_lsn / segment_size as Lsn;
                let next_seg_beginning = (lsn_idx + 1) * segment_size as Lsn;
                assert_usize(next_seg_beginning - base_lsn)
            } else {
                total_len
            };

            debug!(
                "wrote lsns {}-{} to disk at offsets {}-{}, maxed {} complete_len {}",
                base_lsn,
                base_lsn + total_len as Lsn - 1,
                log_offset,
                log_offset + total_len as LogOffset - 1,
                maxed,
                complete_len
            );
            self.mark_interval(base_lsn, complete_len);
        }

        M.written_bytes.measure(total_len as u64);

        // NB the below deferred logic is important to ensure
        // that we never actually free a segment until all threads
        // that may have witnessed a DiskPtr that points into it
        // have completed their (crossbeam-epoch)-pinned operations.
        let guard = pin();
        let max_header_stable_lsn = self.max_header_stable_lsn.clone();
        guard.defer(move || {
            trace!("bumping atomic header lsn to {}", stored_max_stable_lsn);
            bump_atomic_lsn(&max_header_stable_lsn, stored_max_stable_lsn)
        });

        guard.flush();

        let current_max_header_stable_lsn =
            self.max_header_stable_lsn.load(SeqCst);

        self.sa_stabilize(current_max_header_stable_lsn, &guard)
    }

    // It's possible that IO buffers are written out of order!
    // So we need to use this to keep track of them, and only
    // increment self.stable. If we didn't do this, then we would
    // accidentally decrement self.stable sometimes, or bump stable
    // above an offset that corresponds to a buffer that hasn't actually
    // been written yet! It's OK to use a mutex here because it is pretty
    // fast, compared to the other operations on shared state.
    fn mark_interval(&self, whence: Lsn, len: usize) {
        debug!("mark_interval({}, {})", whence, len);
        assert!(
            len > 0,
            "mark_interval called with an empty length at {}",
            whence
        );
        let mut intervals = self.intervals.lock();

        let interval = (whence, whence + len as Lsn - 1);

        intervals.push(interval);

        #[cfg(any(test, feature = "event_log", feature = "lock_free_delays"))]
        assert!(
            intervals.len() < 10000,
            "intervals is getting strangely long... {:?}",
            *intervals
        );

        // reverse sort
        intervals.sort_unstable_by(|a, b| b.cmp(a));

        let mut updated = false;

        let len_before = intervals.len();

        while let Some(&(low, high)) = intervals.last() {
            assert!(low <= high);
            let cur_stable = self.stable_lsn.load(SeqCst);
            assert!(
                low > cur_stable,
                "somehow, we marked offset {} stable while \
                 interval {}-{} had not yet been applied!",
                cur_stable,
                low,
                high
            );
            if cur_stable + 1 == low {
                let old = self.stable_lsn.swap(high, SeqCst);
                assert_eq!(
                    old, cur_stable,
                    "concurrent stable offset modification detected"
                );
                debug!("new highest interval: {} - {}", low, high);
                let (_low, _high) = intervals.pop().unwrap();
                updated = true;
            } else {
                break;
            }
        }

        if len_before - intervals.len() > 100 {
            debug!("large merge of {} intervals", len_before - intervals.len());
        }

        if updated {
            // having held the mutex makes this linearized
            // with the notify below.
            drop(intervals);

            let _notified = self.interval_updated.notify_all();
        }
    }

    pub(in crate::pagecache) fn current_iobuf(&self) -> Arc<IoBuf> {
        // we bump up the ref count, and forget the arc to retain a +1.
        // If we didn't forget it, it would then go back down again,
        // even though we just created a new reference to it, leading
        // to double-frees.
        let arc = unsafe { Arc::from_raw(self.iobuf.load(SeqCst)) };
        #[allow(clippy::mem_forget)]
        std::mem::forget(arc.clone());
        arc
    }
}

/// Blocks until the specified log sequence number has
/// been made stable on disk. Returns the number of
/// bytes written.
pub(in crate::pagecache) fn make_stable(
    iobufs: &Arc<IoBufs>,
    lsn: Lsn,
) -> Result<usize> {
    let _measure = Measure::new(&M.make_stable);

    // NB before we write the 0th byte of the file, stable  is -1
    let first_stable = iobufs.stable();
    if first_stable >= lsn {
        return Ok(0);
    }

    let mut stable = first_stable;

    while stable < lsn {
        if let Err(e) = iobufs.config.global_error() {
            let intervals = iobufs.intervals.lock();

            // having held the mutex makes this linearized
            // with the notify below.
            drop(intervals);

            let _notified = iobufs.interval_updated.notify_all();
            return Err(e);
        }

        let iobuf = iobufs.current_iobuf();
        let header = iobuf.get_header();
        if offset(header) == 0 || is_sealed(header) || iobuf.lsn > lsn {
            // nothing to write, don't bother sealing
            // current IO buffer.
        } else {
            maybe_seal_and_write_iobuf(iobufs, &iobuf, header, false)?;
            stable = iobufs.stable();
            // NB we have to continue here to possibly clear
            // the next io buffer, which may have dirty
            // data we need to flush (and maybe no other
            // thread is still alive to do so)
            continue;
        }

        // block until another thread updates the stable lsn
        let mut waiter = iobufs.intervals.lock();

        stable = iobufs.stable();
        if stable < lsn {
            trace!("waiting on cond var for make_stable({})", lsn);

            if cfg!(feature = "event_log") {
                let timeout = iobufs
                    .interval_updated
                    .wait_for(&mut waiter, std::time::Duration::from_secs(30));
                if timeout.timed_out() {
                    fn tn() -> String {
                        std::thread::current()
                            .name()
                            .unwrap_or("unknown")
                            .to_owned()
                    }
                    panic!(
                        "{} failed to make_stable after 30 seconds. \
                         waiting to stabilize lsn {}, current stable {} \
                         intervals: {:?}",
                        tn(),
                        lsn,
                        iobufs.stable(),
                        waiter
                    );
                }
            } else {
                iobufs.interval_updated.wait(&mut waiter);
            }
        } else {
            trace!("make_stable({}) returning", lsn);
            break;
        }
    }

    Ok(assert_usize(stable - first_stable))
}

/// Called by users who wish to force the current buffer
/// to flush some pending writes. Returns the number
/// of bytes written during this call.
pub(in crate::pagecache) fn flush(iobufs: &Arc<IoBufs>) -> Result<usize> {
    let max_reserved_lsn = iobufs.max_reserved_lsn.load(SeqCst);
    make_stable(iobufs, max_reserved_lsn)
}

/// Attempt to seal the current IO buffer, possibly
/// writing it to disk if there are no other writers
/// operating on it.
pub(in crate::pagecache) fn maybe_seal_and_write_iobuf(
    iobufs: &Arc<IoBufs>,
    iobuf: &Arc<IoBuf>,
    header: Header,
    from_reserve: bool,
) -> Result<()> {
    if is_sealed(header) {
        // this buffer is already sealed. nothing to do here.
        return Ok(());
    }

    // NB need to do this before CAS because it can get
    // written and reset by another thread afterward
    let lid = iobuf.offset;
    let lsn = iobuf.lsn;
    let capacity = iobuf.capacity;
    let segment_size = iobufs.config.segment_size;

    if offset(header) > capacity {
        // a race happened, nothing we can do
        return Ok(());
    }

    let sealed = mk_sealed(header);
    let res_len = offset(sealed);

    let maxed = from_reserve || capacity - res_len < MAX_MSG_HEADER_LEN;

    let worked = iobuf.linearized(|| {
        if iobuf.cas_header(header, sealed).is_err() {
            // cas failed, don't try to continue
            return false;
        }

        trace!("sealed iobuf with lsn {}", lsn);

        if maxed {
            // NB we linearize this together with sealing
            // the header here to guarantee that in write_to_log,
            // which may be executing as soon as the seal is set
            // by another thread, the thread that calls
            // iobuf.get_maxed() is linearized with this one!
            trace!("setting maxed to true for iobuf with lsn {}", lsn);
            iobuf.set_maxed(true);
        }
        true
    });
    if !worked {
        return Ok(());
    }

    assert!(
        capacity + SEG_HEADER_LEN >= res_len,
        "res_len of {} higher than buffer capacity {}",
        res_len,
        capacity
    );

    assert_ne!(
        lid,
        LogOffset::max_value(),
        "sealing something that should never have \
         been claimed (iobuf lsn {})\n{:?}",
        lsn,
        iobufs
    );

    // open new slot
    let mut next_lsn = lsn;

    let measure_assign_offset = Measure::new(&M.assign_offset);

    let next_offset = if maxed {
        // roll lsn to the next offset
        let lsn_idx = lsn / segment_size as Lsn;
        next_lsn = (lsn_idx + 1) * segment_size as Lsn;

        // mark unused as clear
        debug!(
            "rolling to new segment after clearing {}-{}",
            lid,
            lid + res_len as LogOffset,
        );

        match iobufs.with_sa(|sa| sa.next(next_lsn)) {
            Ok(ret) => ret,
            Err(e) => {
                iobufs.config.set_global_error(e.clone());
                let intervals = iobufs.intervals.lock();

                // having held the mutex makes this linearized
                // with the notify below.
                drop(intervals);

                let _notified = iobufs.interval_updated.notify_all();
                return Err(e);
            }
        }
    } else {
        debug!(
            "advancing offset within the current segment from {} to {}",
            lid,
            lid + res_len as LogOffset
        );
        next_lsn += res_len as Lsn;

        lid + res_len as LogOffset
    };

    // NB as soon as the "sealed" bit is 0, this allows new threads
    // to start writing into this buffer, so do that after it's all
    // set up. expect this thread to block until the buffer completes
    // its entire life cycle as soon as we do that.
    let next_iobuf = if maxed {
        let mut next_iobuf = IoBuf {
            buf: Arc::new(UnsafeCell::new(AlignedBuf::new(segment_size))),
            header: CachePadded::new(AtomicU64::new(0)),
            base: 0,
            offset: next_offset,
            lsn: next_lsn,
            capacity: segment_size,
            maxed: AtomicBool::new(false),
            linearizer: Mutex::new(()),
            stored_max_stable_lsn: -1,
        };

        next_iobuf.store_segment_header(sealed, next_lsn, iobufs.stable());

        next_iobuf
    } else {
        let new_cap = capacity - res_len;
        assert_ne!(new_cap, 0);
        let last_salt = salt(sealed);
        let new_salt = bump_salt(last_salt);

        IoBuf {
            // reuse the previous io buffer
            buf: iobuf.buf.clone(),
            header: CachePadded::new(AtomicU64::new(new_salt)),
            base: iobuf.base + res_len,
            offset: next_offset,
            lsn: next_lsn,
            capacity: new_cap,
            maxed: AtomicBool::new(false),
            linearizer: Mutex::new(()),
            stored_max_stable_lsn: -1,
        }
    };

    // we acquire this mutex to guarantee that any threads that
    // are going to wait on the condition variable will observe
    // the change.
    debug_delay();
    let intervals = iobufs.intervals.lock();
    let old_ptr = iobufs
        .iobuf
        .swap(Arc::into_raw(Arc::new(next_iobuf)) as *mut IoBuf, SeqCst);

    let old_arc = unsafe { Arc::from_raw(old_ptr) };

    pin().defer(move || drop(old_arc));

    // having held the mutex makes this linearized
    // with the notify below.
    drop(intervals);

    let _notified = iobufs.interval_updated.notify_all();

    drop(measure_assign_offset);

    // if writers is 0, it's our responsibility to write the buffer.
    if n_writers(sealed) == 0 {
        iobufs.config.global_error()?;
        trace!(
            "asynchronously writing iobuf with lsn {} to log from maybe_seal",
            lsn
        );
        let iobufs = iobufs.clone();
        let iobuf = iobuf.clone();
        let _result = threadpool::spawn(move || {
            if let Err(e) = iobufs.write_to_log(&iobuf) {
                error!(
                    "hit error while writing iobuf with lsn {}: {:?}",
                    lsn, e
                );
                let intervals = iobufs.intervals.lock();

                // having held the mutex makes this linearized
                // with the notify below.
                drop(intervals);

                let _notified = iobufs.interval_updated.notify_all();
                iobufs.config.set_global_error(e);
            }
        });

        #[cfg(feature = "event_log")]
        _result.wait();

        Ok(())
    } else {
        Ok(())
    }
}

impl Debug for IoBufs {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> std::result::Result<(), fmt::Error> {
        formatter.write_fmt(format_args!("IoBufs {{ buf: {:?} }}", self.iobuf))
    }
}

impl Debug for IoBuf {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> std::result::Result<(), fmt::Error> {
        let header = self.get_header();
        formatter.write_fmt(format_args!(
            "\n\tIoBuf {{ lid: {}, n_writers: {}, offset: \
             {}, sealed: {} }}",
            self.offset,
            n_writers(header),
            offset(header),
            is_sealed(header)
        ))
    }
}

pub(crate) const fn is_sealed(v: Header) -> bool {
    v & 1 << 31 == 1 << 31
}

pub(crate) const fn mk_sealed(v: Header) -> Header {
    v | 1 << 31
}

pub(crate) const fn n_writers(v: Header) -> Header {
    v << 33 >> 57
}

#[inline]
pub(crate) fn incr_writers(v: Header) -> Header {
    assert_ne!(n_writers(v), MAX_WRITERS);
    v + (1 << 24)
}

#[inline]
pub(crate) fn decr_writers(v: Header) -> Header {
    assert_ne!(n_writers(v), 0);
    v - (1 << 24)
}

#[inline]
pub(crate) fn offset(v: Header) -> usize {
    let ret = v << 40 >> 40;
    usize::try_from(ret).unwrap()
}

#[inline]
pub(crate) fn bump_offset(v: Header, by: usize) -> Header {
    assert_eq!(by >> 24, 0);
    v + (by as Header)
}

pub(crate) const fn bump_salt(v: Header) -> Header {
    (v + (1 << 32)) & 0xFFFF_FFFF_0000_0000
}

pub(crate) const fn salt(v: Header) -> Header {
    v >> 32 << 32
}
