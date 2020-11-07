use std::{
    alloc::{alloc, dealloc, Layout},
    cell::UnsafeCell,
    sync::atomic::AtomicPtr,
};

use crate::{pagecache::*, *};

macro_rules! io_fail {
    ($self:expr, $e:expr) => {
        #[cfg(feature = "failpoints")]
        {
            debug_delay();
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
        let last_salt = header::salt(last);
        let new_salt = header::bump_salt(last_salt);
        let bumped = header::bump_offset(new_salt, SEG_HEADER_LEN);
        self.set_header(bumped);
    }

    pub(crate) fn get_header(&self) -> Header {
        debug_delay();
        self.header.load(Acquire)
    }

    pub(crate) fn set_header(&self, new: Header) {
        debug_delay();
        self.header.store(new, Release);
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

#[derive(Debug)]
pub(crate) struct StabilityIntervals {
    fsynced_ranges: Vec<(Lsn, Lsn)>,
    batches: BTreeMap<Lsn, Lsn>,
    stable_lsn: Lsn,
}

impl StabilityIntervals {
    fn new(lsn: Lsn) -> StabilityIntervals {
        StabilityIntervals {
            stable_lsn: lsn,
            fsynced_ranges: vec![],
            batches: BTreeMap::default(),
        }
    }

    pub(crate) fn mark_batch(&mut self, interval: (Lsn, Lsn)) {
        assert!(interval.0 > self.stable_lsn);
        self.batches.insert(interval.0, interval.1);
    }

    fn mark_fsync(&mut self, interval: (Lsn, Lsn)) -> Option<Lsn> {
        trace!(
            "pushing interval {:?} into fsynced_ranges {:?}",
            interval,
            self.fsynced_ranges
        );
        if let Some((low, high)) = self.fsynced_ranges.last_mut() {
            if *low == interval.1 + 1 {
                *low = interval.0
            } else if *high + 1 == interval.0 {
                *high = interval.1
            } else {
                self.fsynced_ranges.push(interval);
            }
        } else {
            self.fsynced_ranges.push(interval);
        }

        #[cfg(any(test, feature = "event_log", feature = "lock_free_delays"))]
        assert!(
            self.fsynced_ranges.len() < 10000,
            "intervals is getting strangely long... {:?}",
            self
        );

        // reverse sort
        self.fsynced_ranges
            .sort_unstable_by_key(|&range| std::cmp::Reverse(range));

        while let Some(&(low, high)) = self.fsynced_ranges.last() {
            assert!(low <= high);
            let cur_stable = self.stable_lsn;
            assert!(
                low > cur_stable,
                "somehow, we marked offset {} stable while \
                 interval {}-{} had not yet been applied!",
                cur_stable,
                low,
                high
            );
            if cur_stable + 1 == low {
                debug!("new highest interval: {} - {}", low, high);
                self.fsynced_ranges.pop().unwrap();
                self.stable_lsn = high;
            } else {
                break;
            }
        }

        let mut batch_stable_lsn = None;

        // batches must be atomically recoverable, which
        // means that we should wait until the entire
        // batch has been stabilized before any parts
        // of the batch are allowed to be reused
        // due to having marked them as stable.
        while let Some((low, high)) =
            self.batches.iter().map(|(l, h)| (*l, *h)).next()
        {
            assert!(
                low < high,
                "expected batch low mark {} to be below high mark {}",
                low,
                high
            );

            if high <= self.stable_lsn {
                // the entire batch has been written to disk
                // and fsynced, so we can propagate its stability
                // through the `batch_stable_lsn` variable.
                if let Some(bsl) = batch_stable_lsn {
                    assert!(bsl < high);
                }
                batch_stable_lsn = Some(high);
                self.batches.remove(&low).unwrap();
            } else {
                if low <= self.stable_lsn {
                    // the batch has not been fully written
                    // to disk, but we can communicate that
                    // the region before the batch has
                    // stabilized.
                    batch_stable_lsn = Some(low - 1);
                }
                break;
            }
        }

        if self.batches.is_empty() {
            Some(self.stable_lsn)
        } else {
            batch_stable_lsn
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
    pub intervals: Mutex<StabilityIntervals>,
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
        let segment_cleaner = SegmentCleaner::default();

        let mut segment_accountant: SegmentAccountant =
            SegmentAccountant::start(
                config.clone(),
                snapshot,
                segment_cleaner.clone(),
            )?;

        let segment_size = config.segment_size;

        let (recovered_lid, recovered_lsn) =
            snapshot.recovered_coords(config.segment_size);

        let (next_lid, next_lsn) = match (recovered_lid, recovered_lsn) {
            (Some(next_lid), Some(next_lsn)) => {
                debug!(
                    "starting log at recovered active \
                    offset {}, recovered lsn {}",
                    next_lid, next_lsn
                );
                (next_lid, next_lsn)
            }
            (None, None) => {
                debug!("starting log for a totally fresh system");
                let next_lsn = 0;
                let next_lid = segment_accountant.next(next_lsn)?;
                (next_lid, next_lsn)
            }
            (None, Some(next_lsn)) => {
                let next_lid = segment_accountant.next(next_lsn)?;
                debug!(
                    "starting log at clean offset {}, recovered lsn {}",
                    next_lid, next_lsn
                );
                (next_lid, next_lsn)
            }
            (Some(_), None) => unreachable!(),
        };

        assert!(next_lsn >= Lsn::try_from(next_lid).unwrap());

        debug!(
            "starting IoBufs with next_lsn: {} \
             next_lid: {}",
            next_lsn, next_lid
        );

        // we want stable to begin at -1 if the 0th byte
        // of our file has not yet been written.
        let stable = next_lsn - 1;

        // the tip offset is not completely full yet, reuse it
        let base = assert_usize(next_lid % segment_size as LogOffset);

        let mut iobuf = IoBuf {
            buf: Arc::new(UnsafeCell::new(AlignedBuf::new(segment_size))),
            header: CachePadded::new(AtomicU64::new(0)),
            base,
            offset: next_lid,
            lsn: next_lsn,
            capacity: segment_size - base,
            stored_max_stable_lsn: -1,
        };

        if snapshot.active_segment.is_none() {
            iobuf.store_segment_header(0, next_lsn, stable);
        }

        Ok(IoBufs {
            config,

            iobuf: AtomicPtr::new(Arc::into_raw(Arc::new(iobuf)) as *mut IoBuf),

            intervals: Mutex::new(StabilityIntervals::new(stable)),
            interval_updated: Condvar::new(),

            stable_lsn: AtomicLsn::new(stable),
            max_reserved_lsn: AtomicLsn::new(stable),
            max_header_stable_lsn: Arc::new(AtomicLsn::new(next_lsn)),
            segment_accountant: Mutex::new(segment_accountant),
            segment_cleaner,
            deferred_segment_ops: stack::Stack::default(),
            #[cfg(feature = "io_uring")]
            submission_mutex: Mutex::new(()),
            #[cfg(feature = "io_uring")]
            io_uring: rio::new()?,
        })
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
        old_cache_infos: &[CacheInfo],
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
                old_cache_infos: old_cache_infos.to_vec(),
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
        let segments = self.with_sa(|sa| sa.segment_snapshot_iter_from(lsn));

        LogIter {
            config: self.config.clone(),
            max_lsn: Some(self.stable()),
            cur_lsn: None,
            segment_base: None,
            segments,
            last_stage: false,
        }
    }

    /// Returns the last stable offset in storage.
    pub(in crate::pagecache) fn stable(&self) -> Lsn {
        debug_delay();
        self.stable_lsn.load(Acquire)
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

        assert!(header::is_sealed(header));

        let bytes_to_write = header::offset(header);

        trace!(
            "write_to_log log_offset {} lsn {} len {}",
            log_offset,
            base_lsn,
            bytes_to_write
        );

        let maxed = header::is_maxed(header);
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

            // initialize the remainder of this buffer (only pad_len of this
            // will be part of the Cap message)
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
            self.max_header_stable_lsn.load(Acquire);

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

        let updated = intervals.mark_fsync(interval);

        if let Some(new_stable_lsn) = updated {
            trace!("mark_interval new highest lsn {}", new_stable_lsn);
            self.stable_lsn.store(new_stable_lsn, SeqCst);

            #[cfg(feature = "event_log")]
            {
                // We add 1 because we want it to stay monotonic with recovery
                // LSN, which deals with the next LSN after the last stable one.
                // We need to do this while intervals is held otherwise it
                // may race with another thread that stabilizes something
                // lower.
                self.config.event_log.stabilized_lsn(new_stable_lsn + 1);
            }

            // having held the mutex makes this linearized
            // with the notify below.
            drop(intervals);
        }
        let _notified = self.interval_updated.notify_all();
    }

    pub(in crate::pagecache) fn current_iobuf(&self) -> Arc<IoBuf> {
        // we bump up the ref count, and forget the arc to retain a +1.
        // If we didn't forget it, it would then go back down again,
        // even though we just created a new reference to it, leading
        // to double-frees.
        let arc = unsafe { Arc::from_raw(self.iobuf.load(Acquire)) };
        #[allow(clippy::mem_forget)]
        std::mem::forget(arc.clone());
        arc
    }
}

pub(crate) fn roll_iobuf(iobufs: &Arc<IoBufs>) -> Result<usize> {
    let iobuf = iobufs.current_iobuf();
    let header = iobuf.get_header();
    if header::is_sealed(header) {
        trace!("skipping roll_iobuf due to already-sealed header");
        return Ok(0);
    }
    if header::offset(header) == 0 {
        trace!("skipping roll_iobuf due to empty segment");
    } else {
        trace!("sealing ioubuf from  roll_iobuf");
        maybe_seal_and_write_iobuf(iobufs, &iobuf, header, false)?;
    }

    Ok(header::offset(header))
}

/// Blocks until the specified log sequence number has
/// been made stable on disk. Returns the number of
/// bytes written. Suitable as a full consistency
/// barrier.
pub(in crate::pagecache) fn make_stable(
    iobufs: &Arc<IoBufs>,
    lsn: Lsn,
) -> Result<usize> {
    make_stable_inner(iobufs, lsn, false)
}

/// Blocks until the specified log sequence number
/// has been written to disk. it's assumed that
/// log messages are always written contiguously
/// due to the way reservations manage io buffer
/// tenancy. this is only suitable for use
/// before trying to read a message from the log,
/// so that the system can avoid a full barrier
/// if the desired item has already been made
/// durable.
pub(in crate::pagecache) fn make_durable(
    iobufs: &Arc<IoBufs>,
    lsn: Lsn,
) -> Result<usize> {
    make_stable_inner(iobufs, lsn, true)
}

pub(in crate::pagecache) fn make_stable_inner(
    iobufs: &Arc<IoBufs>,
    lsn: Lsn,
    partial_durability: bool,
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
        if header::offset(header) == 0
            || header::is_sealed(header)
            || iobuf.lsn > lsn
        {
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

        // check global error again now that we are holding a mutex
        if let Err(e) = iobufs.config.global_error() {
            // having held the mutex makes this linearized
            // with the notify below.
            drop(waiter);

            let _notified = iobufs.interval_updated.notify_all();
            return Err(e);
        }

        stable = iobufs.stable();

        if partial_durability {
            if waiter.stable_lsn > lsn {
                return Ok(assert_usize(stable - first_stable));
            }

            for (low, high) in &waiter.fsynced_ranges {
                if *low <= lsn && *high > lsn {
                    return Ok(assert_usize(stable - first_stable));
                }
            }
        }

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
            debug!("make_stable({}) returning", lsn);
            break;
        }
    }

    Ok(assert_usize(stable - first_stable))
}

/// Called by users who wish to force the current buffer
/// to flush some pending writes. Returns the number
/// of bytes written during this call.
pub(in crate::pagecache) fn flush(iobufs: &Arc<IoBufs>) -> Result<usize> {
    let _cc = concurrency_control::read();
    let max_reserved_lsn = iobufs.max_reserved_lsn.load(Acquire);
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
    if header::is_sealed(header) {
        // this buffer is already sealed. nothing to do here.
        return Ok(());
    }

    // NB need to do this before CAS because it can get
    // written and reset by another thread afterward
    let lid = iobuf.offset;
    let lsn = iobuf.lsn;
    let capacity = iobuf.capacity;
    let segment_size = iobufs.config.segment_size;

    if header::offset(header) > capacity {
        // a race happened, nothing we can do
        return Ok(());
    }

    let res_len = header::offset(header);
    let maxed = from_reserve || capacity - res_len < MAX_MSG_HEADER_LEN;
    let sealed = if maxed {
        trace!("setting maxed to true for iobuf with lsn {}", lsn);
        header::mk_maxed(header::mk_sealed(header))
    } else {
        header::mk_sealed(header)
    };

    let worked = iobuf.cas_header(header, sealed).is_ok();
    if !worked {
        return Ok(());
    }

    trace!("sealed iobuf with lsn {}", lsn);

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
            stored_max_stable_lsn: -1,
        };

        next_iobuf.store_segment_header(sealed, next_lsn, iobufs.stable());

        next_iobuf
    } else {
        let new_cap = capacity - res_len;
        assert_ne!(new_cap, 0);
        let last_salt = header::salt(sealed);
        let new_salt = header::bump_salt(last_salt);

        IoBuf {
            // reuse the previous io buffer
            buf: iobuf.buf.clone(),
            header: CachePadded::new(AtomicU64::new(new_salt)),
            base: iobuf.base + res_len,
            offset: next_offset,
            lsn: next_lsn,
            capacity: new_cap,
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
    if header::n_writers(sealed) == 0 {
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

                // store error before notifying so that waiting threads will see
                // it
                iobufs.config.set_global_error(e);

                let intervals = iobufs.intervals.lock();

                // having held the mutex makes this linearized
                // with the notify below.
                drop(intervals);

                let _notified = iobufs.interval_updated.notify_all();
            }
        })?;

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
            header::n_writers(header),
            header::offset(header),
            header::is_sealed(header)
        ))
    }
}
