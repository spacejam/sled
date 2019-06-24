use std::{
    mem::size_of,
    sync::atomic::AtomicBool,
    sync::atomic::Ordering::SeqCst,
    sync::{Arc, Condvar, Mutex, RwLock},
};

use self::reader::LogReader;

use super::*;

// This is the most writers in a single IO buffer
// that we have space to accomodate in the counter
// for writers in the IO buffer header.
pub(crate) const MAX_WRITERS: Header = 127;

pub(crate) type Header = u64;

macro_rules! io_fail {
    ($self:expr, $e:expr) => {
        #[cfg(feature = "failpoints")]
        fail_point!($e, |_| {
            $self.config.set_global_error(Error::FailPoint);
            // wake up any waiting threads so they don't stall forever
            $self.interval_updated.notify_all();
            Err(Error::FailPoint)
        });
    };
}

pub(crate) struct IoBuf {
    pub(crate) buf: UnsafeCell<Vec<u8>>,
    header: CachePadded<AtomicU64>,
    pub(super) lid: LogId,
    pub(super) lsn: Lsn,
    pub(super) capacity: usize,
    maxed: AtomicBool,
    linearizer: Mutex<()>,
    stored_max_stable_lsn: Lsn,
}

unsafe impl Sync for IoBuf {}

pub(super) struct IoBufs {
    pub(super) config: Config,

    // We have a fixed number of io buffers. Sometimes they will all be
    // full, and in order to prevent threads from having to spin in
    // the reserve function, we can have them block until a buffer becomes
    // available.
    pub(crate) iobuf: RwLock<Arc<IoBuf>>,

    // Pending intervals that have been written to stable storage, but may be
    // higher than the current value of `stable` due to interesting thread
    // interleavings.
    pub(crate) intervals: Mutex<Vec<(Lsn, Lsn)>>,
    pub(super) interval_updated: Condvar,

    // The highest CONTIGUOUS log sequence number that has been written to
    // stable storage. This may be lower than the length of the underlying
    // file, and there may be buffers that have been written out-of-order
    // to stable storage due to interesting thread interleavings.
    pub(crate) stable_lsn: AtomicLsn,
    pub(crate) max_reserved_lsn: AtomicLsn,
    pub(crate) max_header_stable_lsn: AtomicLsn,
    pub(crate) segment_accountant: Mutex<SegmentAccountant>,
}

/// `IoBufs` is a set of lock-free buffers for coordinating
/// writes to underlying storage.
impl IoBufs {
    pub(crate) fn start(
        config: Config,
        mut snapshot: Snapshot,
    ) -> Result<IoBufs> {
        // open file for writing
        let file = &config.file;

        let io_buf_size = config.io_buf_size;

        if snapshot.max_lsn < SEG_HEADER_LEN as Lsn {
            debug!(
                "setting snapshot to default as max_lsn < {}",
                SEG_HEADER_LEN
            );
            snapshot = Snapshot::default();
        }

        let snapshot_max_lsn = snapshot.max_lsn;
        let snapshot_last_lid = snapshot.last_lid;
        let snapshot_max_header_stable_lsn = snapshot.max_header_stable_lsn;

        let mut segment_accountant: SegmentAccountant =
            SegmentAccountant::start(config.clone(), snapshot)?;

        let (next_lsn, next_lid) =
            if snapshot_max_lsn % config.io_buf_size as Lsn == 0 {
                (snapshot_max_lsn, snapshot_last_lid)
            } else {
                let width = match file.read_message(
                    snapshot_last_lid,
                    snapshot_max_lsn,
                    &config,
                ) {
                    Ok(LogRead::Failed(_, len))
                    | Ok(LogRead::Inline(_, _, len)) => len + MSG_HEADER_LEN,
                    Ok(LogRead::Blob(_lsn, _buf, _blob_ptr)) => {
                        BLOB_INLINE_LEN + MSG_HEADER_LEN
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
                    snapshot_max_lsn + width as Lsn,
                    snapshot_last_lid + width as LogId,
                )
            };

        let mut iobuf = IoBuf::new(io_buf_size);

        trace!(
            "starting IoBufs with next_lsn: {} \
             next_lid: {}",
            next_lsn,
            next_lid
        );

        // we want stable to begin at -1 if the 0th byte
        // of our file has not yet been written.
        let stable = next_lsn - 1;

        if next_lsn % config.io_buf_size as Lsn == 0 {
            // allocate new segment for data

            if next_lsn == 0 {
                assert_eq!(next_lid, 0);
            }
            let lid = segment_accountant.next(next_lsn)?;

            iobuf.lid = lid;
            iobuf.capacity = io_buf_size;
            iobuf.store_segment_header(0, next_lsn, stable);

            maybe_fail!("initial allocation");
            file.pwrite_all(&*vec![0; config.io_buf_size], lid)?;
            file.sync_all()?;
            maybe_fail!("initial allocation post");

            debug!(
                "starting log at clean offset {}, recovered lsn {}",
                next_lid, next_lsn
            );
        } else {
            // the tip offset is not completely full yet, reuse it
            let offset = assert_usize(next_lid % io_buf_size as LogId);
            iobuf.lid = next_lid;
            iobuf.capacity = io_buf_size - offset;
            iobuf.lsn = next_lsn;

            debug!(
                "starting log at split offset {}, recovered lsn {}",
                next_lid, next_lsn
            );
        }

        // remove all blob files larger than our stable offset
        gc_blobs(&config, stable)?;

        Ok(IoBufs {
            config,

            iobuf: RwLock::new(Arc::new(iobuf)),

            intervals: Mutex::new(vec![]),
            interval_updated: Condvar::new(),

            stable_lsn: AtomicLsn::new(stable),
            max_reserved_lsn: AtomicLsn::new(stable),
            max_header_stable_lsn: AtomicLsn::new(
                snapshot_max_header_stable_lsn,
            ),
            segment_accountant: Mutex::new(segment_accountant),
        })
    }

    /// SegmentAccountant access for coordination with the `PageCache`
    pub(super) fn with_sa<B, F>(&self, f: F) -> B
    where
        F: FnOnce(&mut SegmentAccountant) -> B,
    {
        let start = clock();

        debug_delay();
        let mut sa = self.segment_accountant.lock().unwrap();

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
        let io_buf_size = self.config.io_buf_size;
        let segment_base_lsn = lsn / io_buf_size as Lsn * io_buf_size as Lsn;
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
    pub(super) fn stable(&self) -> Lsn {
        debug_delay();
        self.stable_lsn.load(SeqCst) as Lsn
    }

    // Adds a header to the front of the buffer
    pub(crate) fn encapsulate(
        &self,
        in_buf: &[u8],
        out_buf: &mut [u8],
        lsn: Lsn,
        over_blob_threshold: bool,
        is_blob_rewrite: bool,
    ) -> Result<()> {
        let mut _blob_ptr = None;

        let to_reserve = if over_blob_threshold {
            // write blob to file
            io_fail!(self, "blob blob write");
            write_blob(&self.config, lsn, in_buf)?;

            let lsn_buf: [u8; size_of::<BlobPointer>()] =
                u64_to_arr(lsn as u64);

            _blob_ptr = Some(lsn_buf);

            _blob_ptr.as_mut().unwrap()
        } else {
            in_buf
        };

        assert_eq!(out_buf.len(), to_reserve.len() + MSG_HEADER_LEN);

        let header = MessageHeader {
            kind: if over_blob_threshold || is_blob_rewrite {
                MessageKind::Blob
            } else {
                MessageKind::Inline
            },
            lsn,
            len: to_reserve.len(),
            crc32: 0,
        };

        let header_bytes: [u8; MSG_HEADER_LEN] = header.into();

        unsafe {
            std::ptr::copy_nonoverlapping(
                header_bytes.as_ptr(),
                out_buf.as_mut_ptr(),
                MSG_HEADER_LEN,
            );
            std::ptr::copy_nonoverlapping(
                to_reserve.as_ptr(),
                out_buf.as_mut_ptr().add(MSG_HEADER_LEN),
                to_reserve.len(),
            );
        }

        Ok(())
    }

    // ensure self.max_reserved_lsn is set to this Lsn or greater
    pub(crate) fn bump_max_reserved_lsn(&self, lsn: Lsn) {
        let mut current = self.max_reserved_lsn.load(SeqCst);
        loop {
            if current >= lsn {
                return;
            }
            let last =
                self.max_reserved_lsn.compare_and_swap(current, lsn, SeqCst);
            if last == current {
                // we succeeded.
                return;
            }
            current = last;
        }
    }

    // ensure self.max_header_stable_lsn is set to this Lsn or greater
    pub(crate) fn bump_max_header_stable_lsn(&self, lsn: Lsn) {
        let mut current = self.max_header_stable_lsn.load(SeqCst);
        loop {
            if current >= lsn {
                return;
            }
            let last = self
                .max_header_stable_lsn
                .compare_and_swap(current, lsn, SeqCst);
            if last == current {
                // we succeeded.
                return;
            }
            current = last;
        }
    }

    // Write an IO buffer's data to stable storage and set up the
    // next IO buffer for writing.
    pub(crate) fn write_to_log(&self, iobuf: &IoBuf) -> Result<()> {
        let _measure = Measure::new(&M.write_to_log);
        let header = iobuf.get_header();
        let lid = iobuf.lid;
        let base_lsn = iobuf.lsn;
        let capacity = iobuf.capacity;

        let io_buf_size = self.config.io_buf_size;

        assert_eq!(
            (lid % io_buf_size as LogId) as Lsn,
            base_lsn % io_buf_size as Lsn
        );

        assert_ne!(
            lid,
            LogId::max_value(),
            "created reservation for uninitialized slot",
        );

        assert!(is_sealed(header));

        let bytes_to_write = offset(header);

        let maxed = iobuf.linearized(|| iobuf.get_maxed());
        let unused_space = capacity - bytes_to_write;
        let should_pad = maxed && unused_space >= MSG_HEADER_LEN;

        // a pad is a null message written to the end of a buffer
        // to signify that nothing else will be written into it
        if should_pad {
            let data = unsafe { (*iobuf.buf.get()).as_mut_slice() };
            let pad_len = capacity - bytes_to_write - MSG_HEADER_LEN;

            // take the crc of the random bytes already after where we
            // would place our header.
            let padding_bytes = vec![EVIL_BYTE; pad_len];

            let header = MessageHeader {
                kind: MessageKind::Pad,
                lsn: base_lsn + bytes_to_write as Lsn,
                len: pad_len,
                crc32: 0,
            };

            let header_bytes: [u8; MSG_HEADER_LEN] = header.into();

            unsafe {
                std::ptr::copy_nonoverlapping(
                    header_bytes.as_ptr(),
                    data.as_mut_ptr().add(bytes_to_write),
                    MSG_HEADER_LEN,
                );
                std::ptr::copy_nonoverlapping(
                    padding_bytes.as_ptr(),
                    data.as_mut_ptr().add(bytes_to_write + MSG_HEADER_LEN),
                    pad_len,
                );
            }

            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&padding_bytes);
            hasher.update(&header_bytes);
            let crc32 = hasher.finalize();
            let crc32_arr = u32_to_arr(crc32 ^ 0xFFFF_FFFF);

            unsafe {
                std::ptr::copy_nonoverlapping(
                    crc32_arr.as_ptr(),
                    data.as_mut_ptr().add(bytes_to_write + 13),
                    std::mem::size_of::<u32>(),
                );
            }
        }

        let total_len = if maxed { capacity } else { bytes_to_write };

        let data = unsafe { (*iobuf.buf.get()).as_mut_slice() };

        let f = &self.config.file;
        io_fail!(self, "buffer write");
        f.pwrite_all(&data[..total_len], lid)?;
        f.sync_all()?;
        io_fail!(self, "buffer write post");

        if total_len > 0 {
            let complete_len = if maxed {
                let lsn_idx = base_lsn / io_buf_size as Lsn;
                let next_seg_beginning = (lsn_idx + 1) * io_buf_size as Lsn;
                assert_usize(next_seg_beginning - base_lsn)
            } else {
                total_len
            };

            debug!(
                "wrote lsns {}-{} to disk at offsets {}-{}, maxed {} complete_len {}",
                base_lsn,
                base_lsn + total_len as Lsn - 1,
                lid,
                lid + total_len as LogId - 1,
                maxed,
                complete_len
            );
            self.mark_interval(base_lsn, complete_len);
        }

        M.written_bytes.measure(total_len as f64);

        self.bump_max_header_stable_lsn(iobuf.stored_max_stable_lsn);

        Ok(())
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
        let mut intervals = self.intervals.lock().unwrap();
        let lsn_before = self.stable_lsn.load(SeqCst) as Lsn;

        let interval = (whence, whence + len as Lsn - 1);

        intervals.push(interval);

        #[cfg(feature = "event_log")]
        assert!(
            intervals.len() < 10000,
            "intervals is getting crazy... {:?}",
            *intervals
        );

        // reverse sort
        intervals.sort_unstable_by(|a, b| b.cmp(a));

        let mut updated = false;

        let len_before = intervals.len();
        let mut lsn_after = lsn_before;

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
                intervals.pop();
                updated = true;
                lsn_after = high;
            } else {
                break;
            }
        }

        if len_before - intervals.len() > 100 {
            debug!("large merge of {} intervals", len_before - intervals.len());
        }

        if updated {
            self.interval_updated.notify_all();
        }

        // NB we continue to hold the intervals mutex for
        // our calls to the segment accountant below so
        // that we guarantee that we deactivate segments
        // in order of LSN.
        let logical_segment_before =
            lsn_before / self.config.io_buf_size as Lsn;
        let logical_segment_after = lsn_after / self.config.io_buf_size as Lsn;
        if logical_segment_before != logical_segment_after {
            self.with_sa(move |sa| {
                for logical_segment in logical_segment_before..logical_segment_after {
                    let segment_lsn = logical_segment * self.config.io_buf_size as Lsn;
                    // transition this segment into deplete-only mode
                    trace!(
                        "deactivating segment with lsn {}",
                        segment_lsn,
                    );
                    if let Err(e) = sa.deactivate_segment(segment_lsn) {
                        error!("segment accountant failed to deactivate segment: {}", e);
                    }
                }
            });
        }
    }

    pub(super) fn current_iobuf(&self) -> Arc<IoBuf> {
        self.iobuf.read().unwrap().clone()
    }
}

/// Blocks until the specified log sequence number has
/// been made stable on disk. Returns the number of
/// bytes written.
pub(crate) fn make_stable(iobufs: &Arc<IoBufs>, lsn: Lsn) -> Result<usize> {
    let _measure = Measure::new(&M.make_stable);

    // NB before we write the 0th byte of the file, stable  is -1
    let first_stable = iobufs.stable();
    if first_stable >= lsn {
        return Ok(0);
    }

    let mut stable = first_stable;

    while stable < lsn {
        if let Err(e) = iobufs.config.global_error() {
            iobufs.interval_updated.notify_all();
            return Err(e);
        }

        let iobuf = iobufs.current_iobuf();
        let header = iobuf.get_header();
        if offset(header) == 0 || is_sealed(header) || iobuf.lsn > lsn {
            // nothing to write, don't bother sealing
            // current IO buffer.
        } else {
            maybe_seal_and_write_iobuf(iobufs, &iobuf, header, false)?;
        }

        // block until another thread updates the stable lsn
        let waiter = iobufs.intervals.lock().unwrap();

        stable = iobufs.stable();
        if stable < lsn {
            trace!("waiting on cond var for make_stable({})", lsn);

            let _ = iobufs.interval_updated.wait(waiter).unwrap();
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
pub(super) fn flush(iobufs: &Arc<IoBufs>) -> Result<usize> {
    let max_reserved_lsn = iobufs.max_reserved_lsn.load(SeqCst) as Lsn;
    make_stable(iobufs, max_reserved_lsn)
}

/// Attempt to seal the current IO buffer, possibly
/// writing it to disk if there are no other writers
/// operating on it.
pub(crate) fn maybe_seal_and_write_iobuf(
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
    let lid = iobuf.lid;
    let lsn = iobuf.lsn;
    let capacity = iobuf.capacity;
    let io_buf_size = iobufs.config.io_buf_size;

    if offset(header) > capacity {
        // a race happened, nothing we can do
        return Ok(());
    }

    let sealed = mk_sealed(header);
    let res_len = offset(sealed);

    let maxed = from_reserve || capacity - res_len < MSG_HEADER_LEN;

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
        LogId::max_value(),
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
        let lsn_idx = lsn / io_buf_size as Lsn;
        next_lsn = (lsn_idx + 1) * io_buf_size as Lsn;

        // mark unused as clear
        debug!(
            "rolling to new segment after clearing {}-{}",
            lid,
            lid + res_len as LogId,
        );

        match iobufs.with_sa(|sa| sa.next(next_lsn)) {
            Ok(ret) => ret,
            Err(e) => {
                iobufs.interval_updated.notify_all();
                return Err(e);
            }
        }
    } else {
        debug!(
            "advancing offset within the current segment from {} to {}",
            lid,
            lid + res_len as LogId
        );
        next_lsn += res_len as Lsn;

        lid + res_len as LogId
    };

    let mut next_iobuf = IoBuf::new(io_buf_size);
    next_iobuf.lid = next_offset;

    // NB as soon as the "sealed" bit is 0, this allows new threads
    // to start writing into this buffer, so do that after it's all
    // set up. expect this thread to block until the buffer completes
    // its entire lifecycle as soon as we do that.
    if maxed {
        next_iobuf.capacity = io_buf_size;
        next_iobuf.store_segment_header(sealed, next_lsn, iobufs.stable());
    } else {
        let new_cap = capacity - res_len;
        assert_ne!(new_cap, 0);
        next_iobuf.capacity = new_cap;
        next_iobuf.lsn = next_lsn;
        let last_salt = salt(sealed);
        let new_salt = bump_salt(last_salt);
        next_iobuf.set_header(new_salt);
    }

    // we acquire this mutex to guarantee that any threads that
    // are going to wait on the condition variable will observe
    // the change.
    debug_delay();
    let mut mu = iobufs.iobuf.write().unwrap();
    *mu = Arc::new(next_iobuf);
    drop(mu);

    drop(measure_assign_offset);

    // if writers is 0, it's our responsibility to write the buffer.
    if n_writers(sealed) == 0 {
        if let Err(e) = iobufs.config.global_error() {
            iobufs.interval_updated.notify_all();
            return Err(e);
        }
        if let Some(ref thread_pool) = iobufs.config.thread_pool {
            trace!(
                "asynchronously writing iobuf with lsn {} to log from maybe_seal",
                lsn
            );
            let iobufs = iobufs.clone();
            let iobuf = iobuf.clone();
            thread_pool.spawn(move || {
                if let Err(e) = iobufs.write_to_log(&iobuf) {
                    error!(
                        "hit error while writing iobuf with lsn {}: {:?}",
                        lsn, e
                    );
                    iobufs.config.set_global_error(e);
                }
            });
            Ok(())
        } else {
            trace!(
                "synchronously writing iobuf with lsn {} to log from maybe_seal",
                lsn
            );
            iobufs.write_to_log(iobuf)
        }
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
            self.lid,
            n_writers(header),
            offset(header),
            is_sealed(header)
        ))
    }
}

impl IoBuf {
    pub(crate) fn new(buf_size: usize) -> IoBuf {
        IoBuf {
            buf: UnsafeCell::new(vec![0; buf_size]),
            header: CachePadded::new(AtomicU64::new(0)),
            lid: LogId::max_value(),
            lsn: 0,
            capacity: 0,
            maxed: AtomicBool::new(false),
            linearizer: Mutex::new(()),
            stored_max_stable_lsn: 0,
        }
    }

    // use this for operations on an IoBuf that must be
    // linearized together, and can't fit in the header!
    pub(crate) fn linearized<F, B>(&self, f: F) -> B
    where
        F: FnOnce() -> B,
    {
        let _l = self.linearizer.lock().unwrap();
        f()
    }

    // This is called upon the initialization of a fresh segment.
    // We write a new segment header to the beginning of the buffer
    // for assistance during recovery. The caller is responsible
    // for ensuring that the IoBuf's capacity has been set properly.
    pub(crate) fn store_segment_header(
        &mut self,
        last: Header,
        lsn: Lsn,
        highest_known_stable_lsn: Lsn,
    ) {
        debug!("storing lsn {} in beginning of buffer", lsn);
        assert!(self.capacity >= SEG_HEADER_LEN);

        self.stored_max_stable_lsn = highest_known_stable_lsn;

        self.lsn = lsn;

        let header = SegmentHeader {
            lsn,
            highest_known_stable_lsn,
            ok: true,
        };
        let header_bytes: [u8; SEG_HEADER_LEN] = header.into();

        unsafe {
            std::ptr::copy_nonoverlapping(
                header_bytes.as_ptr(),
                (*self.buf.get()).as_mut_ptr(),
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

pub(crate) const fn is_sealed(v: Header) -> bool {
    v & 1 << 31 == 1 << 31
}

pub(crate) const fn mk_sealed(v: Header) -> Header {
    v | 1 << 31
}

pub(crate) const fn n_writers(v: Header) -> Header {
    v << 33 >> 57
}

#[cfg_attr(not(feature = "no_inline"), inline)]
pub(crate) fn incr_writers(v: Header) -> Header {
    assert_ne!(n_writers(v), MAX_WRITERS);
    v + (1 << 24)
}

#[cfg_attr(not(feature = "no_inline"), inline)]
pub(crate) fn decr_writers(v: Header) -> Header {
    assert_ne!(n_writers(v), 0);
    v - (1 << 24)
}

pub(crate) const fn offset(v: Header) -> usize {
    let ret = v << 40 >> 40;
    ret as usize
}

#[cfg_attr(not(feature = "no_inline"), inline)]
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
