#[cfg(target_pointer_width = "32")]
use std::sync::atomic::AtomicI64 as AtomicLsn;
#[cfg(target_pointer_width = "64")]
use std::sync::atomic::AtomicIsize as AtomicLsn;
#[cfg(feature = "failpoints")]
use std::sync::atomic::Ordering::Relaxed;
use std::{
    mem::size_of,
    sync::atomic::Ordering::SeqCst,
    sync::atomic::{spin_loop_hint, AtomicBool, AtomicUsize},
    sync::{Arc, Condvar, Mutex},
};

#[cfg(feature = "zstd")]
use zstd::block::compress;

use self::reader::LogReader;

use super::*;

// This is the most writers in a single IO buffer
// that we have space to accomodate in the counter
// for writers in the IO buffer header.
const MAX_WRITERS: Header = 127;

type Header = u64;

/// A logical sequence number.
#[cfg(target_pointer_width = "64")]
type InnerLsn = isize;
#[cfg(target_pointer_width = "32")]
type InnerLsn = i64;

macro_rules! io_fail {
    ($self:expr, $e:expr) => {
        #[cfg(feature = "failpoints")]
        fail_point!($e, |_| {
            $self._failpoint_crashing.store(true, SeqCst);
            // wake up any waiting threads so they don't stall forever
            $self.interval_updated.notify_all();
            Err(Error::FailPoint)
        });
    };
}

struct IoBuf {
    buf: UnsafeCell<Vec<u8>>,
    header: AtomicUsize,
    lid: AtomicUsize,
    lsn: AtomicUsize,
    capacity: AtomicUsize,
    maxed: AtomicBool,
    linearizer: Mutex<()>,
}

unsafe impl Sync for IoBuf {}

pub(super) struct IoBufs {
    pub(super) config: Config,

    // We have a fixed number of io buffers. Sometimes they will all be
    // full, and in order to prevent threads from having to spin in
    // the reserve function, we can have them block until a buffer becomes
    // available.
    buf_mu: Mutex<()>,
    buf_updated: Condvar,
    bufs: Vec<IoBuf>,
    current_buf: AtomicUsize,
    written_bufs: AtomicUsize,

    // Pending intervals that have been written to stable storage, but may be
    // higher than the current value of `stable` due to interesting thread
    // interleavings.
    intervals: Mutex<Vec<(Lsn, Lsn)>>,
    pub(super) interval_updated: Condvar,

    // The highest CONTIGUOUS log sequence number that has been written to
    // stable storage. This may be lower than the length of the underlying
    // file, and there may be buffers that have been written out-of-order
    // to stable storage due to interesting thread interleavings.
    stable_lsn: AtomicLsn,
    max_reserved_lsn: AtomicLsn,
    segment_accountant: Arc<Mutex<SegmentAccountant>>,

    // used for signifying that we're simulating a crash
    #[cfg(feature = "failpoints")]
    pub(super) _failpoint_crashing: AtomicBool,
}

/// `IoBufs` is a set of lock-free buffers for coordinating
/// writes to underlying storage.
impl IoBufs {
    pub(crate) fn start<R>(
        config: Config,
        mut snapshot: Snapshot<R>,
    ) -> Result<IoBufs, ()> {
        // open file for writing
        let file = config.file()?;

        let io_buf_size = config.io_buf_size;

        let snapshot_max_lsn = snapshot.max_lsn;
        let snapshot_last_lid = snapshot.last_lid;

        let (next_lsn, next_lid) = if snapshot_max_lsn
            < SEG_HEADER_LEN as Lsn
        {
            snapshot.max_lsn = 0;
            snapshot.last_lid = 0;
            (0, 0)
        } else {
            match file.read_message(snapshot_last_lid, &config) {
                Ok(LogRead::Inline(_lsn, _buf, len)) => (
                    snapshot_max_lsn
                        + len as Lsn
                        + MSG_HEADER_LEN as Lsn,
                    snapshot_last_lid
                        + len as LogId
                        + MSG_HEADER_LEN as LogId,
                ),
                Ok(LogRead::Blob(_lsn, _buf, _blob_ptr)) => (
                    snapshot_max_lsn
                        + BLOB_INLINE_LEN as Lsn
                        + MSG_HEADER_LEN as Lsn,
                    snapshot_last_lid
                        + BLOB_INLINE_LEN as LogId
                        + MSG_HEADER_LEN as LogId,
                ),
                other => {
                    // we can overwrite this non-flush
                    debug!(
                            "got non-flush tip while recovering at {}: {:?}",
                            snapshot_last_lid,
                            other
                        );
                    (snapshot_max_lsn, snapshot_last_lid)
                }
            }
        };

        let mut segment_accountant =
            SegmentAccountant::start(config.clone(), snapshot)?;

        let bufs =
            rep_no_copy![IoBuf::new(io_buf_size); config.io_bufs];

        let current_buf = 0;

        trace!(
            "starting IoBufs with next_lsn: {} \
             next_lid: {}",
            next_lsn,
            next_lid
        );

        if next_lsn == 0 {
            // recovering at segment boundary
            assert_eq!(next_lid, next_lsn as LogId);
            let iobuf = &bufs[current_buf];
            let lid = segment_accountant.next(next_lsn)?;

            iobuf.set_lid(lid);
            iobuf.set_capacity(io_buf_size - SEG_TRAILER_LEN);
            iobuf.store_segment_header(0, next_lsn);

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
            let iobuf = &bufs[current_buf];
            let offset = next_lid % io_buf_size as LogId;
            iobuf.set_lid(next_lid);
            iobuf.set_capacity(
                io_buf_size - offset as usize - SEG_TRAILER_LEN,
            );
            iobuf.set_lsn(next_lsn);

            debug!(
                "starting log at split offset {}, recovered lsn {}",
                next_lid, next_lsn
            );
        }

        // we want stable to begin at -1, since the 0th byte
        // of our file has not yet been written.
        let stable = if next_lsn == 0 { -1 } else { next_lsn - 1 };

        // remove all blob files larger than our stable offset
        gc_blobs(&config, stable)?;

        Ok(IoBufs {
            config: config,

            buf_mu: Mutex::new(()),
            buf_updated: Condvar::new(),
            bufs: bufs,
            current_buf: AtomicUsize::new(current_buf),
            written_bufs: AtomicUsize::new(0),

            intervals: Mutex::new(vec![]),
            interval_updated: Condvar::new(),

            stable_lsn: AtomicLsn::new(stable as InnerLsn),
            max_reserved_lsn: AtomicLsn::new(stable as InnerLsn),
            segment_accountant: Arc::new(Mutex::new(
                segment_accountant,
            )),

            #[cfg(feature = "failpoints")]
            _failpoint_crashing: AtomicBool::new(false),
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

    /// SegmentAccountant access for coordination with the `PageCache`,
    /// performed after all threads have exited the currently checked-in
    /// epochs using a crossbeam-epoch EBR guard.
    ///
    /// IMPORTANT: Never call this function with anything that calls
    /// defer on the same thread, as if a guard drops inside, it could
    /// trigger the below mutex lock, causing a deadlock.
    unsafe fn with_sa_deferred<F>(&self, f: F)
    where
        F: FnOnce(&mut SegmentAccountant) + Send + 'static,
    {
        let guard = pin();
        let segment_accountant = self.segment_accountant.clone();

        guard.defer(move || {
            let start = clock();

            debug_delay();
            let mut sa = segment_accountant.lock().unwrap();

            let locked_at = clock();

            M.accountant_lock.measure(locked_at - start);

            let _ = f(&mut sa);

            drop(sa);

            M.accountant_hold.measure(clock() - locked_at);
        });

        guard.flush();
    }

    fn idx(&self) -> usize {
        debug_delay();
        let current_buf = self.current_buf.load(SeqCst);
        current_buf % self.config.io_bufs
    }

    /// Returns the last stable offset in storage.
    pub(super) fn stable(&self) -> Lsn {
        debug_delay();
        self.stable_lsn.load(SeqCst) as Lsn
    }

    // Adds a header to the front of the buffer
    fn encapsulate(
        &self,
        raw_buf: Vec<u8>,
        lsn: Lsn,
        over_blob_threshold: bool,
        is_blob_rewrite: bool,
    ) -> Result<Vec<u8>, ()> {
        let buf = if over_blob_threshold {
            // write blob to file
            io_fail!(self, "blob blob write");
            write_blob(&self.config, lsn, raw_buf)?;

            let lsn_buf: [u8; size_of::<BlobPointer>()] =
                u64_to_arr(lsn as u64);

            lsn_buf.to_vec()
        } else {
            raw_buf
        };

        let crc16 = crc16_arr(&buf);

        let header = MessageHeader {
            kind: if over_blob_threshold || is_blob_rewrite {
                MessageKind::Blob
            } else {
                MessageKind::Inline
            },
            lsn: lsn,
            len: buf.len(),
            crc16: crc16,
        };

        let header_bytes: [u8; MSG_HEADER_LEN] = header.into();

        let mut out = vec![0; MSG_HEADER_LEN + buf.len()];
        out[0..MSG_HEADER_LEN].copy_from_slice(&header_bytes);
        out[MSG_HEADER_LEN..].copy_from_slice(&*buf);
        Ok(out)
    }

    /// Tries to claim a reservation for writing a buffer to a
    /// particular location in stable storge, which may either be
    /// completed or aborted later. Useful for maintaining
    /// linearizability across CAS operations that may need to
    /// persist part of their operation.
    ///
    /// # Panics
    ///
    /// Panics if the desired reservation is greater than the
    /// io buffer size minus the size of a segment header +
    /// a segment footer + a message header.
    pub(super) fn reserve(
        &self,
        raw_buf: Vec<u8>,
    ) -> Result<Reservation<'_>, ()> {
        self.reserve_inner(raw_buf, false)
    }

    /// Reserve a replacement buffer for a previously written
    /// blob write. This ensures the message header has the
    /// proper blob flag set.
    pub(super) fn reserve_blob(
        &self,
        blob_ptr: BlobPointer,
    ) -> Result<Reservation<'_>, ()> {
        let lsn_buf: [u8; size_of::<BlobPointer>()] =
            u64_to_arr(blob_ptr as u64);

        self.reserve_inner(lsn_buf.to_vec(), true)
    }

    fn reserve_inner(
        &self,
        raw_buf: Vec<u8>,
        is_blob_rewrite: bool,
    ) -> Result<Reservation<'_>, ()> {
        let _measure = Measure::new(&M.reserve);

        let io_bufs = self.config.io_bufs;

        // right shift 32 on 32-bit pointer systems panics
        #[cfg(target_pointer_width = "64")]
        assert_eq!((raw_buf.len() + MSG_HEADER_LEN) >> 32, 0);

        #[cfg(feature = "zstd")]
        let buf = if self.config.use_compression {
            let _measure = Measure::new(&M.compress);
            compress(&*raw_buf, self.config.zstd_compression_factor)
                .unwrap()
        } else {
            raw_buf
        };

        #[cfg(not(feature = "zstd"))]
        let buf = raw_buf;

        let total_buf_len = MSG_HEADER_LEN + buf.len();

        let max_overhead =
            std::cmp::max(SEG_HEADER_LEN, SEG_TRAILER_LEN);

        let max_buf_size = (self.config.io_buf_size
            / MINIMUM_ITEMS_PER_SEGMENT)
            - max_overhead;

        let over_blob_threshold = total_buf_len > max_buf_size;

        let inline_buf_len = if over_blob_threshold {
            MSG_HEADER_LEN + size_of::<Lsn>()
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
                }};
        }
        let mut spins = 0;
        loop {
            M.log_reservation_attempted();
            #[cfg(feature = "failpoints")]
            {
                if self._failpoint_crashing.load(Relaxed) {
                    return Err(Error::FailPoint);
                }
            }

            let guard = pin();
            debug_delay();
            let written_bufs = self.written_bufs.load(SeqCst);
            debug_delay();
            let current_buf = self.current_buf.load(SeqCst);
            let idx = current_buf % io_bufs;

            spins += 1;
            if spins > 1_000_000 {
                debug!(
                    "stalling in reserve, idx {}, buf len {}",
                    idx, inline_buf_len,
                );
                spins = 0;
            }

            if written_bufs > current_buf {
                // This can happen because a reservation can finish up
                // before the sealing thread gets around to bumping
                // current_buf.
                trace_once!("written ahead of sealed, spinning");
                spin_loop_hint();
                continue;
            }

            if current_buf - written_bufs >= io_bufs {
                // if written is too far behind, we need to
                // spin while it catches up to avoid overlap
                trace_once!(
                    "old io buffer not written yet, spinning"
                );
                spin_loop_hint();

                // use a condition variable to wait until
                // we've updated the written_bufs counter.
                let _measure =
                    Measure::new(&M.reserve_written_condvar_wait);
                let mut buf_mu = self.buf_mu.lock().unwrap();
                while written_bufs == self.written_bufs.load(SeqCst) {
                    buf_mu = self.buf_updated.wait(buf_mu).unwrap();
                }
                continue;
            }

            // load current header value
            let iobuf = &self.bufs[idx];
            let header = iobuf.get_header();

            // skip if already sealed
            if is_sealed(header) {
                // already sealed, start over and hope cur
                // has already been bumped by sealer.
                trace_once!("io buffer already sealed, spinning");
                spin_loop_hint();

                // use a condition variable to wait until
                // we've updated the current_buf counter.
                let _measure =
                    Measure::new(&M.reserve_current_condvar_wait);
                let mut buf_mu = self.buf_mu.lock().unwrap();
                while current_buf == self.current_buf.load(SeqCst) {
                    buf_mu = self.buf_updated.wait(buf_mu).unwrap();
                }
                continue;
            }

            // try to claim space
            let buf_offset = offset(header);
            let prospective_size =
                buf_offset as usize + inline_buf_len;
            let would_overflow =
                prospective_size > iobuf.get_capacity();
            if would_overflow {
                // This buffer is too full to accept our write!
                // Try to seal the buffer, and maybe write it if
                // there are zero writers.
                trace_once!("io buffer too full, spinning");
                self.maybe_seal_and_write_iobuf(idx, header, true)?;
                spin_loop_hint();
                continue;
            }

            // attempt to claim by incrementing an unsealed header
            let bumped_offset =
                bump_offset(header, inline_buf_len as Header);

            // check for maxed out IO buffer writers
            if n_writers(bumped_offset) == MAX_WRITERS {
                trace_once!(
                    "spinning because our buffer has {} writers already",
                    MAX_WRITERS
                );
                spin_loop_hint();
                continue;
            }

            let claimed = incr_writers(bumped_offset);
            assert!(!is_sealed(claimed));

            if iobuf.cas_header(header, claimed).is_err() {
                // CAS failed, start over
                trace_once!(
                    "CAS failed while claiming buffer slot, spinning"
                );
                spin_loop_hint();
                continue;
            }

            // if we're giving out a reservation,
            // the writer count should be positive
            assert_ne!(n_writers(claimed), 0);

            let lid = iobuf.get_lid();
            assert_ne!(
                lid as usize,
                std::usize::MAX,
                "fucked up on idx {}\n{:?}",
                idx,
                self
            );

            let out_buf =
                unsafe { (*iobuf.buf.get()).as_mut_slice() };

            let res_start = buf_offset as usize;
            let res_end = res_start + inline_buf_len;
            let destination = &mut (out_buf)[res_start..res_end];

            let reservation_offset = lid + u64::from(buf_offset);
            let reservation_lsn =
                iobuf.get_lsn() + u64::from(buf_offset) as Lsn;

            trace!(
                "reserved {} bytes at lsn {} lid {}",
                inline_buf_len,
                reservation_lsn,
                reservation_offset,
            );

            self.bump_max_reserved_lsn(reservation_lsn);

            assert!(!(over_blob_threshold && is_blob_rewrite));

            let encapsulated_buf = self.encapsulate(
                buf,
                reservation_lsn,
                over_blob_threshold,
                is_blob_rewrite,
            )?;

            M.log_reservation_success();

            return Ok(Reservation {
                idx: idx,
                iobufs: self,
                data: encapsulated_buf,
                destination: destination,
                flushed: false,
                lsn: reservation_lsn,
                lid: reservation_offset,
                is_blob: over_blob_threshold || is_blob_rewrite,
                _guard: guard,
            });
        }
    }

    /// Called by Reservation on termination (completion or abort).
    /// Handles departure from shared state, and possibly writing
    /// the buffer to stable storage if necessary.
    pub(super) fn exit_reservation(
        &self,
        idx: usize,
    ) -> Result<(), ()> {
        let iobuf = &self.bufs[idx];
        let mut header = iobuf.get_header();

        // Decrement writer count, retrying until successful.
        let mut spins = 0;
        loop {
            spins += 1;
            if spins > 10 {
                debug!("have spun >10x in decr");
                spins = 0;
            }

            let new_hv = decr_writers(header);
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

        // Succeeded in decrementing writers, if we decremented writers
        // to 0 and it's sealed then we should write it to storage.
        if n_writers(header) == 0 && is_sealed(header) {
            trace!("exiting idx {} from res", idx);
            self.write_to_log(idx)
        } else {
            Ok(())
        }
    }

    /// blocks until the specified log sequence number has
    /// been made stable on disk
    pub(crate) fn make_stable(&self, lsn: Lsn) -> Result<(), ()> {
        let _measure = Measure::new(&M.make_stable);

        // NB before we write the 0th byte of the file, stable  is -1
        while self.stable() < lsn {
            let idx = self.idx();
            let header = self.bufs[idx].get_header();
            if offset(header) == 0 || is_sealed(header) {
                // nothing to write, don't bother sealing
                // current IO buffer.
            } else {
                self.maybe_seal_and_write_iobuf(idx, header, false)?;
                continue;
            }

            // block until another thread updates the stable lsn
            let waiter = self.intervals.lock().unwrap();

            if self.stable() < lsn {
                #[cfg(feature = "failpoints")]
                {
                    if self._failpoint_crashing.load(SeqCst) {
                        return Err(Error::FailPoint);
                    }
                }
                trace!(
                    "waiting on cond var for make_stable({})",
                    lsn
                );

                let _waiter =
                    self.interval_updated.wait(waiter).unwrap();
            } else {
                trace!("make_stable({}) returning", lsn);
                break;
            }
        }

        Ok(())
    }

    /// Called by users who wish to force the current buffer
    /// to flush some pending writes.
    pub(super) fn flush(&self) -> Result<(), ()> {
        let max_reserved_lsn =
            self.max_reserved_lsn.load(SeqCst) as Lsn;
        self.make_stable(max_reserved_lsn)
    }

    // ensure self.max_reserved_lsn is set to this Lsn
    // or greater, for use in correct calls to flush.
    fn bump_max_reserved_lsn(&self, lsn: Lsn) {
        let mut current =
            self.max_reserved_lsn.load(SeqCst) as InnerLsn;
        loop {
            if current >= lsn as InnerLsn {
                return;
            }
            let last = self.max_reserved_lsn.compare_and_swap(
                current,
                lsn as InnerLsn,
                SeqCst,
            );
            if last == current {
                // we succeeded.
                return;
            }
            current = last;
        }
    }

    // Attempt to seal the current IO buffer, possibly
    // writing it to disk if there are no other writers
    // operating on it.
    fn maybe_seal_and_write_iobuf(
        &self,
        idx: usize,
        header: Header,
        from_reserve: bool,
    ) -> Result<(), ()> {
        let iobuf = &self.bufs[idx];

        if is_sealed(header) {
            // this buffer is already sealed. nothing to do here.
            return Ok(());
        }

        // NB need to do this before CAS because it can get
        // written and reset by another thread afterward
        let lid = iobuf.get_lid();
        let lsn = iobuf.get_lsn();
        let capacity = iobuf.get_capacity();
        let io_buf_size = self.config.io_buf_size;

        if offset(header) as usize > capacity {
            // a race happened, nothing we can do
            return Ok(());
        }

        let sealed = mk_sealed(header);
        let res_len = offset(sealed) as usize;

        let maxed = res_len == capacity;

        let worked = iobuf.linearized(|| {
            if iobuf.cas_header(header, sealed).is_err() {
                // cas failed, don't try to continue
                return false;
            }

            trace!("{} sealed", idx);

            if from_reserve || maxed {
                // NB we linearize this together with sealing
                // the header here to guarantee that in write_to_log,
                // which may be executing as soon as the seal is set
                // by another thread, the thread that calls
                // iobuf.get_maxed() is linearized with this one!
                trace!("setting maxed to true for idx {}", idx);
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

        let max = std::usize::MAX as LogId;

        assert_ne!(
            lid, max,
            "sealing something that should never have \
             been claimed (idx {})\n{:?}",
            idx, self
        );

        // open new slot
        let mut next_lsn = lsn;

        let next_offset = if from_reserve || maxed {
            // roll lsn to the next offset
            let lsn_idx = lsn / io_buf_size as Lsn;
            next_lsn = (lsn_idx + 1) * io_buf_size as Lsn;

            // mark unused as clear
            debug!(
                "rolling to new segment after clearing {}-{}",
                lid,
                lid + res_len as LogId,
            );

            let ret = self.with_sa(|sa| sa.next(next_lsn));
            #[cfg(feature = "failpoints")]
            {
                if let Err(Error::FailPoint) = ret {
                    self._failpoint_crashing.store(true, SeqCst);
                    // wake up any waiting threads so they don't stall forever
                    self.interval_updated.notify_all();
                }
            }
            ret?
        } else {
            debug!(
                "advancing offset within the current segment from {} to {}",
                lid,
                lid + res_len as LogId
            );
            next_lsn += res_len as Lsn;

            let next_offset = lid + res_len as LogId;
            next_offset
        };

        let next_idx = (idx + 1) % self.config.io_bufs;
        let next_iobuf = &self.bufs[next_idx];

        // NB we spin on this CAS because the next iobuf may not actually
        // be written to disk yet! (we've lapped the writer in the iobuf
        // ring buffer)
        let mut spins = 0;
        while next_iobuf.cas_lid(max, next_offset).is_err() {
            spins += 1;
            if spins > 1_000_000 {
                debug!(
                    "have spun >1,000,000x in seal of buf {}",
                    idx
                );
                spins = 0;
            }
            #[cfg(feature = "failpoints")]
            {
                if self._failpoint_crashing.load(Relaxed) {
                    // panic!("propagating failpoint");
                    return Err(Error::FailPoint);
                }
            }
            spin_loop_hint();
        }
        trace!("{} log set to {}", next_idx, next_offset);

        // NB as soon as the "sealed" bit is 0, this allows new threads
        // to start writing into this buffer, so do that after it's all
        // set up. expect this thread to block until the buffer completes
        // its entire lifecycle as soon as we do that.
        if from_reserve || maxed {
            next_iobuf.set_capacity(io_buf_size - SEG_TRAILER_LEN);
            next_iobuf.store_segment_header(sealed, next_lsn);
        } else {
            let new_cap = capacity - res_len;
            assert_ne!(new_cap, 0);
            next_iobuf.set_capacity(new_cap);
            next_iobuf.set_lsn(next_lsn);
            let last_salt = salt(sealed);
            let new_salt = bump_salt(last_salt);
            next_iobuf.set_header(new_salt);
        }

        trace!("{} zeroed header", next_idx);

        // we acquire this mutex to guarantee that any threads that
        // are going to wait on the condition variable will observe
        // the change.
        debug_delay();
        let _ = self.buf_mu.lock().unwrap();

        // communicate to other threads that we have advanced an IO buffer.
        debug_delay();
        let _current_buf = self.current_buf.fetch_add(1, SeqCst) + 1;
        trace!("{} current_buf", _current_buf % self.config.io_bufs);

        // let any threads that are blocked on buf_mu know about the
        // updated counter.
        debug_delay();
        self.buf_updated.notify_all();

        // if writers is 0, it's our responsibility to write the buffer.
        if n_writers(sealed) == 0 {
            trace!("writing to log from maybe_seal");
            self.write_to_log(idx)
        } else {
            Ok(())
        }
    }

    // Write an IO buffer's data to stable storage and set up the
    // next IO buffer for writing.
    fn write_to_log(&self, idx: usize) -> Result<(), ()> {
        let _measure = Measure::new(&M.write_to_log);
        let iobuf = &self.bufs[idx];
        let header = iobuf.get_header();
        let lid = iobuf.get_lid();
        let base_lsn = iobuf.get_lsn();
        let capacity = iobuf.get_capacity();

        let io_buf_size = self.config.io_buf_size;

        assert_eq!(
            (lid % io_buf_size as LogId) as Lsn,
            base_lsn % io_buf_size as Lsn
        );

        assert_ne!(
            lid as usize,
            std::usize::MAX,
            "created reservation for uninitialized slot",
        );

        assert!(is_sealed(header));

        let res_len = offset(header) as usize;

        let maxed = iobuf.linearized(|| iobuf.get_maxed());
        let unused_space = capacity - res_len;
        let should_pad = unused_space >= MSG_HEADER_LEN;

        let total_len = if maxed && should_pad {
            let offset = offset(header) as usize;
            let data = unsafe { (*iobuf.buf.get()).as_mut_slice() };
            let len = capacity - offset - MSG_HEADER_LEN;

            // take the crc of the random bytes already after where we
            // would place our header.
            let padding_bytes = vec![EVIL_BYTE; len];
            let crc16 = crc16_arr(&*padding_bytes);

            let header = MessageHeader {
                kind: MessageKind::Pad,
                lsn: base_lsn + offset as Lsn,
                len: len,
                crc16: crc16,
            };

            let header_bytes: [u8; MSG_HEADER_LEN] = header.into();

            data[offset..offset + MSG_HEADER_LEN]
                .copy_from_slice(&header_bytes);
            data[offset + MSG_HEADER_LEN..capacity]
                .copy_from_slice(&*padding_bytes);

            capacity
        } else {
            res_len
        };

        let data = unsafe { (*iobuf.buf.get()).as_mut_slice() };

        let f = self.config.file()?;
        io_fail!(self, "buffer write");
        f.pwrite_all(&data[..total_len], lid)?;
        f.sync_all()?;
        io_fail!(self, "buffer write post");

        // write a trailer if we're maxed
        if maxed {
            let segment_lsn =
                base_lsn / io_buf_size as Lsn * io_buf_size as Lsn;
            let segment_lid =
                lid / io_buf_size as LogId * io_buf_size as LogId;

            let trailer_overhang =
                io_buf_size as Lsn - SEG_TRAILER_LEN as Lsn;

            let trailer_lid = segment_lid + trailer_overhang as LogId;
            let trailer_lsn = segment_lsn + trailer_overhang;

            let trailer = SegmentTrailer {
                lsn: trailer_lsn,
                ok: true,
            };

            let trailer_bytes: [u8; SEG_TRAILER_LEN] = trailer.into();

            io_fail!(self, "trailer write");
            f.pwrite_all(&trailer_bytes, trailer_lid)?;
            f.sync_all()?;
            io_fail!(self, "trailer write post");

            M.written_bytes.measure(SEG_TRAILER_LEN as f64);

            iobuf.set_maxed(false);

            debug!(
                "wrote trailer at lid {} for lsn {}",
                trailer_lid, trailer_lsn
            );

            // transition this segment into deplete-only mode now
            // that n_writers is 0, and all calls to mark_replace/link
            // happen before the reservation completes.
            trace!(
                "deactivating segment with lsn {} at idx {} with lid {}",
                segment_lsn,
                idx,
                lid
            );
            unsafe {
                self.with_sa_deferred(move |sa| {
                    trace!("EBR deactivating segment {} with lsn {} and lid {}", idx, segment_lsn, segment_lid);
                    if let Err(e) = sa.deactivate_segment(segment_lsn, segment_lid) {
                        error!("segment accountant failed to deactivate segment: {}", e);
                    }
                });
            }
        } else {
            trace!(
                "not deactivating segment with lsn {}",
                base_lsn / io_buf_size as Lsn * io_buf_size as Lsn
            );
        }

        if total_len > 0 || maxed {
            let complete_len = if maxed {
                let lsn_idx = base_lsn as usize / io_buf_size;
                let next_seg_beginning = (lsn_idx + 1) * io_buf_size;
                next_seg_beginning - base_lsn as usize
            } else {
                total_len
            };

            debug!(
                "wrote lsns {}-{} to disk at offsets {}-{} in buffer {}",
                base_lsn,
                base_lsn + total_len as Lsn - 1,
                lid,
                lid + total_len as LogId - 1,
                idx
            );
            self.mark_interval(base_lsn, complete_len);
        }

        M.written_bytes.measure(total_len as f64);

        // signal that this IO buffer is now uninitialized
        let max = std::usize::MAX as LogId;
        iobuf.set_lid(max);
        trace!("{} log <- MAX", idx);

        // we acquire this mutex to guarantee that any threads that
        // are going to wait on the condition variable will observe
        // the change.
        debug_delay();
        let _ = self.buf_mu.lock().unwrap();

        // communicate to other threads that we have written an IO buffer.
        debug_delay();
        let _written_bufs = self.written_bufs.fetch_add(1, SeqCst);
        trace!("{} written", _written_bufs % self.config.io_bufs);

        // let any threads that are blocked on buf_mu know about the
        // updated counter.
        debug_delay();
        self.buf_updated.notify_all();

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
        trace!("mark_interval({}, {})", whence, len);
        assert_ne!(
            len,
            0,
            "mark_interval called with a zero-length range, starting from {}",
            whence
        );
        let mut intervals = self.intervals.lock().unwrap();

        let interval = (whence, whence + len as Lsn - 1);

        intervals.push(interval);

        debug_assert!(
            intervals.len() < 1000,
            "intervals is getting crazy... {:?}",
            *intervals
        );

        // reverse sort
        intervals.sort_unstable_by(|a, b| b.cmp(a));

        let mut updated = false;

        let len_before = intervals.len();

        while let Some(&(low, high)) = intervals.last() {
            assert_ne!(low, high);
            let cur_stable = self.stable_lsn.load(SeqCst) as Lsn;
            assert!(
                low > cur_stable,
                "somehow, we marked offset {} stable while \
                 interval {}-{} had not yet been applied!",
                cur_stable,
                low,
                high
            );
            if cur_stable + 1 == low {
                let old =
                    self.stable_lsn.swap(high as InnerLsn, SeqCst)
                        as Lsn;
                assert_eq!(
                    old, cur_stable,
                    "concurrent stable offset modification detected"
                );
                debug!("new highest interval: {} - {}", low, high);
                intervals.pop();
                updated = true;
            } else {
                break;
            }
        }

        if len_before - intervals.len() > 100 {
            debug!(
                "large merge of {} intervals",
                len_before - intervals.len()
            );
        }

        if updated {
            self.interval_updated.notify_all();
        }
    }
}

impl Drop for IoBufs {
    fn drop(&mut self) {
        // don't do any more IO if we're simulating a crash
        #[cfg(feature = "failpoints")]
        {
            if self._failpoint_crashing.load(SeqCst) {
                return;
            }
        }

        if let Err(e) = self.flush() {
            error!("failed to flush from IoBufs::drop: {}", e);
        }

        if let Ok(f) = self.config.file() {
            f.sync_all().unwrap();
        }

        debug!("IoBufs dropped");
    }
}

impl Debug for IoBufs {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> std::result::Result<(), fmt::Error> {
        debug_delay();
        let current_buf = self.current_buf.load(SeqCst);
        debug_delay();
        let written_bufs = self.written_bufs.load(SeqCst);

        formatter.write_fmt(format_args!(
            "IoBufs {{ sealed: {}, written: {}, bufs: {:?} }}",
            current_buf, written_bufs, self.bufs
        ))
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
            self.get_lid(),
            n_writers(header),
            offset(header),
            is_sealed(header)
        ))
    }
}

impl IoBuf {
    fn new(buf_size: usize) -> IoBuf {
        IoBuf {
            buf: UnsafeCell::new(vec![0; buf_size]),
            header: AtomicUsize::new(0),
            lid: AtomicUsize::new(std::usize::MAX),
            lsn: AtomicUsize::new(0),
            capacity: AtomicUsize::new(0),
            maxed: AtomicBool::new(false),
            linearizer: Mutex::new(()),
        }
    }

    // use this for operations on an IoBuf that must be
    // linearized together, and can't fit in the header!
    fn linearized<F, B>(&self, f: F) -> B
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
    fn store_segment_header(&self, last: Header, lsn: Lsn) {
        debug!("storing lsn {} in beginning of buffer", lsn);
        assert!(
            self.get_capacity() >= SEG_HEADER_LEN + SEG_TRAILER_LEN
        );

        self.set_lsn(lsn);

        let header = SegmentHeader { lsn: lsn, ok: true };
        let header_bytes: [u8; SEG_HEADER_LEN] = header.into();

        unsafe {
            (*self.buf.get())[0..SEG_HEADER_LEN]
                .copy_from_slice(&header_bytes);
        }

        // ensure writes to the buffer land after our header.
        let last_salt = salt(last);
        let new_salt = bump_salt(last_salt);
        let bumped = bump_offset(new_salt, SEG_HEADER_LEN as Header);
        self.set_header(bumped);
    }

    fn set_capacity(&self, cap: usize) {
        debug_delay();
        self.capacity.store(cap, SeqCst);
    }

    fn get_capacity(&self) -> usize {
        debug_delay();
        self.capacity.load(SeqCst)
    }

    fn set_lsn(&self, lsn: Lsn) {
        debug_delay();
        self.lsn.store(lsn as usize, SeqCst);
    }

    fn set_maxed(&self, maxed: bool) {
        debug_delay();
        self.maxed.store(maxed, SeqCst);
    }

    fn get_maxed(&self) -> bool {
        debug_delay();
        self.maxed.load(SeqCst)
    }

    fn get_lsn(&self) -> Lsn {
        debug_delay();
        self.lsn.load(SeqCst) as Lsn
    }

    fn set_lid(&self, offset: LogId) {
        debug_delay();
        self.lid.store(offset as usize, SeqCst);
    }

    fn get_lid(&self) -> LogId {
        debug_delay();
        self.lid.load(SeqCst) as LogId
    }

    fn get_header(&self) -> Header {
        debug_delay();
        self.header.load(SeqCst) as Header
    }

    fn set_header(&self, new: Header) {
        debug_delay();
        self.header.store(new as usize, SeqCst);
    }

    fn cas_header(
        &self,
        old: Header,
        new: Header,
    ) -> std::result::Result<Header, Header> {
        debug_delay();
        let res = self.header.compare_and_swap(
            old as usize,
            new as usize,
            SeqCst,
        ) as Header;
        if res == old {
            Ok(new)
        } else {
            Err(res)
        }
    }

    fn cas_lid(
        &self,
        old: LogId,
        new: LogId,
    ) -> std::result::Result<LogId, LogId> {
        debug_delay();
        let res = self.lid.compare_and_swap(
            old as usize,
            new as usize,
            SeqCst,
        ) as LogId;
        if res == old {
            Ok(new)
        } else {
            Err(res)
        }
    }
}

#[cfg_attr(not(feature = "no_inline"), inline)]
fn is_sealed(v: Header) -> bool {
    v & 1 << 31 == 1 << 31
}

#[cfg_attr(not(feature = "no_inline"), inline)]
fn mk_sealed(v: Header) -> Header {
    v | 1 << 31
}

#[cfg_attr(not(feature = "no_inline"), inline)]
fn n_writers(v: Header) -> Header {
    v << 33 >> 57
}

#[cfg_attr(not(feature = "no_inline"), inline)]
fn incr_writers(v: Header) -> Header {
    assert_ne!(n_writers(v), MAX_WRITERS);
    v + (1 << 24)
}

#[cfg_attr(not(feature = "no_inline"), inline)]
fn decr_writers(v: Header) -> Header {
    assert_ne!(n_writers(v), 0);
    v - (1 << 24)
}

#[cfg_attr(not(feature = "no_inline"), inline)]
fn offset(v: Header) -> Header {
    v << 40 >> 40
}

#[cfg_attr(not(feature = "no_inline"), inline)]
fn bump_offset(v: Header, by: Header) -> Header {
    assert_eq!(by >> 24, 0);
    v + by
}

#[cfg_attr(not(feature = "no_inline"), inline)]
fn bump_salt(v: Header) -> Header {
    (v + (1 << 32)) & 0xFFFFFFFF00000000
}

#[cfg_attr(not(feature = "no_inline"), inline)]
fn salt(v: Header) -> Header {
    v >> 32 << 32
}
