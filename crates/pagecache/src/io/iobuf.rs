use std::sync::{Condvar, Mutex};
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize};
use std::sync::atomic::Ordering::SeqCst;

#[cfg(feature = "zstd")]
use zstd::block::compress;

use self::reader::LogReader;

use super::*;

type Header = u64;

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
    config: Config,
    bufs: Vec<IoBuf>,
    current_buf: AtomicUsize,
    written_bufs: AtomicUsize,
    // Pending intervals that have been written to stable storage, but may be
    // higher than the current value of `stable` due to interesting thread
    // interleavings.
    pub(super) intervals: Mutex<Vec<(Lsn, Lsn)>>,
    pub(super) interval_updated: Condvar,
    // The highest CONTIGUOUS log sequence number that has been written to
    // stable storage. This may be lower than the length of the underlying
    // file, and there may be buffers that have been written out-of-order
    // to stable storage due to interesting thread interleavings.
    stable: AtomicIsize,
    segment_accountant: Mutex<SegmentAccountant>,
}

/// `IoBufs` is a set of lock-free buffers for coordinating
/// writes to underlying storage.
impl IoBufs {
    pub fn start<R>(
        config: Config,
        mut snapshot: Snapshot<R>,
    ) -> CacheResult<IoBufs, ()> {
        // if configured, start env_logger and/or cpuprofiler
        global_init();

        // open file for writing
        let file = config.file()?;

        let io_buf_size = config.get_io_buf_size();

        let snapshot_max_lsn = snapshot.max_lsn;
        let snapshot_last_lid = snapshot.last_lid;

        let (next_lsn, next_lid) =
            if snapshot_max_lsn < SEG_HEADER_LEN as Lsn {
                snapshot.max_lsn = 0;
                snapshot.last_lid = 0;
                (0, 0)
            } else {
                match file.read_message(
                    snapshot_last_lid,
                    io_buf_size,
                    config.get_use_compression(),
                ) {
                    Ok(LogRead::Flush(_lsn, _buf, len)) => (
                        snapshot_max_lsn + len as Lsn +
                            MSG_HEADER_LEN as
                                Lsn,
                        snapshot_last_lid + len as LogID +
                            MSG_HEADER_LEN as
                                LogID,
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

        let bufs = rep_no_copy![IoBuf::new(io_buf_size); config.get_io_bufs()];

        let current_buf = 0;

        trace!(
            "starting IoBufs with next_lsn: {} \
               next_lid: {}",
            next_lsn,
            next_lid
        );

        if next_lsn == 0 {
            // recovering at segment boundary
            assert_eq!(next_lid, next_lsn as LogID);
            let iobuf = &bufs[current_buf];
            let (lid, last_given) = segment_accountant.next(next_lsn)?;

            iobuf.set_lid(lid);
            iobuf.set_capacity(io_buf_size - SEG_TRAILER_LEN);
            iobuf.store_segment_header(0, next_lsn, last_given);

            file.pwrite_all(&*vec![0; config.get_io_buf_size()], lid)?;
            file.sync_all()?;

            debug!(
                "starting log at clean offset {}, recovered lsn {}",
                next_lid,
                next_lsn
            );
        } else {
            // the tip offset is not completely full yet, reuse it
            let iobuf = &bufs[current_buf];
            let offset = next_lid % io_buf_size as LogID;
            iobuf.set_lid(next_lid);
            iobuf.set_capacity(io_buf_size - offset as usize - SEG_TRAILER_LEN);
            iobuf.set_lsn(next_lsn);

            debug!(
                "starting log at split offset {}, recovered lsn {}",
                next_lid,
                next_lsn
            );
        }

        // we want stable to begin at -1, since the 0th byte
        // of our file has not yet been written.
        let stable = if next_lsn == 0 { -1 } else { next_lsn - 1 };

        Ok(IoBufs {
            bufs: bufs,
            current_buf: AtomicUsize::new(current_buf),
            written_bufs: AtomicUsize::new(0),
            intervals: Mutex::new(vec![]),
            interval_updated: Condvar::new(),
            stable: AtomicIsize::new(stable),
            config: config,
            segment_accountant: Mutex::new(segment_accountant),
        })
    }

    /// SegmentAccountant access for coordination with the `PageCache`
    pub(super) fn with_sa<B, F>(&self, f: F) -> B
        where F: FnOnce(&mut SegmentAccountant) -> B
    {
        let start = clock();

        let mut sa = self.segment_accountant.lock().unwrap();

        let locked_at = clock();

        M.accountant_lock.measure(locked_at - start);

        let ret = f(&mut sa);

        M.accountant_hold.measure(clock() - locked_at);

        ret
    }

    fn idx(&self) -> usize {
        debug_delay();
        let current_buf = self.current_buf.load(SeqCst);
        current_buf % self.config.get_io_bufs()
    }

    /// Returns the last stable offset in storage.
    pub(super) fn stable(&self) -> Lsn {
        debug_delay();
        self.stable.load(SeqCst) as Lsn
    }

    // Adds a header to the buffer, and optionally compresses
    // the buffer.
    // NB the caller is responsible for later setting the Lsn
    // bytes after a reservation has been acquired.
    fn encapsulate(&self, raw_buf: Vec<u8>) -> Vec<u8> {
        #[cfg(feature = "zstd")]
        let buf = if self.config.get_use_compression() {
            let _measure = Measure::new(&M.compress);
            compress(&*raw_buf, self.config.get_zstd_compression_factor())
                .unwrap()
        } else {
            raw_buf
        };

        #[cfg(not(feature = "zstd"))]
        let buf = raw_buf;

        let crc16 = crc16_arr(&buf);

        let header = MessageHeader {
            successful_flush: true,
            lsn: 0,
            len: buf.len(),
            crc16: crc16,
        };

        let header_bytes: [u8; MSG_HEADER_LEN] = header.into();

        let mut out = vec![0; MSG_HEADER_LEN + buf.len()];
        out[0..MSG_HEADER_LEN].copy_from_slice(&header_bytes);
        out[MSG_HEADER_LEN..].copy_from_slice(&*buf);
        out
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
    ) -> CacheResult<Reservation, ()> {
        let _measure = Measure::new(&M.reserve);

        let io_bufs = self.config.get_io_bufs();

        assert_eq!((raw_buf.len() + MSG_HEADER_LEN) >> 32, 0);

        let buf = self.encapsulate(raw_buf);

        let segment_overhead = SEG_HEADER_LEN + SEG_TRAILER_LEN;
        let max_buf_size = (self.config.get_io_buf_size() - segment_overhead) /
            self.config.get_min_items_per_segment();

        assert!(
            buf.len() <= max_buf_size,
            "trying to write a buffer that is too large \
            to be stored in the IO buffer. buf len: {} current max: {}. \
            a future version of pagecache will implement automatic \
            fragmentation of large values. feel free to open \
            an issue if this is a pressing need of yours.",
            buf.len(),
            max_buf_size
        );

        trace!("reserving buf of len {}", buf.len());

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
            debug_delay();
            let written_bufs = self.written_bufs.load(SeqCst);
            debug_delay();
            let current_buf = self.current_buf.load(SeqCst);
            let idx = current_buf % io_bufs;

            spins += 1;
            if spins > 1_000_000 {
                debug!("stalling in reserve, idx {}", idx);
                spins = 0;
            }

            if written_bufs > current_buf {
                // This can happen because a reservation can finish up
                // before the sealing thread gets around to bumping
                // current_buf.
                trace_once!("written ahead of sealed, spinning");
                M.log_looped();
                yield_now();
                continue;
            }

            if current_buf - written_bufs >= io_bufs {
                // if written is too far behind, we need to
                // spin while it catches up to avoid overlap
                trace_once!("old io buffer not written yet, spinning");
                M.log_looped();
                yield_now();
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
                M.log_looped();
                yield_now();
                continue;
            }

            // try to claim space
            let buf_offset = offset(header);
            let prospective_size = buf_offset as usize + buf.len();
            let would_overflow = prospective_size > iobuf.get_capacity();
            if would_overflow {
                // This buffer is too full to accept our write!
                // Try to seal the buffer, and maybe write it if
                // there are zero writers.
                trace_once!("io buffer too full, spinning");
                self.maybe_seal_and_write_iobuf(idx, header, true)?;
                M.log_looped();
                yield_now();
                continue;
            }

            // attempt to claim by incrementing an unsealed header
            let bumped_offset = bump_offset(header, buf.len() as Header);
            let claimed = incr_writers(bumped_offset);
            assert!(!is_sealed(claimed));

            if iobuf.cas_header(header, claimed).is_err() {
                // CAS failed, start over
                trace_once!("CAS failed while claiming buffer slot, spinning");
                M.log_looped();
                yield_now();
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

            let out_buf = unsafe { (*iobuf.buf.get()).as_mut_slice() };

            let res_start = buf_offset as usize;
            let res_end = res_start + buf.len();
            let destination = &mut (out_buf)[res_start..res_end];

            let reservation_offset = lid + u64::from(buf_offset);
            let reservation_lsn = iobuf.get_lsn() +
                u64::from(buf_offset) as Lsn;

            // we assign the LSN now that we know what it is
            assert_eq!(&buf[1..9], &[0u8; 8]);
            let lsn_bytes: [u8; 8] =
                unsafe { std::mem::transmute(reservation_lsn) };
            let mut buf = buf;
            buf[1..9].copy_from_slice(&lsn_bytes);

            trace!(
                "reserved {} bytes at lsn {} lid {}",
                buf.len(),
                reservation_lsn,
                reservation_offset,
            );

            return Ok(Reservation {
                idx: idx,
                iobufs: self,
                data: buf,
                destination: destination,
                flushed: false,
                lsn: reservation_lsn,
                lid: reservation_offset,
            });
        }
    }

    /// Called by Reservation on termination (completion or abort).
    /// Handles departure from shared state, and possibly writing
    /// the buffer to stable storage if necessary.
    pub(super) fn exit_reservation(&self, idx: usize) -> CacheResult<(), ()> {
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

    /// Called by users who wish to force the current buffer
    /// to flush some pending writes. Useful when blocking on
    /// a particular offset to become stable. May need to
    /// be called multiple times. May not do anything
    /// if there is contention on the current IO buffer
    /// or no data to flush.
    pub(super) fn flush(&self) -> CacheResult<(), ()> {
        let idx = self.idx();
        let header = self.bufs[idx].get_header();
        if offset(header) == 0 || is_sealed(header) {
            // nothing to write, don't bother sealing
            // current IO buffer.
            return Ok(());
        }
        self.maybe_seal_and_write_iobuf(idx, header, false)
    }

    // Attempt to seal the current IO buffer, possibly
    // writing it to disk if there are no other writers
    // operating on it.
    fn maybe_seal_and_write_iobuf(
        &self,
        idx: usize,
        header: Header,
        from_reserve: bool,
    ) -> CacheResult<(), ()> {
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
        let io_buf_size = self.config.get_io_buf_size();

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

        let max = std::usize::MAX as LogID;

        assert_ne!(
            lid,
            max,
            "sealing something that should never have \
            been claimed (idx {})\n{:?}",
            idx,
            self
        );

        // open new slot
        let mut next_lsn = lsn;

        let (next_offset, last_given) = if from_reserve || maxed {
            // roll lsn to the next offset
            let lsn_idx = lsn / io_buf_size as Lsn;
            next_lsn = (lsn_idx + 1) * io_buf_size as Lsn;
            let segment_remainder = next_lsn - (lsn + res_len as Lsn);

            // mark unused as clear
            debug!(
                "rolling to new segment after clearing {}-{}",
                lid,
                lid + res_len as LogID,
            );

            // we want to just mark the part that won't get marked in
            // write_to_log, which is basically just the wasted tip here.
            let low_lsn = lsn + res_len as Lsn;
            self.mark_interval(low_lsn, segment_remainder as usize);

            self.with_sa(|sa| sa.next(next_lsn))?
        } else {
            debug!(
                "advancing offset within the current segment from {} to {}",
                lid,
                lid + res_len as LogID
            );
            next_lsn += res_len as Lsn;

            let next_offset = lid + res_len as LogID;
            (next_offset, 0)
        };

        let next_idx = (idx + 1) % self.config.get_io_bufs();
        let next_iobuf = &self.bufs[next_idx];

        // NB we spin on this CAS because the next iobuf may not actually
        // be written to disk yet! (we've lapped the writer in the iobuf
        // ring buffer)
        let mut spins = 0;
        while next_iobuf.cas_lid(max, next_offset).is_err() {
            spins += 1;
            if spins > 1_000_000 {
                debug!("have spun >1,000,000x in seal of buf {}", idx);
                spins = 0;
            }
            yield_now();
        }
        trace!("{} log set to {}", next_idx, next_offset);

        // NB as soon as the "sealed" bit is 0, this allows new threads
        // to start writing into this buffer, so do that after it's all
        // set up. expect this thread to block until the buffer completes
        // its entire lifecycle as soon as we do that.
        if from_reserve || maxed {
            next_iobuf.set_capacity(io_buf_size - SEG_TRAILER_LEN);
            next_iobuf.store_segment_header(sealed, next_lsn, last_given);
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

        debug_delay();
        let _current_buf = self.current_buf.fetch_add(1, SeqCst) + 1;
        trace!("{} current_buf", _current_buf % self.config.get_io_bufs());

        // if writers is 0, it's our responsibility to write the buffer.
        if n_writers(sealed) == 0 {
            self.write_to_log(idx)
        } else {
            Ok(())
        }
    }

    // Write an IO buffer's data to stable storage and set up the
    // next IO buffer for writing.
    fn write_to_log(&self, idx: usize) -> CacheResult<(), ()> {
        let _measure = Measure::new(&M.write_to_log);
        let iobuf = &self.bufs[idx];
        let header = iobuf.get_header();
        let lid = iobuf.get_lid();
        let base_lsn = iobuf.get_lsn();

        let io_buf_size = self.config.get_io_buf_size();

        assert_eq!(
            (lid % io_buf_size as LogID) as Lsn,
            base_lsn % io_buf_size as Lsn
        );

        assert_ne!(
            lid as usize,
            std::usize::MAX,
            "created reservation for uninitialized slot",
        );

        let res_len = offset(header) as usize;

        let data = unsafe { (*iobuf.buf.get()).as_mut_slice() };

        let f = self.config.file()?;
        f.pwrite_all(&data[..res_len], lid)?;
        f.sync_all()?;

        if res_len > 0 {
            debug!(
                "wrote lsns {}-{} to disk at offsets {}-{}",
                base_lsn,
                base_lsn + res_len as Lsn - 1,
                lid,
                lid + res_len as LogID - 1
            );
            self.mark_interval(base_lsn, res_len);
        }

        // write a trailer if we're maxed
        let maxed = iobuf.linearized(|| iobuf.get_maxed());
        if maxed {
            let segment_lsn = base_lsn / io_buf_size as Lsn *
                io_buf_size as Lsn;
            let segment_lid = lid / io_buf_size as LogID * io_buf_size as LogID;

            let trailer_overhang = io_buf_size as Lsn - SEG_TRAILER_LEN as Lsn;

            let trailer_lid = segment_lid + trailer_overhang as LogID;
            let trailer_lsn = segment_lsn + trailer_overhang;

            let trailer = SegmentTrailer {
                lsn: trailer_lsn,
                ok: true,
            };

            let trailer_bytes: [u8; SEG_TRAILER_LEN] = trailer.into();

            f.pwrite_all(&trailer_bytes, trailer_lid)?;
            f.sync_all()?;
            iobuf.set_maxed(false);

            debug!(
                "wrote trailer at lid {} for lsn {}",
                trailer_lid,
                trailer_lsn
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
            self.with_sa(|sa| sa.deactivate_segment(segment_lsn, segment_lid));
        } else {
            trace!(
                "not deactivating segment with lsn {}",
                base_lsn / io_buf_size as Lsn * io_buf_size as Lsn
            );
        }

        M.written_bytes.measure(res_len as f64);

        // signal that this IO buffer is now uninitialized
        let max = std::usize::MAX as LogID;
        iobuf.set_lid(max);
        trace!("{} log <- MAX", idx);

        // communicate to other threads that we have written an IO buffer.
        debug_delay();
        let _written_bufs = self.written_bufs.fetch_add(1, SeqCst);
        trace!("{} written", _written_bufs % self.config.get_io_bufs());

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
            let cur_stable = self.stable.load(SeqCst);
            // FIXME somehow, we marked offset 2233715 stable while interval 2231715-2231999 had
            // not yet been applied!
            assert!(
                low > cur_stable,
                "somehow, we marked offset {} stable while \
                interval {}-{} had not yet been applied!",
                cur_stable,
                low,
                high
            );
            if cur_stable + 1 == low {
                let old = self.stable.swap(high, SeqCst);
                assert_eq!(
                    old,
                    cur_stable,
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
            debug!("large merge of {} intervals", len_before - intervals.len());
        }

        if updated {
            self.interval_updated.notify_all();
        }
    }
}

impl Drop for IoBufs {
    fn drop(&mut self) {
        for _ in 0..self.config.get_io_bufs() {
            self.flush().unwrap();
        }
        if let Ok(f) = self.config.file() {
            f.sync_all().unwrap();
        }

        debug!("IoBufs dropped");
    }
}

impl Debug for IoBufs {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        debug_delay();
        let current_buf = self.current_buf.load(SeqCst);
        debug_delay();
        let written_bufs = self.written_bufs.load(SeqCst);

        formatter.write_fmt(format_args!(
            "IoBufs {{ sealed: {}, written: {}, bufs: {:?} }}",
            current_buf,
            written_bufs,
            self.bufs
        ))
    }
}

impl Debug for IoBuf {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
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
        where F: FnOnce() -> B
    {
        let _l = self.linearizer.lock().unwrap();
        f()
    }

    // This is called upon the initialization of a fresh segment.
    // We write a new segment header to the beginning of the buffer
    // for assistance during recovery. The caller is responsible
    // for ensuring that the IoBuf's capacity has been set properly.
    fn store_segment_header(&self, last: Header, lsn: Lsn, prev: LogID) {
        debug!("storing lsn {} in beginning of buffer", lsn);
        assert!(self.get_capacity() >= SEG_HEADER_LEN + SEG_TRAILER_LEN);

        self.set_lsn(lsn);

        let header = SegmentHeader {
            lsn: lsn,
            prev: prev,
            ok: true,
        };
        let header_bytes: [u8; SEG_HEADER_LEN] = header.into();

        unsafe {
            (*self.buf.get())[0..SEG_HEADER_LEN].copy_from_slice(&header_bytes);
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

    fn set_lid(&self, offset: LogID) {
        debug_delay();
        self.lid.store(offset as usize, SeqCst);
    }

    fn get_lid(&self) -> LogID {
        debug_delay();
        self.lid.load(SeqCst) as LogID
    }

    fn get_header(&self) -> Header {
        debug_delay();
        self.header.load(SeqCst) as Header
    }

    fn set_header(&self, new: Header) {
        debug_delay();
        self.header.store(new as usize, SeqCst);
    }

    fn cas_header(&self, old: Header, new: Header) -> Result<Header, Header> {
        debug_delay();
        let res = self.header.compare_and_swap(
            old as usize,
            new as usize,
            SeqCst,
        ) as Header;
        if res == old { Ok(new) } else { Err(res) }
    }

    fn cas_lid(&self, old: LogID, new: LogID) -> Result<LogID, LogID> {
        debug_delay();
        let res = self.lid.compare_and_swap(
            old as usize,
            new as usize,
            SeqCst,
        ) as LogID;
        if res == old { Ok(new) } else { Err(res) }
    }
}

#[inline(always)]
fn is_sealed(v: Header) -> bool {
    v & 1 << 31 == 1 << 31
}

#[inline(always)]
fn mk_sealed(v: Header) -> Header {
    v | 1 << 31
}

#[inline(always)]
fn n_writers(v: Header) -> Header {
    v << 33 >> 57
}

#[inline(always)]
fn incr_writers(v: Header) -> Header {
    assert_ne!(n_writers(v), 127);
    v + (1 << 24)
}

#[inline(always)]
fn decr_writers(v: Header) -> Header {
    assert_ne!(n_writers(v), 0);
    v - (1 << 24)
}

#[inline(always)]
fn offset(v: Header) -> Header {
    v << 40 >> 40
}

#[inline(always)]
fn bump_offset(v: Header, by: Header) -> Header {
    assert_eq!(by >> 24, 0);
    v + by
}

#[inline(always)]
fn bump_salt(v: Header) -> Header {
    (v + (1 << 32)) & 0xFFFFFFFF00000000
}

#[inline(always)]
fn salt(v: Header) -> Header {
    v >> 32 << 32
}

#[inline(always)]
fn yield_now() {
    #[cfg(nightly)]
    {
        std::sync::atomic::hint_core_should_pause();
    }

    #[cfg(not(nightly))]
    {
        std::thread::yield_now();
    }
}
