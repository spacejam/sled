use std::io::{Seek, Write};
use std::path::Path;
use std::sync::{Condvar, Mutex};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering::SeqCst;

#[cfg(feature = "rayon")]
use rayon::prelude::*;

#[cfg(feature = "zstd")]
use zstd::block::compress;

use super::*;

struct IoBuf {
    buf: UnsafeCell<Vec<u8>>,
    header: AtomicUsize,
    lid: AtomicUsize,
    lsn: AtomicUsize,
    capacity: AtomicUsize,
    maxed: AtomicBool,
}

unsafe impl Sync for IoBuf {}

pub(super) struct IoBufs {
    config: Arc<Config>,
    bufs: Vec<IoBuf>,
    current_buf: AtomicUsize,
    written_bufs: AtomicUsize,
    // Pending intervals that have been written to stable storage, but may be
    // higher than the current value of `stable` due to interesting thread
    // interleavings.
    pub(super) intervals: Mutex<Vec<(LogID, LogID)>>,
    pub(super) interval_updated: Condvar,
    // The highest CONTIGUOUS log sequence number that has been written to
    // stable storage. This may be lower than the length of the underlying
    // file, and there may be buffers that have been written out-of-order
    // to stable storage due to interesting thread interleavings.
    stable: AtomicUsize,
    file_for_writing: Mutex<std::fs::File>,
    segment_accountant: Mutex<SegmentAccountant>,
}

/// `IoBufs` is a set of lock-free buffers for coordinating
/// writes to underlying storage.
impl IoBufs {
    pub fn start(config: Arc<Config>, segments: Vec<Segment>) -> IoBufs {
        let path = config.get_path();

        let dir = Path::new(&path).parent().expect(
            "could not parse provided path",
        );

        if dir != Path::new("") {
            if dir.is_file() {
                panic!(
                    "provided parent directory is a file, \
                    not a directory: {:?}",
                    dir
                );
            }

            if !dir.exists() {
                std::fs::create_dir_all(dir).unwrap();
            }
        }

        let io_buf_size = config.get_io_buf_size();

        let mut segment_accountant =
            SegmentAccountant::start(config.clone(), segments);

        let bufs = rep_no_copy![IoBuf::new(io_buf_size); config.get_io_bufs()];

        let current_buf = 0;
        let recovered_lsn = segment_accountant.recovered_lsn();
        let recovered_lid = segment_accountant.recovered_lid();

        // open file for writing
        let mut options = std::fs::OpenOptions::new();
        options.create(true);
        options.write(true);
        let mut file = options.open(&path).unwrap();

        trace!(
            "starting IoBufs with recovered_lsn: {} \
               recovered_lid: {}",
            recovered_lsn,
            recovered_lid
        );

        if recovered_lid % io_buf_size as LogID == 0 {
            // clean offset, need to create a new one and initialize it
            let iobuf = &bufs[current_buf];
            let (lid, last_given) = segment_accountant.next(recovered_lsn);
            iobuf.set_lid(lid);
            iobuf.set_capacity(io_buf_size - SEG_TRAILER_LEN);
            iobuf.store_segment_header(recovered_lsn, last_given);

            file.seek(SeekFrom::Start(lid)).unwrap();
            file.write_all(&*vec![0; config.get_io_buf_size()]).unwrap();
            file.sync_all().unwrap();

            debug!(
                "starting log at clean offset {}, recovered lsn {}",
                recovered_lid,
                recovered_lsn
            );
        } else {
            // the tip offset is not completely full yet, reuse it
            let iobuf = &bufs[current_buf];
            let offset = recovered_lid % io_buf_size as LogID;
            iobuf.set_lid(recovered_lid);
            iobuf.set_capacity(io_buf_size - offset as usize - SEG_TRAILER_LEN);
            iobuf.set_lsn(recovered_lsn);

            debug!(
                "starting log at split offset {}, recovered lsn {}",
                recovered_lid,
                recovered_lsn
            );
        }

        IoBufs {
            bufs: bufs,
            current_buf: AtomicUsize::new(current_buf),
            written_bufs: AtomicUsize::new(0),
            intervals: Mutex::new(vec![]),
            interval_updated: Condvar::new(),
            stable: AtomicUsize::new(recovered_lsn as usize),
            config: config,
            file_for_writing: Mutex::new(file),
            segment_accountant: Mutex::new(segment_accountant),
        }
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
            let start = clock();
            let res = compress(&*raw_buf, 5).unwrap();
            M.compress.measure(clock() - start);
            res
        } else {
            raw_buf
        };

        #[cfg(not(feature = "zstd"))]
        let buf = raw_buf;

        let crc16 = crc16_arr(&buf);

        let header = MessageHeader {
            valid: true,
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
    pub(super) fn reserve(&self, raw_buf: Vec<u8>) -> Reservation {
        let start = clock();

        assert_eq!((raw_buf.len() + MSG_HEADER_LEN) >> 32, 0);

        let buf = self.encapsulate(raw_buf);

        assert!(
            buf.len() <=
                self.config.get_io_buf_size() -
                    (SEG_HEADER_LEN + SEG_TRAILER_LEN),
            "trying to write a buffer that is too large \
            to be stored in the IO buffer."
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
            let idx = current_buf % self.config.get_io_bufs();

            spins += 1;
            if spins > 1_000_000 {
                debug!("{:?} stalling in reserve, idx {}", tn(), idx);
                spins = 0;
            }

            if written_bufs > current_buf {
                // This can happen because a reservation can finish up
                // before the sealing thread gets around to bumping
                // current_buf.
                trace_once!("({:?}) written ahead of sealed, spinning", tn());
                M.log_looped();
                yield_now();
                continue;
            }

            if current_buf - written_bufs >= self.config.get_io_bufs() {
                // if written is too far behind, we need to
                // spin while it catches up to avoid overlap
                trace_once!(
                    "({:?}) old io buffer not written yet, spinning",
                    tn()
                );
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
                trace_once!("({:?}) io buffer already sealed, spinning", tn());
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
                self.maybe_seal_and_write_iobuf(idx, header, true);
                trace_once!("({:?}) io buffer too full, spinning", tn());
                M.log_looped();
                yield_now();
                continue;
            }

            // attempt to claim by incrementing an unsealed header
            let bumped_offset = bump_offset(header, buf.len() as u32);
            let claimed = incr_writers(bumped_offset);
            assert!(!is_sealed(claimed));

            if iobuf.cas_header(header, claimed).is_err() {
                // CAS failed, start over
                trace_once!(
                    "({:?}) CAS failed while claiming buffer slot, spinning",
                    tn()
                );
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
                "({:?}) fucked up on idx {}\n{:?}",
                tn(),
                idx,
                self
            );

            let out_buf = unsafe { (*iobuf.buf.get()).as_mut_slice() };

            let res_start = buf_offset as usize;
            let res_end = res_start + buf.len();
            let destination = &mut (out_buf)[res_start..res_end];

            let reservation_offset = lid + u64::from(buf_offset);
            let reservation_lsn = iobuf.get_lsn() + u64::from(buf_offset);

            // we assign the LSN now that we know what it is
            assert_eq!(&buf[1..9], &[0u8; 8]);
            let lsn_bytes: [u8; 8] =
                unsafe { std::mem::transmute(reservation_lsn) };
            let mut buf = buf;
            buf[1..9].copy_from_slice(&lsn_bytes);

            M.reserve.measure(clock() - start);

            trace!(
                "reserved {} bytes at lsn {} lid {}",
                buf.len(),
                reservation_lsn,
                reservation_offset,
            );

            return Reservation {
                idx: idx,
                iobufs: self,
                data: buf,
                destination: destination,
                flushed: false,
                lsn: reservation_lsn,
                lid: reservation_offset,
            };
        }
    }

    /// Called by Reservation on termination (completion or abort).
    /// Handles departure from shared state, and possibly writing
    /// the buffer to stable storage if necessary.
    pub(super) fn exit_reservation(&self, idx: usize) {
        let iobuf = &self.bufs[idx];
        let mut header = iobuf.get_header();

        // Decrement writer count, retrying until successful.
        let mut spins = 0;
        loop {
            spins += 1;
            if spins > 10 {
                debug!("{:?} have spun >10x in decr", tn());
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
            self.write_to_log(idx);
        }
    }

    /// Called by users who wish to force the current buffer
    /// to flush some pending writes. Useful when blocking on
    /// a particular offset to become stable. May need to
    /// be called multiple times. May not do anything
    /// if there is contention on the current IO buffer
    /// or no data to flush.
    pub(super) fn flush(&self) {
        let idx = self.idx();
        let header = self.bufs[idx].get_header();
        if offset(header) == 0 || is_sealed(header) {
            // nothing to write, don't bother sealing
            // current IO buffer.
            return;
        }
        self.maybe_seal_and_write_iobuf(idx, header, false);
    }

    // Attempt to seal the current IO buffer, possibly
    // writing it to disk if there are no other writers
    // operating on it.
    fn maybe_seal_and_write_iobuf(
        &self,
        idx: usize,
        header: u32,
        from_reserve: bool,
    ) {
        let iobuf = &self.bufs[idx];

        if is_sealed(header) {
            // this buffer is already sealed. nothing to do here.
            return;
        }

        // NB need to do this before CAS because it can get
        // written and reset by another thread afterward
        let lid = iobuf.get_lid();
        let lsn = iobuf.get_lsn();
        let capacity = iobuf.get_capacity();
        let io_buf_size = self.config.get_io_buf_size();

        let sealed = mk_sealed(header);

        if iobuf.cas_header(header, sealed).is_err() {
            // cas failed, don't try to continue
            return;
        }
        trace!("({:?}) {} sealed", tn(), idx);

        // open new slot
        let res_len = offset(sealed) as usize;
        let max = std::usize::MAX as LogID;

        assert_ne!(
            lid,
            max,
            "({:?}) sealing something that should never have \
            been claimed (idx {})\n{:?}",
            tn(),
            idx,
            self
        );

        let mut next_lsn = lsn;

        let maxed = res_len == capacity;

        let (next_offset, last_given) = if from_reserve || maxed {
            // FIXME this isn't linearized with the thread that actually writes it
            // we will write a trailer to the iobuf after writing it.
            iobuf.set_maxed(true);

            // roll lsn to the next offset
            let segment_offset = next_lsn % io_buf_size as Lsn;
            let segment_remainder = io_buf_size as Lsn - segment_offset;
            next_lsn += segment_remainder;

            // mark unused as clear
            debug!(
                "rolling to new segment after clearing {}-{}",
                lid,
                lid + res_len as LogID,
            );

            if res_len != io_buf_size && res_len != segment_remainder as usize {
                // we want to just mark the part that won't get marked in
                // write_to_log, which is basically just the wasted tip here.
                let low_lsn = lsn + res_len as Lsn;
                self.mark_interval((low_lsn, next_lsn));
            }

            let (next_offset, last_given) =
                self.with_sa(|sa| sa.next(next_lsn));

            // TODO put this file writing logic into the SegmentAccountant
            // zero out the entire new segment on disk
            debug!("zeroing out segment beginning at {}", next_offset);
            let mut f = self.file_for_writing.lock().unwrap();
            f.seek(SeekFrom::Start(next_offset)).unwrap();
            f.write_all(&*vec![0; self.config.get_io_buf_size()])
                .unwrap();
            f.sync_all().unwrap();

            (next_offset, Some(last_given))
        } else {
            debug!(
                "advancing offset within the current segment from {} to {}",
                lid,
                lid + res_len as LogID
            );
            next_lsn += res_len as Lsn;

            let next_offset = lid + res_len as LogID;
            (next_offset, None)
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
        trace!("({:?}) {} log set", tn(), next_idx);

        // NB as soon as the "sealed" bit is 0, this allows new threads
        // to start writing into this buffer, so do that after it's all
        // set up. expect this thread to block until the buffer completes
        // its entire lifecycle as soon as we do that.
        if from_reserve || maxed {
            next_iobuf.set_capacity(io_buf_size - SEG_TRAILER_LEN);
            next_iobuf.store_segment_header(next_lsn, last_given.unwrap());
        } else {
            let new_cap = capacity - res_len;
            assert_ne!(new_cap, 0);
            next_iobuf.set_capacity(new_cap);
            next_iobuf.set_lsn(next_lsn);
            next_iobuf.set_header(0);
        }

        trace!("({:?}) {} zeroed header", tn(), next_idx);

        debug_delay();
        let _current_buf = self.current_buf.fetch_add(1, SeqCst) + 1;
        trace!(
            "({:?}) {} current_buf",
            tn(),
            _current_buf % self.config.get_io_bufs()
        );

        // if writers is 0, it's our responsibility to write the buffer.
        if n_writers(sealed) == 0 {
            self.write_to_log(idx);
        }
    }

    // Write an IO buffer's data to stable storage and set up the
    // next IO buffer for writing.
    fn write_to_log(&self, idx: usize) {
        let start = clock();
        let iobuf = &self.bufs[idx];
        let header = iobuf.get_header();
        let lid = iobuf.get_lid();
        let base_lsn = iobuf.get_lsn();

        let io_buf_size = self.config.get_io_buf_size();

        assert_eq!(lid % io_buf_size as LogID, base_lsn % io_buf_size as Lsn);

        assert_ne!(
            lid as usize,
            std::usize::MAX,
            "({:?}) created reservation for uninitialized slot",
            tn()
        );

        let res_len = offset(header) as usize;

        let data = unsafe { (*iobuf.buf.get()).as_mut_slice() };

        let mut f = self.file_for_writing.lock().unwrap();
        f.seek(SeekFrom::Start(lid)).unwrap();
        f.write_all(&data[..res_len]).unwrap();
        f.sync_all().unwrap();

        // write a trailer if we're maxed
        if iobuf.get_maxed() {
            let segment_lsn = base_lsn / io_buf_size as Lsn *
                io_buf_size as Lsn;
            let segment_lid = lid / io_buf_size as LogID * io_buf_size as LogID;

            let trailer_overhang = io_buf_size as Lsn - SEG_TRAILER_LEN as Lsn;

            let trailer_lid = segment_lid + trailer_overhang;
            let trailer_lsn = segment_lsn + trailer_overhang;

            let trailer = SegmentTrailer {
                lsn: trailer_lsn,
                ok: true,
            };

            let trailer_bytes: [u8; SEG_TRAILER_LEN] = trailer.into();

            trace!(
                "writing trailer at lid {} for lsn {}",
                trailer_lid,
                trailer_lsn
            );

            f.seek(SeekFrom::Start(trailer_lid)).unwrap();
            f.write_all(&trailer_bytes).unwrap();
            f.sync_all().unwrap();
            iobuf.set_maxed(false);

            // transition this segment into deplete-only mode now
            // that n_writers is 0, and all calls to mark_replace/link
            // happen before the reservation completes.
            trace!("deactivating segment with lsn {}", segment_lsn);
            self.with_sa(|sa| sa.deactivate_segment(segment_lsn, segment_lid));
        } else {
            trace!(
                "not deactivating segment with lsn {}",
                base_lsn / io_buf_size as Lsn * io_buf_size as Lsn
            );
        }

        M.written_bytes.measure(res_len as f64);
        // signal that this IO buffer is uninitialized
        let max = std::usize::MAX as LogID;
        iobuf.set_lid(max);
        trace!("({:?}) {} log <- MAX", tn(), idx);

        // communicate to other threads that we have written an IO buffer.
        debug_delay();
        let _written_bufs = self.written_bufs.fetch_add(1, SeqCst);
        trace!(
            "({:?}) {} written",
            tn(),
            _written_bufs % self.config.get_io_bufs()
        );

        if res_len != 0 {
            let interval = (base_lsn, base_lsn + res_len as Lsn);

            debug!("wrote lsns {}-{} to disk at offsets {}-{}", 
                    base_lsn, base_lsn + res_len as Lsn, lid,
                    lid + res_len as LogID,);
            self.mark_interval(interval);
        }

        M.write_to_log.measure(clock() - start);
    }

    // It's possible that IO buffers are written out of order!
    // So we need to use this to keep track of them, and only
    // increment self.stable. If we didn't do this, then we would
    // accidentally decrement self.stable sometimes, or bump stable
    // above an offset that corresponds to a buffer that hasn't actually
    // been written yet! It's OK to use a mutex here because it is pretty
    // fast, compared to the other operations on shared state.
    fn mark_interval(&self, interval: (Lsn, Lsn)) {
        trace!("mark_interval({} - {})", interval.0, interval.1);
        assert_ne!(
            interval.0,
            interval.1,
            "mark_interval called with a zero-length range!"
        );
        assert!(
            interval.0 < interval.1,
            "tried to mark_interval with a high-end lower than the low-end"
        );
        let mut intervals = self.intervals.lock().unwrap();

        intervals.push(interval);

        debug_assert!(intervals.len() < 100, "intervals is getting crazy...");

        // reverse sort
        intervals.sort_unstable_by(|a, b| b.cmp(a));

        let mut updated = false;

        while let Some(&(low, high)) = intervals.last() {
            assert_ne!(low, high);
            let cur_stable = self.stable.load(SeqCst) as LogID;
            assert!(low >= cur_stable);
            if cur_stable == low {
                let old = self.stable.swap(high as usize, SeqCst);
                assert_eq!(old, cur_stable as usize);
                debug!("new highest interval: {} - {}", low, high);
                intervals.pop();
                updated = true;
            } else {
                break;
            }
        }

        if updated {
            self.interval_updated.notify_all();
        }
    }
}

impl Drop for IoBufs {
    fn drop(&mut self) {
        for _ in 0..self.config.get_io_bufs() {
            self.flush();
        }
        let f = self.file_for_writing.lock().unwrap();
        f.sync_all().unwrap();

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
        }
    }

    // This is called upon the initialization of a fresh segment.
    // We write a new segment header to the beginning of the buffer
    // for assistance during recovery. The caller is responsible
    // for ensuring that the IoBuf's capacity has been set properly.
    fn store_segment_header(&self, lsn: Lsn, prev: LogID) {
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
        let bumped = bump_offset(0, SEG_HEADER_LEN as u32);
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

    fn get_header(&self) -> u32 {
        debug_delay();
        self.header.load(SeqCst) as u32
    }

    fn set_header(&self, new: u32) {
        debug_delay();
        self.header.store(new as usize, SeqCst);
    }

    fn cas_header(&self, old: u32, new: u32) -> Result<u32, u32> {
        debug_delay();
        let res = self.header.compare_and_swap(
            old as usize,
            new as usize,
            SeqCst,
        ) as u32;
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
fn is_sealed(v: u32) -> bool {
    v >> 31 == 1
}

#[inline(always)]
fn mk_sealed(v: u32) -> u32 {
    v | 1 << 31
}

#[inline(always)]
fn n_writers(v: u32) -> u32 {
    v << 1 >> 25
}

#[inline(always)]
fn incr_writers(v: u32) -> u32 {
    assert_ne!(n_writers(v), 127);
    v + (1 << 24)
}

#[inline(always)]
fn decr_writers(v: u32) -> u32 {
    assert_ne!(n_writers(v), 0);
    v - (1 << 24)
}

#[inline(always)]
fn offset(v: u32) -> u32 {
    v << 8 >> 8
}

#[inline(always)]
fn bump_offset(v: u32, by: u32) -> u32 {
    assert_eq!(by >> 24, 0);
    v + by
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
