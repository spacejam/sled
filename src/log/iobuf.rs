use std::io::{Seek, Write};

use zstd::block::compress;

use super::*;

#[doc(hidden)]
pub const HEADER_LEN: usize = 7;

struct IoBuf {
    buf: UnsafeCell<Vec<u8>>,
    header: AtomicUsize,
    log_offset: AtomicUsize,
}

unsafe impl Sync for IoBuf {}

impl Debug for IoBuf {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let header = self.get_header();
        formatter.write_fmt(format_args!("\n\tIoBuf {{ log_offset: {}, n_writers: {}, offset: \
                                          {}, sealed: {} }}",
                                         self.get_log_offset(),
                                         n_writers(header),
                                         offset(header),
                                         is_sealed(header)))
    }
}

impl IoBuf {
    fn new(buf_size: usize) -> IoBuf {
        IoBuf {
            buf: UnsafeCell::new(vec![0; buf_size]),
            header: AtomicUsize::new(0),
            log_offset: AtomicUsize::new(std::usize::MAX),
        }
    }

    fn set_log_offset(&self, offset: LogID) {
        self.log_offset.store(offset as usize, SeqCst);
    }

    fn get_log_offset(&self) -> LogID {
        self.log_offset.load(SeqCst) as LogID
    }

    fn get_header(&self) -> u32 {
        self.header.load(SeqCst) as u32
    }

    fn set_header(&self, new: u32) {
        self.header.store(new as usize, SeqCst);
    }

    fn cas_header(&self, old: u32, new: u32) -> Result<u32, u32> {
        let res = self.header.compare_and_swap(old as usize, new as usize, SeqCst) as u32;
        if res == old {
            Ok(new)
        } else {
            Err(res)
        }
    }

    fn cas_log_offset(&self, old: LogID, new: LogID) -> Result<LogID, LogID> {
        let res = self.log_offset.compare_and_swap(old as usize, new as usize, SeqCst) as LogID;
        if res == old {
            Ok(new)
        } else {
            Err(res)
        }
    }
}

pub struct IoBufs {
    config: Config,
    bufs: Vec<IoBuf>,
    current_buf: AtomicUsize,
    written_bufs: AtomicUsize,
    // Pending intervals that have been written to stable storage, but may be
    // higher than the current value of `stable` due to interesting thread
    // interleavings.
    intervals: Mutex<Vec<(LogID, LogID)>>,
    // The highest CONTIGUOUS offset that has been written to stable storage.
    // This may be lower than the length of the underlying file, and there
    // may be buffers that have been written out-of-order to stable storage
    // due to interesting thread interleavings.
    stable: AtomicUsize,
}

impl Debug for IoBufs {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let current_buf = self.current_buf.load(SeqCst);
        let written_bufs = self.written_bufs.load(SeqCst);

        formatter.write_fmt(format_args!("IoBufs {{ sealed: {}, written: {}, bufs: {:?} }}",
                                         current_buf,
                                         written_bufs,
                                         self.bufs))
    }
}

/// `IoBufs` is a set of lock-free buffers for coordinating
/// writes to underlying storage.
impl IoBufs {
    pub fn new(config: Config) -> IoBufs {
        let disk_offset = {
            let cached_f = config.cached_file();
            let file = cached_f.borrow();
            file.metadata().unwrap().len()
        };

        let bufs = rep_no_copy![IoBuf::new(config.get_io_buf_size()); config.get_io_bufs()];

        let current_buf = 0;
        bufs[current_buf].set_log_offset(disk_offset);

        IoBufs {
            bufs: bufs,
            current_buf: AtomicUsize::new(current_buf),
            written_bufs: AtomicUsize::new(0),
            intervals: Mutex::new(vec![]),
            stable: AtomicUsize::new(disk_offset as usize),
            config: config,
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    fn idx(&self) -> usize {
        let current_buf = self.current_buf.load(SeqCst);
        current_buf % self.config.get_io_bufs()
    }

    /// Returns the last stable offset in storage.
    pub(super) fn stable(&self) -> LogID {
        self.stable.load(SeqCst) as LogID
    }

    /// Tries to claim a reservation for writing a buffer to a
    /// particular location in stable storge, which may either be
    /// completed or aborted later. Useful for maintaining
    /// linearizability across CAS operations that may need to
    /// persist part of their operation.
    ///
    /// # Panics
    ///
    /// Panics if the desired reservation is greater than 8388601 bytes..
    /// (config io buf size - 7)
    pub(super) fn reserve(&self, raw_buf: Vec<u8>) -> Reservation {
        assert_eq!((raw_buf.len() + HEADER_LEN) >> 32, 0);

        let buf = encapsulate(raw_buf, self.config.get_use_compression());

        assert!(buf.len() <= self.config.get_io_buf_size());

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
            let written_bufs = self.written_bufs.load(SeqCst);
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
                continue;
            }

            if current_buf - written_bufs >= self.config.get_io_bufs() {
                // if written is too far behind, we need to
                // spin while it catches up to avoid overlap
                trace_once!("({:?}) old io buffer not written yet, spinning", tn());
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
                continue;
            }

            // try to claim space
            let buf_offset = offset(header);
            if buf_offset as LogID + buf.len() as LogID > self.config.get_io_buf_size() as LogID {
                // This buffer is too full to accept our write!
                // Try to seal the buffer, and maybe write it if
                // there are zero writers.
                self.maybe_seal_and_write_iobuf(idx, header);
                trace_once!("({:?}) io buffer too full, spinning", tn());
                continue;
            }

            // attempt to claim by incrementing an unsealed header
            let bumped_offset = bump_offset(header, buf.len() as u32);
            let claimed = incr_writers(bumped_offset);
            assert!(!is_sealed(claimed));

            if iobuf.cas_header(header, claimed).is_err() {
                // CAS failed, start over
                trace_once!("({:?}) CAS failed while claiming buffer slot, spinning",
                            tn());
                continue;
            }

            // if we're giving out a reservation,
            // the writer count should be positive
            assert_ne!(n_writers(claimed), 0);

            let log_offset = iobuf.get_log_offset();
            assert_ne!(log_offset as usize,
                       std::usize::MAX,
                       "({:?}) fucked up on idx {}\n{:?}",
                       tn(),
                       idx,
                       self);

            let mut out_buf = unsafe { (*iobuf.buf.get()).as_mut_slice() };

            let res_start = buf_offset as usize;
            let res_end = res_start + buf.len();
            let destination = &mut (out_buf)[res_start..res_end];

            return Reservation {
                idx: idx,
                iobufs: self,
                data: buf,
                destination: destination,
                flushed: false,
                base_disk_offset: log_offset as LogID,
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

        // Succeeded in decrementing writers, if we decremented writers to 0
        // and it's sealed then we should write it to storage.
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
        self.maybe_seal_and_write_iobuf(idx, header);
    }

    // Attempt to seal the current IO buffer, possibly
    // writing it to disk if there are no other writers
    // operating on it.
    fn maybe_seal_and_write_iobuf(&self, idx: usize, header: u32) {
        let iobuf = &self.bufs[idx];

        if is_sealed(header) {
            // this buffer is already sealed. nothing to do here.
            return;
        }

        // NB need to do this before CAS because it can get
        // written and reset by another thread afterward
        let log_offset = iobuf.get_log_offset();

        let sealed = mk_sealed(header);

        if iobuf.cas_header(header, sealed).is_err() {
            // cas failed, don't try to continue
            return;
        }
        trace!("({:?}) {} sealed", tn(), idx);

        // open new slot
        let res_len = offset(sealed) as usize;
        let max = std::usize::MAX as LogID;

        assert_ne!(log_offset,
                   max,
                   "({:?}) sealing something that should never have been claimed (idx {})\n{:?}",
                   tn(),
                   idx,
                   self);
        let next_offset = log_offset + res_len as LogID;
        let next_idx = (idx + 1) % self.config.get_io_bufs();
        let next_iobuf = &self.bufs[next_idx];

        let mut spins = 0;
        while next_iobuf.cas_log_offset(max, next_offset).is_err() {
            spins += 1;
            if spins > 1_000_000 {
                debug!("have spun >1,000,000x in seal of buf {}", idx);
                spins = 0;
            }
        }
        trace!("({:?}) {} log set", tn(), next_idx);

        // NB allows new threads to start writing into this buffer
        next_iobuf.set_header(0);
        trace!("({:?}) {} zeroed header", tn(), next_idx);

        let current_buf = self.current_buf.fetch_add(1, SeqCst) + 1;
        trace!("({:?}) {} current_buf",
               tn(),
               current_buf % self.config.get_io_bufs());

        // if writers is 0, it's our responsibility to write the buffer.
        if n_writers(sealed) == 0 {
            self.write_to_log(idx);
        }
    }

    // Write an IO buffer's data to stable storage and set up the
    // next IO buffer for writing.
    fn write_to_log(&self, idx: usize) {
        let iobuf = &self.bufs[idx];
        let header = iobuf.get_header();
        let log_offset = iobuf.get_log_offset();
        let interval = (log_offset, log_offset + offset(header) as LogID);

        assert_ne!(log_offset as usize,
                   std::usize::MAX,
                   "({:?}) created reservation for uninitialized slot",
                   tn());

        let res_len = offset(header) as usize;
        let data = unsafe { (*iobuf.buf.get()).as_mut_slice() };
        let dirty_bytes = &data[0..res_len];

        let cached_f = self.config.cached_file();
        let mut f = cached_f.borrow_mut();
        f.seek(SeekFrom::Start(log_offset)).unwrap();
        f.write_all(dirty_bytes).unwrap();

        // signal that this IO buffer is uninitialized
        let max = std::usize::MAX as LogID;
        iobuf.set_log_offset(max);
        trace!("({:?}) {} log <- MAX", tn(), idx);

        // communicate to other threads that we have written an IO buffer.
        let written_bufs = self.written_bufs.fetch_add(1, SeqCst);
        trace!("({:?}) {} written",
               tn(),
               written_bufs % self.config.get_io_bufs());

        self.mark_interval(interval);
    }

    // It's possible that IO buffers are written out of order!
    // So we need to use this to keep track of them, and only
    // increment self.stable. If we didn't do this, then we would
    // accidentally decrement self.stable sometimes, or bump stable
    // above an offset that corresponds to a buffer that hasn't actually
    // been written yet! It's OK to use a mutex here because it is pretty
    // fast, compared to the other operations on shared state.
    fn mark_interval(&self, interval: (LogID, LogID)) {
        let mut intervals = self.intervals.lock().unwrap();
        intervals.push(interval);
        intervals.sort();

        while let Some(&(low, high)) = intervals.get(0) {
            let cur_stable = self.stable.load(SeqCst) as LogID;
            if cur_stable == low {
                let old = self.stable.swap(high as usize, SeqCst);
                assert_eq!(old, cur_stable as usize);
                intervals.remove(0);
            } else {
                break;
            }
        }
    }
}

impl Drop for IoBufs {
    fn drop(&mut self) {
        for _ in 0..self.config.get_io_bufs() {
            self.flush();
        }
    }
}

#[inline(always)]
fn encapsulate(raw_buf: Vec<u8>, use_compression: bool) -> Vec<u8> {
    let mut buf = if use_compression {
        compress(&*raw_buf, 5).unwrap()
    } else {
        raw_buf
    };

    let mut size_bytes = ops::usize_to_array(buf.len()).to_vec();
    let mut valid_bytes = vec![1u8];
    let mut crc16_bytes = crc16_arr(&buf).to_vec();

    let mut out = Vec::with_capacity(HEADER_LEN + buf.len());
    out.append(&mut valid_bytes);
    out.append(&mut size_bytes);
    out.append(&mut crc16_bytes);
    assert_eq!(out.len(), HEADER_LEN);
    out.append(&mut buf);
    out
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
