use std::io::{Seek, Write};

use rand::{Rng, thread_rng};

use super::*;

pub const HEADER_LEN: usize = 7;

struct IOBuf {
    buf: UnsafeCell<Vec<u8>>,
    header: AtomicUsize,
    log_offset: AtomicUsize,
}

impl Debug for IOBuf {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let header = self.get_header();
        formatter.write_fmt(format_args!("\n\tIOBuf {{ log_offset: {}, n_writers: {}, offset: \
                                          {}, sealed: {} }}",
                                         self.get_log_offset(),
                                         n_writers(header),
                                         offset(header),
                                         is_sealed(header)))
    }
}

impl IOBuf {
    fn new(buf_size: usize) -> IOBuf {
        IOBuf {
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

pub struct IOBufs {
    bufs: Vec<IOBuf>,
    sealed_bufs: AtomicUsize,
    written_bufs: AtomicUsize,

    intervals: Mutex<Vec<(LogID, LogID)>>,
    stable: AtomicUsize,
    pub file: Mutex<fs::File>,

    config: Config,
}

impl Debug for IOBufs {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let sealed_bufs = self.sealed_bufs.load(SeqCst);
        let written_bufs = self.written_bufs.load(SeqCst);

        formatter.write_fmt(format_args!("IOBufs {{ sealed: {}, written: {}, bufs: {:?} }}",
                                         sealed_bufs,
                                         written_bufs,
                                         self.bufs))
    }
}

/// `IOBufs` is a set of lock-free buffers for coordinating
/// writes to underlying storage.
impl IOBufs {
    pub fn new(config: Config) -> IOBufs {
        let mut options = fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);

        let (file, disk_offset) = if let Some(p) = config.get_path() {
            let file = options.open(p).unwrap();
            let disk_offset = file.metadata().unwrap().len();
            (file, disk_offset)
        } else {
            let nonce: String = thread_rng().gen_ascii_chars().take(10).collect();
            let path = format!("__rsdb_memory_{}.log", nonce);

            // "poor man's shared memory"
            // We retain an open descriptor to the file,
            // but it is no longer attached to this path,
            // so it continues to exist as a set of
            // anonymously mapped pages in memory only.
            let file = options.open(&path).unwrap();
            fs::remove_file(path).unwrap();
            (file, 0)
        };

        let bufs = rep_no_copy![IOBuf::new(config.get_io_buf_size()); config.get_io_bufs()];

        let sealed_bufs = 0;
        bufs[sealed_bufs].set_log_offset(disk_offset);

        IOBufs {
            file: Mutex::new(file),
            bufs: bufs,
            sealed_bufs: AtomicUsize::new(sealed_bufs),
            written_bufs: AtomicUsize::new(0),
            intervals: Mutex::new(vec![]),
            stable: AtomicUsize::new(disk_offset as usize),
            config: config,
        }
    }

    pub fn config(&self) -> Config {
        self.config.clone()
    }

    fn idx(&self) -> usize {
        let sealed_bufs = self.sealed_bufs.load(SeqCst);
        sealed_bufs % self.config.get_io_bufs()
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
    pub(super) fn reserve(&self, mut raw_buf: Vec<u8>) -> Reservation {
        let buf = encapsulate(&mut *raw_buf);
        assert_eq!(buf.len() >> 32, 0);
        assert!(buf.len() <= self.config.get_io_buf_size());

        let mut printed = false;
        let mut spins = 0;
        loop {
            let written_bufs = self.written_bufs.load(SeqCst);
            let sealed_bufs = self.sealed_bufs.load(SeqCst);
            let idx = sealed_bufs % self.config.get_io_bufs();

            spins += 1;
            if spins > 1_000_000 {
                // debug!("{:?} stalling in reserve, idx {}", tn(), idx);
                spins = 0;
            }

            if written_bufs > sealed_bufs {
                // This can happen because a reservation can finish up
                // before the sealing thread gets around to bumping
                // sealed_bufs.
                if !printed {
                    // trace!("({:?}) written ahead of sealed, spinning", tn());
                    printed = true;
                }
                continue;
            }

            if sealed_bufs - written_bufs >= self.config.get_io_bufs() - 1 {
                // if written is too far behind, we need to
                // spin while it catches up to avoid overlap
                if !printed {
                    // trace!("({:?}) old io buffer not written yet, spinning", tn());
                    printed = true;
                }
                continue;
            }

            // load current header value
            let ref iobuf = self.bufs[idx];
            let header = iobuf.get_header();

            // skip if already sealed
            if is_sealed(header) {
                // already sealed, start over and hope cur
                // has already been bumped by sealer.
                if !printed {
                    // trace!("({:?}) io buffer already sealed, spinning", tn());
                    printed = true;
                }
                continue;
            }

            // try to claim space
            let buf_offset = offset(header);
            if buf_offset as LogID + buf.len() as LogID > self.config.get_io_buf_size() as LogID {
                // This buffer is too full to accept our write!
                // Try to seal the buffer, and maybe write it if
                // there are zero writers.
                self.maybe_seal_and_write_iobuf(idx, header);
                if !printed {
                    // trace!("({:?}) io buffer too full, spinning", tn());
                    printed = true;
                }
                continue;
            }

            // attempt to claim by incrementing an unsealed header
            let bumped_offset = bump_offset(header, buf.len() as u32);
            let claimed = incr_writers(bumped_offset);
            assert!(!is_sealed(claimed));

            if iobuf.cas_header(header, claimed).is_err() {
                // CAS failed, start over
                if !printed {
                    // trace!("({:?}) CAS failed while claiming buffer slot, spinning", tn());
                    printed = true;
                }
                continue;
            }

            // if we're giving out a reservation,
            // the writer count should be positive
            assert!(n_writers(claimed) != 0);

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
        let ref iobuf = self.bufs[idx];
        let mut header = iobuf.get_header();

        // Decrement writer count, retrying until successful.
        let mut spins = 0;
        loop {
            spins += 1;
            if spins > 10 {
                // debug!("{:?} have spun >10x in decr", tn());
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
        let ref iobuf = self.bufs[idx];

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
        // trace!("({:?}) {} sealed", tn(), idx);

        // open new slot
        let res_len = offset(sealed) as usize;
        let max = std::usize::MAX as LogID;

        // FIXME triggered by flush()
        assert_ne!(log_offset,
                   max,
                   "({:?}) sealing something that should never have been claimed (idx {})\n{:?}",
                   tn(),
                   idx,
                   self);
        let next_offset = log_offset + res_len as LogID;
        let next_idx = (idx + 1) % self.config.get_io_bufs();
        let ref next_iobuf = self.bufs[next_idx];

        let mut spins = 0;
        while next_iobuf.cas_log_offset(max, next_offset).is_err() {
            spins += 1;
            if spins > 1_000_000 {
                // debug!("have spun >1,000,000x in seal of buf {}", idx);
                spins = 0;
            }
        }
        // trace!("({:?}) {} log set", tn(), next_idx);

        // NB allows new threads to start writing into this buffer
        next_iobuf.set_header(0);
        // trace!("({:?}) {} zeroed header", tn(), next_idx);

        let sealed_bufs = self.sealed_bufs.fetch_add(1, SeqCst) + 1;
        // trace!("({:?}) {} sealed_bufs", tn(), sealed_bufs % self.config.get_io_bufs());

        // if writers is 0, it's our responsibility to write the buffer.
        if n_writers(sealed) == 0 {
            self.write_to_log(idx);
        }
    }

    // Write an IO buffer's data to stable storage and set up the
    // next IO buffer for writing.
    fn write_to_log(&self, idx: usize) {
        let mut log = self.file.lock().unwrap();

        let ref iobuf = self.bufs[idx];
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

        log.seek(SeekFrom::Start(log_offset)).unwrap();
        log.write_all(&dirty_bytes).unwrap();

        // signal that this IO buffer is uninitialized
        let max = std::usize::MAX as LogID;
        iobuf.set_log_offset(max);
        // trace!("({:?}) {} log <- MAX", tn(), idx);

        // communicate to other threads that we have written an IO buffer.
        let written_bufs = self.written_bufs.fetch_add(1, SeqCst);
        // trace!("({:?}) {} written", tn(), written_bufs % self.config.get_io_bufs());

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

impl Drop for IOBufs {
    fn drop(&mut self) {
        for _ in 0..self.config.get_io_bufs() {
            self.flush();
        }
    }
}

#[inline(always)]
fn encapsulate(buf: &mut [u8]) -> Vec<u8> {
    let size_bytes = ops::usize_to_array(buf.len()).to_vec();
    let valid_bytes = vec![1u8];
    let crc16_bytes = crc16_arr(buf).to_vec();

    let mut out = Vec::with_capacity(HEADER_LEN + buf.len());
    out.extend_from_slice(&*valid_bytes);
    out.extend_from_slice(&*size_bytes);
    out.extend_from_slice(&*crc16_bytes);
    out.extend_from_slice(buf);
    assert_eq!(out.len(), HEADER_LEN + buf.len());
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
    assert!(n_writers(v) != 127);
    v + (1 << 24)
}

#[inline(always)]
fn decr_writers(v: u32) -> u32 {
    assert!(n_writers(v) != 0);
    v - (1 << 24)
}

#[inline(always)]
fn offset(v: u32) -> u32 {
    v << 8 >> 8
}

#[inline(always)]
fn bump_offset(v: u32, by: u32) -> u32 {
    assert!(by >> 24 == 0);
    v + by
}
