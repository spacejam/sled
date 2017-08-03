use std::io::{Seek, Write};

use rand::{Rng, thread_rng};

use super::*;

pub const HEADER_LEN: usize = 7;

struct IOBuf {
    buf: UnsafeCell<Vec<u8>>,
    header: AtomicUsize,
    log_offset: AtomicUsize,
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
    current_buf: AtomicUsize,
    written_bufs: AtomicUsize,

    intervals: Mutex<Vec<(LogID, LogID)>>,
    stable: AtomicUsize,
    pub file: Mutex<fs::File>,

    config: Config,
}

impl Debug for IOBufs {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let current_buf = self.current_buf.load(SeqCst);
        let written = self.written_bufs.load(SeqCst);
        let slow_writers = current_buf - written >= self.config.get_io_bufs();
        let idx = current_buf % self.config.get_io_bufs();
        // load current header value
        let ref iobuf = self.bufs[idx];
        let hv = iobuf.get_header();
        let n_writers = n_writers(hv);
        let offset = offset(hv);
        let sealed = is_sealed(hv);

        let debug = format!("IOBufs {{ idx: {}, slow_writers: {},  n_writers: {}, \
                             offset: {}, sealed: {} }}",
                            idx,
                            slow_writers,
                            n_writers,
                            offset,
                            sealed);

        fmt::Debug::fmt(&debug, formatter)
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

        let current_buf = 0;
        bufs[current_buf].set_log_offset(disk_offset);

        IOBufs {
            file: Mutex::new(file),
            bufs: bufs,
            current_buf: AtomicUsize::new(current_buf),
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
    pub(super) fn reserve(&self, mut raw_buf: Vec<u8>) -> Reservation {
        let buf = encapsulate(&mut *raw_buf);
        assert_eq!(buf.len() >> 32, 0);
        assert!(buf.len() <= self.config.get_io_buf_size());

        let mut spins = 0;
        loop {
            let written = self.written_bufs.load(SeqCst);
            let current_buf = self.current_buf.load(SeqCst);
            let idx = current_buf % self.config.get_io_bufs();

            spins += 1;
            if spins > 1_000_000 {
                // println!("{:?} stalling in reserve, idx {}", thread::current().name(), idx);
                spins = 0;
            }

            if written > current_buf {
                // This can happen because a reservation can finish up
                // before the sealing thread gets around to bumping
                // current_buf.
                continue;
            }

            if current_buf - written > self.config.get_io_bufs() {
                // if written is too far behind, we need to
                // spin while it catches up to avoid overlap
                continue;
            }

            // load current header value
            let ref iobuf = self.bufs[idx];
            let header = iobuf.get_header();

            // skip if already sealed
            if is_sealed(header) {
                // already sealed, start over and hope cur
                // has already been bumped by sealer.
                // println!("cur is late to be bumped: {:?}", self);
                continue;
            }

            // try to claim space
            let buf_offset = offset(header);
            if buf_offset as LogID + buf.len() as LogID > self.config.get_io_buf_size() as LogID {
                // This buffer is too full to accept our write!
                // Try to seal the buffer, and maybe write it if
                // there are zero writers.
                self.maybe_seal_and_write_iobuf(idx, header);
                continue;
            }

            // attempt to claim by incrementing an unsealed header
            let bumped_offset = bump_offset(header, buf.len() as u32);
            let claimed = incr_writers(bumped_offset);
            assert!(!is_sealed(claimed));

            if iobuf.cas_header(header, claimed).is_err() {
                // CAS failed, start over
                continue;
            }

            // if we're giving out a reservation,
            // the writer count should be positive
            assert!(n_writers(claimed) != 0);

            let mut spins = 0;
            let mut log_offset: LogID;
            loop {
                log_offset = iobuf.get_log_offset();

                // if we're giving out a reservation, it should
                // be for an initialized buffer.
                if log_offset as usize != std::usize::MAX {
                    break;
                }

                spins += 1;
                if spins == 1_000_000 {
                    // println!("stalling while waiting on log_offset to be reset by last writer.");
                    spins = 0;
                }
            }

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
                // println!("{:?} have spun >10x in decr", thread::current().name());
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
        if offset(header) == 0 {
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

        let sealed = mk_sealed(header);

        if iobuf.cas_header(header, sealed).is_err() {
            // cas failed, don't try to continue
            return;
        }

        // open new slot
        self.current_buf.fetch_add(1, SeqCst);

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
                   "created reservation for uninitialized slot");

        let res_len = offset(header) as usize;
        let data = unsafe { (*iobuf.buf.get()).as_mut_slice() };
        let dirty_bytes = &data[0..res_len];

        log.seek(SeekFrom::Start(log_offset)).unwrap();
        log.write_all(&dirty_bytes).unwrap();

        // signal that this IO buffer is uninitialized
        let max = std::usize::MAX as LogID;
        iobuf.set_log_offset(max);

        // We spin here on trying to set the next buffer's disk offset.
        // We need to spin here because we have the information about where
        // its disk offset should be, and we choose to block here instead of when
        // we write it in the future and it's not set up yet.
        // Its offset is set to usize::MAX after it has been sucessfully written,
        // so we must wait for this to be true before setting the new disk offset,
        // otherwise we will alter the offset of pending writes.
        let next_offset = log_offset + res_len as LogID;

        let mut spins = 0;
        loop {
            spins += 1;
            if spins > 1_000_000 {
                // println!("{:?} have spun >1,000,000x in seal of buf {}", thread::current().name(), idx);
                spins = 0;
            }
            if self.bufs[(idx + 1) % self.config.get_io_bufs()]
                .cas_log_offset(max, next_offset)
                .is_ok() {
                // success, now we can stop stalling and do other work
                break;
            }
        }

        // NB allows new threads to start writing into this buffer
        iobuf.set_header(0);

        // communicate to other threads that we have written an IO buffer.
        self.written_bufs.fetch_add(1, SeqCst);

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
