use std::fmt::{self, Debug};
use std::fs;
use std::io::{self, Read, Write, Seek, SeekFrom, Error, ErrorKind};
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::cell::UnsafeCell;
use std::thread;
use std::os::unix::io::AsRawFd;

#[cfg(target_os="linux")]
use libc::{fallocate, FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE};

use super::*;

pub const HEADER_LEN: usize = 7;
pub const MAX_BUF_SZ: usize = 2 << 22; // 8mb
pub const N_BUFS: usize = 2;

pub trait Log: Send + Sync {
    fn reserve(&self, Vec<u8>) -> Reservation;
    fn write(&self, Vec<u8>) -> LogID;
    fn read(&self, id: LogID) -> io::Result<Option<Vec<u8>>>;
    fn stable_offset(&self) -> LogID;
    fn make_stable(&self, id: LogID);
    fn punch_hole(&self, id: LogID);
}

/// `LockFreeLog` is responsible for putting data on disk, and retrieving
/// it later on.
pub struct LockFreeLog {
    iobufs: IOBufs,
}

unsafe impl Send for LockFreeLog {}
unsafe impl Sync for LockFreeLog {}

impl LockFreeLog {
    /// create new lock-free log
    pub fn start_system(path: String) -> LockFreeLog {
        let cur_id = fs::metadata(path.clone()).map(|m| m.len()).unwrap_or(0);

        let mut options = fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);
        let file = options.open(path).unwrap();

        let iobufs = IOBufs::new(file, cur_id as usize);

        LockFreeLog { iobufs: iobufs }
    }
}

impl Log for LockFreeLog {
    /// claim a spot on disk, which can later be filled or aborted
    fn reserve(&self, buf: Vec<u8>) -> Reservation {
        self.iobufs.reserve(buf)
    }

    /// write a buffer to disk
    fn write(&self, buf: Vec<u8>) -> LogID {
        self.iobufs.write(buf)
    }

    /// read a buffer from the disk
    fn read(&self, id: LogID) -> io::Result<Option<Vec<u8>>> {
        let mut f = self.iobufs.file.lock().unwrap();
        f.seek(SeekFrom::Start(id))?;

        let mut valid = [0u8; 1];
        f.read_exact(&mut valid)?;
        if valid[0] == 0 {
            return Ok(None);
        }

        let mut len_buf = [0u8; 4];
        f.read_exact(&mut len_buf)?;
        let len = ops::array_to_usize(len_buf);
        if len > MAX_BUF_SZ {
            let msg = format!("read invalid message length, {} should be <= {}",
                              len,
                              MAX_BUF_SZ);
            return Err(Error::new(ErrorKind::Other, msg));
        }

        let mut crc16_buf = [0u8; 2];
        f.read_exact(&mut crc16_buf)?;

        let mut buf = Vec::with_capacity(len);
        unsafe {
            buf.set_len(len);
        }
        f.read_exact(&mut buf)?;

        let checksum = crc16_arr(&buf);
        if checksum != crc16_buf {
            let msg = format!("read data failed crc16 checksum, {:?} should be {:?}",
                              checksum,
                              crc16_buf);
            return Err(Error::new(ErrorKind::Other, msg));
        }

        Ok(Some(buf))
    }

    /// returns the current stable offset written to disk
    fn stable_offset(&self) -> LogID {
        self.iobufs.stable.load(SeqCst) as LogID
    }

    /// blocks until the specified id has been made stable on disk
    fn make_stable(&self, id: LogID) {
        let mut spins = 0;
        loop {
            spins += 1;
            if spins > 2000000 {
                println!("{:?} have spun >2000000x in make_stable",
                         thread::current().name());
                spins = 0;
            }
            let cur = self.iobufs.stable.load(SeqCst) as LogID;
            if cur > id {
                return;
            }

            self.iobufs.try_to_advance_buffer();
        }
    }

    /// deallocates the data part of a log id
    fn punch_hole(&self, id: LogID) {
        // we zero out the valid byte, and use fallocate to punch a hole
        // in the actual data, but keep the len for recovery.
        let mut f = self.iobufs.file.lock().unwrap();
        // zero out valid bit
        f.seek(SeekFrom::Start(id)).unwrap();
        let zeros = vec![0];
        f.write_all(&*zeros).unwrap();
        f.seek(SeekFrom::Start(id + 1)).unwrap();
        let mut len_buf = [0u8; 4];
        f.read_exact(&mut len_buf).unwrap();

        let len = ops::array_to_usize(len_buf);
        #[cfg(target_os="linux")]
        let mode = FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE;
        let fd = f.as_raw_fd();

        unsafe {
            // 5 is valid (1) + len (4), 2 is crc16
            #[cfg(target_os="linux")]
            fallocate(fd, mode, id as i64 + 5, len as i64 + 2);
        }
    }
}

struct IOBufs {
    file: Mutex<fs::File>,
    bufs: Vec<UnsafeCell<Vec<u8>>>,
    headers: Vec<AtomicUsize>,
    log_offsets: Vec<AtomicUsize>,
    current_buf: AtomicUsize,
    written_bufs: AtomicUsize,
    intervals: Mutex<Vec<(LogID, LogID)>>,
    stable: AtomicUsize,
}

impl Debug for IOBufs {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let current_buf = self.current_buf.load(SeqCst);
        let written = self.written_bufs.load(SeqCst);
        let slow_writers = current_buf - written >= N_BUFS;
        let idx = current_buf % N_BUFS;
        // load current header value
        let ref header = self.headers[idx % N_BUFS];
        let hv = header.load(SeqCst) as u32;
        let n_writers = bit_ops::n_writers(hv);
        let offset = bit_ops::offset(hv);
        let sealed = bit_ops::is_sealed(hv);

        let debug = format!("IOBufs {{ idx: {}, slow_writers: {},  n_writers: {}, offset: {}, \
                             sealed: {} }}",
                            idx,
                            slow_writers,
                            n_writers,
                            offset,
                            sealed);

        fmt::Debug::fmt(&debug, formatter)
    }
}

unsafe impl Send for IOBufs {}
unsafe impl Sync for IOBufs {}

impl IOBufs {
    fn new(file: fs::File, disk_offset: usize) -> IOBufs {
        let stable = AtomicUsize::new(disk_offset);
        let current_buf = 0;
        let bufs = rep_no_copy![UnsafeCell::new(vec![0; MAX_BUF_SZ]); N_BUFS];
        let headers = rep_no_copy![AtomicUsize::new(0); N_BUFS];
        let log_offsets = rep_no_copy![AtomicUsize::new(std::usize::MAX); N_BUFS];
        log_offsets[current_buf].store(disk_offset, SeqCst);
        IOBufs {
            file: Mutex::new(file),
            bufs: bufs,
            headers: headers,
            log_offsets: log_offsets,
            current_buf: AtomicUsize::new(current_buf),
            written_bufs: AtomicUsize::new(0),
            intervals: Mutex::new(vec![]),
            stable: stable,
        }
    }

    fn reserve(&self, raw_buf: Vec<u8>) -> Reservation {
        let buf = encapsulate(&*raw_buf);
        assert_eq!(buf.len() >> 32, 0);
        assert!(buf.len() <= MAX_BUF_SZ);

        let mut spins = 0;
        loop {
            spins += 1;
            // load atomic progress counters
            let written = self.written_bufs.load(SeqCst);
            let current_buf = self.current_buf.load(SeqCst);
            let idx = current_buf % N_BUFS;
            if spins > 100_000 {
                println!("{:?} have spun >100,000x in reserve, idx {}",
                         thread::current().name(),
                         idx);
                spins = 0;
            }

            // if written is too far behind, we need to
            // spin while it catches up to avoid overlap
            if current_buf - written >= N_BUFS {
                // println!("writers are behind: {:?}", self);
                continue;
            }

            // load current header value
            let ref header = self.headers[idx % N_BUFS];
            let mut hv = header.load(SeqCst) as u32;

            // skip if already sealed
            if bit_ops::is_sealed(hv) {
                // already sealed, start over and hope cur
                // has already been bumped by sealer.
                // println!("cur is late to be bumped: {:?}", self);
                continue;
            }

            // try to claim space, seal otherwise
            let buf_offset = bit_ops::offset(hv);
            if buf_offset + buf.len() as u32 > MAX_BUF_SZ as u32 {
                // attempt seal once, flush if no active writers, then start over
                self.roll_buffer(idx, hv);
                continue;
            }

            // attempt to claim
            let claimed = bit_ops::incr_writers(bit_ops::bump_offset(hv, buf.len() as u32));
            assert!(!bit_ops::is_sealed(claimed));

            let cas_hv = header.compare_and_swap(hv as usize, claimed as usize, SeqCst);
            if cas_hv != hv as usize {
                // CAS failed, start over
                continue;
            }
            hv = claimed;

            // if we're giving out a reservation,
            // the writer count should be positive
            assert!(bit_ops::n_writers(hv) != 0);

            assert_ne!(self.log_offsets[idx % N_BUFS].load(SeqCst), std::usize::MAX);
            return self.reservation(buf_offset, buf, idx);
        }
    }

    fn try_to_advance_buffer(&self) {
        // TODO incr seal decr roll
        let current_buf = self.current_buf.load(SeqCst);
        let idx = current_buf % N_BUFS;
        let hv = self.headers[idx].load(SeqCst) as u32;
        self.roll_buffer(idx, hv);
    }

    fn roll_buffer(&self, idx: usize, last_hv: u32) {
        match self.seal_header_and_bump_offsets(last_hv, idx) {
            Ok(h) if bit_ops::n_writers(h) == 0 => {
                // nobody else is going to flush this, so we need to

                assert_ne!(self.log_offsets[idx % N_BUFS].load(SeqCst), std::usize::MAX);

                self.write_to_log(idx);
            }
            _ => {}
        }
    }

    fn seal_header_and_bump_offsets(&self, hv: u32, idx: usize) -> Result<u32, ()> {
        let ref header = self.headers[idx % N_BUFS];
        let ref next_header = self.headers[(idx + 1) % N_BUFS];
        let ref cur_log_offset = self.log_offsets[idx % N_BUFS];
        let ref next_log_offset = self.log_offsets[(idx + 1) % N_BUFS];

        if bit_ops::is_sealed(hv) {
            // don't want to double seal, since we should change critical offsets only once
            return Err(());
        }
        let sealed = bit_ops::mk_sealed(hv);
        if header.compare_and_swap(hv as usize, sealed as usize, SeqCst) == hv as usize {
            // println!("sealed buf with {} writers", ops::n_writers(sealed));
            // We succeeded in setting seal,
            // so we get to bump cur and log_offset.

            // NB This is effectively a global lock until self.current_buf gets bumped.

            // NB Set next offset before bumping self.current_buf.

            // Also note that written_bufs may be incremented before we reach
            // the increment of current_buf below, as a writing thread
            // sees the seal. This is why we initialize current_buf to 1 and
            // written_bufs to 0.

            let our_log_offset = cur_log_offset.load(SeqCst);
            assert_ne!(our_log_offset, std::usize::MAX);

            let next_offset = our_log_offset + bit_ops::offset(sealed) as usize;

            // !! setup new slot
            next_header.store(0, SeqCst);

            let mut spins = 0;
            loop {
                spins += 1;
                if spins > 100_000 {
                    println!("{:?} have spun >100,000x in seal of buf {}",
                             thread::current().name(),
                             bit_ops::n_writers(sealed));
                    spins = 0;
                }
                // FIXME panicked at 'assertion failed: `(left == right)` (left: `2089005254`, right:
                // `18446744073709551615`)
                let old = next_log_offset.compare_and_swap(std::usize::MAX, next_offset, SeqCst);
                if old == std::usize::MAX {
                    break;
                }
            }

            // !! open new slot
            self.current_buf.fetch_add(1, SeqCst);
            // println!("setting next buf to {}", (next_buf + 1) % N_BUFS);

            Ok(sealed)
        } else {
            Err(())
        }
    }


    fn reservation(&self, buf_offset: u32, res_buf: Vec<u8>, idx: usize) -> Reservation {
        Reservation {
            idx: idx,
            iobufs: self.clone(),
            res_buf: res_buf,
            buf_offset: buf_offset,
            flushed: false,
            base_disk_offset: self.log_offsets[idx % N_BUFS].load(SeqCst) as LogID,
        }
    }

    fn write(&self, buf: Vec<u8>) -> LogID {
        let res = self.reserve(buf);
        res.complete()
    }

    fn flush(&self, idx: usize, buf_offset: usize, data: &[u8]) {
        let mut out_buf = unsafe { (*self.bufs[idx % N_BUFS].get()).as_mut_slice() };

        let data_end = buf_offset + data.len();

        (out_buf)[buf_offset..data_end].copy_from_slice(data);

        self.decr_writers_maybe_slam(idx)
    }

    fn decr_writers_maybe_slam(&self, idx: usize) {
        let mut hv = self.headers[idx % N_BUFS].load(SeqCst) as u32;

        // decr writer count, retrying
        let mut spins = 0;
        loop {
            spins += 1;
            if spins > 1000 {
                println!("{:?} have spun >1000x in decr", thread::current().name());
                spins = 0;
            }
            let new_hv = bit_ops::decr_writers(hv) as usize;
            let old_hv = self.headers[idx % N_BUFS].compare_and_swap(hv as usize, new_hv, SeqCst);
            if old_hv == hv as usize {
                // succeeded in decrementing writers, if we set it to 0 and it's
                // sealed then we should persist it.
                if bit_ops::n_writers(new_hv as u32) == 0 && bit_ops::is_sealed(new_hv as u32) {
                    // the actual buffer offset for writing is communicated in new_hv
                    self.write_to_log(idx);
                }

                return;
            }

            // we failed to decr, reload and retry
            hv = old_hv as u32;

            // if this is 0, it means too many decr's have happened
            // or too few incr's have happened
            assert_ne!(bit_ops::n_writers(hv), 0);
        }
    }

    // sends the reservation to a writer thread
    fn write_to_log(&self, idx: usize) {
        let mut log = self.file.lock().unwrap();

        // println!("logwriter starting write of idx {}", idx);

        let header = self.headers[idx % N_BUFS].load(SeqCst) as u32;
        let log_offset = self.log_offsets[idx % N_BUFS].load(SeqCst) as u64;
        let interval = (log_offset, log_offset + bit_ops::offset(header) as u64);

        assert_ne!(log_offset as usize,
                   std::usize::MAX,
                   "created reservation for uninitialized slot");

        let data = unsafe { (*self.bufs[idx % N_BUFS].get()).as_mut_slice() };

        let res_len = bit_ops::offset(header) as usize;

        let dirty_bytes = &data[0..res_len];

        log.seek(SeekFrom::Start(log_offset)).unwrap();
        log.write_all(&dirty_bytes).unwrap();

        // TODO this MAY not be necessary, but
        // asserts are still failing this "taint"
        self.log_offsets[idx % N_BUFS].store(std::usize::MAX, SeqCst);

        // println!("deinitialized idx {}", self.idx);

        // bump self.written by 1
        self.written_bufs.fetch_add(1, SeqCst);
        // println!("writer offset now {}", (new_writer_offset + 1) % N_BUFS);

        self.mark_interval(interval);
        // println!("finished writing idx of {}", res.idx);
    }

    fn mark_interval(&self, interval: (LogID, LogID)) {
        let mut intervals = self.intervals.lock().unwrap();
        intervals.push(interval);
        intervals.sort();

        while let Some(&(low, high)) = intervals.get(0) {
            let cur_stable = self.stable.load(SeqCst) as u64;
            // println!("cs: {}, low: {}, high: {}, n_pending: {}", cur_stable, low, high, written_intervals.len());
            // println!("{:?}", written_intervals);

            if cur_stable == low {
                // println!("bumping");
                let old = self.stable.swap(high as usize, SeqCst);
                assert_eq!(old, cur_stable as usize);
                intervals.remove(0);
            } else {
                // println!("break!");
                break;
            }
        }
    }
}

#[derive(Clone)]
pub struct Reservation<'a> {
    iobufs: &'a IOBufs,
    idx: usize,
    res_buf: Vec<u8>,
    buf_offset: u32,
    flushed: bool,
    base_disk_offset: LogID,
}

unsafe impl<'a> Send for Reservation<'a> {}

impl<'a> Drop for Reservation<'a> {
    fn drop(&mut self) {
        // We auto-abort if the user never uses a reservation.
        if !self.res_buf.is_empty() && !self.flushed {
            self.clone().abort();
        }
    }
}

impl<'a> Reservation<'a> {
    /// cancel the reservation, placing a failed flush on disk
    pub fn abort(self) {
        self.flush(false);
    }

    /// complete the reservation, placing the buffer on disk at the log_id
    pub fn complete(self) -> LogID {
        self.flush(true)
    }

    /// get the log_id for accessing this buffer in the future
    pub fn log_id(&self) -> LogID {
        self.base_disk_offset
    }

    fn flush(mut self, valid: bool) -> LogID {
        if self.flushed {
            panic!("flushing already-flushed reservation!");
        }

        self.flushed = true;

        if !valid {
            zero_failed_buf(&mut self.res_buf);
        }

        self.iobufs.flush(self.idx, self.buf_offset as usize, &*self.res_buf);

        self.log_id()
    }
}

#[inline(always)]
fn encapsulate(buf: &[u8]) -> Vec<u8> {
    let mut out_buf = vec![0; buf.len() + HEADER_LEN];
    let size_bytes = ops::usize_to_array(buf.len()).to_vec();
    let (valid_bytes, crc16_bytes) = (vec![1u8], crc16_arr(buf));

    let valid_end = valid_bytes.len();
    let size_start = valid_end;
    let size_end = valid_end + size_bytes.len();
    let crc16_start = size_end;
    let crc16_end = size_end + crc16_bytes.len();
    let data_start = HEADER_LEN;

    (out_buf)[0..valid_end].copy_from_slice(&*valid_bytes);
    (out_buf)[size_start..size_end].copy_from_slice(&*size_bytes);
    (out_buf)[crc16_start..crc16_end].copy_from_slice(&crc16_bytes);

    if buf.len() > 0 {
        (out_buf)[data_start..].copy_from_slice(buf);
    }

    out_buf
}

#[inline(always)]
fn zero_failed_buf(buf: &mut [u8]) {
    if buf.len() < HEADER_LEN {
        panic!("somehow zeroing a buf shorter than HEADER_LEN");
    }

    // zero the valid byte, and the bytes after the size in the header
    buf[0] = 0;
    for mut c in buf[5..].iter_mut() {
        *c = 0;
    }
}

mod bit_ops {
    #[inline(always)]
    pub fn is_sealed(v: u32) -> bool {
        v >> 31 == 1
    }

    #[inline(always)]
    pub fn mk_sealed(v: u32) -> u32 {
        v | 1 << 31
    }

    #[inline(always)]
    pub fn n_writers(v: u32) -> u32 {
        v << 1 >> 25
    }

    #[inline(always)]
    pub fn incr_writers(v: u32) -> u32 {
        assert!(n_writers(v) != 127);
        v + (1 << 24)
    }

    #[inline(always)]
    pub fn decr_writers(v: u32) -> u32 {
        assert!(n_writers(v) != 0);
        v - (1 << 24)
    }

    #[inline(always)]
    pub fn offset(v: u32) -> u32 {
        v << 8 >> 8
    }

    #[inline(always)]
    pub fn bump_offset(v: u32, by: u32) -> u32 {
        assert!(by >> 24 == 0);
        v + by
    }
}

pub struct MemLog {
    inner: LockFreeLog,
}

impl MemLog {
    pub fn new() -> MemLog {
        let log = LockFreeLog::start_system("/dev/shm/__rsdb_memory.log".to_owned());
        MemLog { inner: log }
    }
}

impl Log for MemLog {
    fn reserve(&self, buf: Vec<u8>) -> Reservation {
        self.inner.reserve(buf)
    }

    fn write(&self, buf: Vec<u8>) -> LogID {
        self.inner.write(buf)
    }

    fn read(&self, id: LogID) -> io::Result<Option<Vec<u8>>> {
        self.inner.read(id)
    }

    fn stable_offset(&self) -> LogID {
        self.inner.stable_offset()
    }

    fn make_stable(&self, id: LogID) {
        self.inner.make_stable(id);
    }

    fn punch_hole(&self, id: LogID) {
        self.inner.punch_hole(id);
    }
}
