use std::fmt::{self, Debug};
use std::fs;
use std::io::{self, Read, Write, Seek, SeekFrom, Error, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::cell::{UnsafeCell, RefCell};
use std::thread;
use std::sync::mpsc::{channel, Sender};
use std::os::unix::io::AsRawFd;

use crossbeam::sync::MsQueue;
use libc::{fallocate, FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE};

use super::*;

const HEADER_LEN: usize = 7;
const MAX_BUF_SZ: usize = 1_000_000;
// FIXME N_BUFS being so high is a bandaid for avoiding livelock at wraparound
const N_BUFS: usize = 77;
const N_WRITER_THREADS: usize = 3;

#[derive(Debug, Clone, Eq, PartialEq, RustcDecodable, RustcEncodable)]
#[repr(C)]
pub enum LogDelta {
    Page,
    Merge {
        left: PageID,
        right: PageID,
    },
    Split {
        left: PageID,
        right: PageID,
    },
    FailedFlush, // on-disk only
}

#[derive(Debug, Clone, Eq, PartialEq, RustcDecodable, RustcEncodable)]
#[repr(C)]
pub struct LogPage;

#[derive(Debug, Clone, Eq, PartialEq, RustcDecodable, RustcEncodable)]
#[repr(C)]
pub enum LogData {
    Full(LogPage),
    Deltas(Vec<LogDelta>),
}

pub struct Log {
    iobufs: Arc<IOBufs>,
    stable: Arc<AtomicUsize>,
    path: String,
    file: RefCell<fs::File>,
}

unsafe impl Send for Log {}

impl Clone for Log {
    fn clone(&self) -> Log {
        Log {
            iobufs: self.iobufs.clone(),
            stable: self.stable.clone(),
            path: self.path.clone(),
            file: RefCell::new(open_log_for_punching(self.path.clone())),
        }
    }
}

impl Log {
    /// create new lock-free log
    pub fn start_system(path: String) -> Log {
        let stable = Arc::new(AtomicUsize::new(0));
        let q = Arc::new(MsQueue::new());
        let lw = LogWriter::new(path.clone(), q.clone(), stable.clone());
        let offset = lw.open_offset;

        thread::Builder::new()
            .name("LogWriter".to_string())
            .spawn(move || {
                lw.run();
            })
            .unwrap();

        Log {
            iobufs: Arc::new(IOBufs::new(q, offset as usize)),
            stable: stable,
            path: path.clone(),
            file: RefCell::new(open_log_for_punching(path)),
        }
    }

    /// claim a spot on disk, which can later be filled or aborted
    pub fn reserve(&self, sz: usize) -> Reservation {
        assert_eq!(sz >> 32, 0);
        assert!(sz <= MAX_BUF_SZ - HEADER_LEN);
        self.iobufs.reserve(sz as u32)
    }

    /// write a buffer to disk
    pub fn write(&self, buf: &[u8]) -> LogID {
        self.iobufs.write(buf)
    }

    /// read a buffer from the disk
    pub fn read(&self, id: LogID) -> io::Result<Option<Vec<u8>>> {
        let mut f = self.file.borrow_mut();
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
    pub fn stable_offset(&self) -> LogID {
        self.stable.load(Ordering::SeqCst) as LogID
    }

    /// blocks until the specified id has been made stable on disk
    pub fn make_stable(&self, id: LogID) {
        let mut spins = 0;
        loop {
            spins += 1;
            if spins > 2000000 {
                println!("{:?} have spun >2000000x in make_stable",
                         thread::current().name());
                spins = 0;
            }
            let cur = self.stable.load(Ordering::SeqCst) as LogID;
            // println!("cur {} id {}", cur, id);
            if cur > id {
                return;
            }
        }
    }

    /// shut down the writer threads
    pub fn shutdown(self) {
        self.iobufs.clone().shutdown();
    }

    /// deallocates the data part of a log id
    pub fn punch_hole(&self, id: LogID) {
        // we zero out the valid byte, and use fallocate to punch a hole
        // in the actual data, but keep the len for recovery.
        let mut f = self.file.borrow_mut();
        // zero out valid bit
        f.seek(SeekFrom::Start(id)).unwrap();
        let zeros = vec![0];
        f.write_all(&*zeros).unwrap();
        f.seek(SeekFrom::Start(id + 1)).unwrap();
        let mut len_buf = [0u8; 4];
        f.read_exact(&mut len_buf).unwrap();

        let len = ops::array_to_usize(len_buf);
        let mode = FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE;
        let fd = f.as_raw_fd();

        unsafe {
            // 5 is valid (1) + len (4), 2 is crc16
            fallocate(fd, mode, id as i64 + 5, len as i64 + 2);
        }
    }
}

fn open_log_for_punching(path: String) -> fs::File {
    let mut options = fs::OpenOptions::new();
    options.create(true);
    options.read(true);
    options.write(true);
    // TODO make logfile configurable
    options.open(path).unwrap()
}

#[derive(Clone)]
struct IOBufs {
    bufs: Vec<Arc<UnsafeCell<Vec<u8>>>>,
    headers: Vec<Arc<AtomicUsize>>,
    log_offsets: Vec<Arc<AtomicUsize>>,
    current_buf: Arc<AtomicUsize>,
    written_bufs: Arc<AtomicUsize>,
    plunger: Arc<MsQueue<ResOrShutdown>>,
    shutdown: Arc<AtomicBool>,
}

impl Debug for IOBufs {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let current_buf = self.current_buf.load(Ordering::SeqCst);
        let written = self.written_bufs.load(Ordering::SeqCst);
        let slow_writers = current_buf - written >= N_BUFS;
        let idx = current_buf % N_BUFS;
        // load current header value
        let header = self.headers[idx].clone();
        let hv = header.load(Ordering::SeqCst) as u32;
        let n_writers = ops::n_writers(hv);
        let offset = ops::offset(hv);
        let sealed = ops::is_sealed(hv);

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

#[derive(Clone)]
pub struct Reservation {
    base_disk_offset: LogID,
    res_len: u32, // this may be different from header, due to concurrent access
    buf_offset: u32,
    buf: Arc<UnsafeCell<Vec<u8>>>,
    last_hv: u32, // optimization to avoid more atomic loads
    header: Arc<AtomicUsize>,
    next_header: Arc<AtomicUsize>,
    current_buf: Arc<AtomicUsize>,
    written_bufs: Arc<AtomicUsize>,
    plunger: Arc<MsQueue<ResOrShutdown>>,
    idx: usize,
    cur_log_offset: Arc<AtomicUsize>,
    next_log_offset: Arc<AtomicUsize>,
}

unsafe impl Send for Reservation {}

impl IOBufs {
    fn new(plunger: Arc<MsQueue<ResOrShutdown>>, disk_offset: usize) -> IOBufs {
        let current_buf = 1;
        let bufs = rep_no_copy![Arc::new(UnsafeCell::new(vec![0; MAX_BUF_SZ])); N_BUFS];
        let headers = rep_no_copy![Arc::new(AtomicUsize::new(0)); N_BUFS];
        let log_offsets = rep_no_copy![Arc::new(AtomicUsize::new(std::usize::MAX)); N_BUFS];
        log_offsets[current_buf].store(disk_offset, Ordering::SeqCst);
        IOBufs {
            bufs: bufs,
            headers: headers,
            log_offsets: log_offsets,
            current_buf: Arc::new(AtomicUsize::new(current_buf)),
            written_bufs: Arc::new(AtomicUsize::new(0)),
            plunger: plunger,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    fn reserve(&self, len: u32) -> Reservation {
        let len = len + HEADER_LEN as u32;
        let mut spins = 0;
        loop {
            spins += 1;
            // load atomic progress counters
            let written = self.written_bufs.load(Ordering::SeqCst);
            let current_buf = self.current_buf.load(Ordering::SeqCst);
            let idx = current_buf % N_BUFS;
            if spins > 100_000 {
                println!("{:?} have spun >100,000x in reserve, idx {}",
                         thread::current().name(),
                         idx);
                spins = 0;
            }

            // println!("using buf {}", idx);

            // if written is too far behind, we need to
            // spin while it catches up to avoid overlap
            if current_buf - written + 1 >= N_BUFS {
                // println!("writers are behind: {:?}", self);
                continue;
            }

            // load current header value
            let header = self.headers[idx].clone();
            let mut hv = header.load(Ordering::SeqCst) as u32;

            // skip if already sealed
            if ops::is_sealed(hv) {
                // already sealed, start over and hope cur
                // has already been bumped by sealer.
                // println!("cur is late to be bumped: {:?}", self);
                continue;
            }

            // try to claim space, seal otherwise
            let buf_offset = ops::offset(hv);
            if buf_offset + len > MAX_BUF_SZ as u32 {
                // attempt seal once, flush if no active writers, then start over
                match seal_header_and_bump_offsets(&header,
                                                   &*self.headers[(idx + 1) % N_BUFS],
                                                   hv,
                                                   &*self.log_offsets[idx],
                                                   &*self.log_offsets[(idx + 1) % N_BUFS],
                                                   &*self.current_buf) {
                    Ok(h) if ops::n_writers(h) == 0 => {
                        // nobody else is going to flush this, so we need to

                        // println!("creating zero-writer res to clear buf");
                        assert_ne!(self.log_offsets[idx].load(Ordering::SeqCst),
                                   std::usize::MAX);
                        let res = self.reservation(buf_offset, 0, idx, h);
                        res.decr_writers_maybe_slam();
                    }
                    _ => {}
                }
                continue;
            }

            // attempt to claim
            let claimed = ops::incr_writers(ops::bump_offset(hv, len));
            assert!(!ops::is_sealed(claimed));

            let cas_hv = header.compare_and_swap(hv as usize, claimed as usize, Ordering::SeqCst);
            if cas_hv != hv as usize {
                // CAS failed, start over
                continue;
            }
            if ops::n_writers(hv) == 0 {
                // println!("using idx {}, went from {} to {} writers, offset {} to {}", idx, ops::n_writers(hv), ops::n_writers(claimed), ops::offset(hv), ops::offset(claimed));
            }
            hv = claimed;

            // if we're giving out a reservation,
            // the writer count should be positive
            assert!(ops::n_writers(hv) != 0);

            assert_ne!(self.log_offsets[idx].load(Ordering::SeqCst),
                       std::usize::MAX);
            return self.reservation(buf_offset, len, idx, hv);
        }
    }

    fn reservation(&self, buf_offset: u32, len: u32, idx: usize, last_hv: u32) -> Reservation {
        return Reservation {
            base_disk_offset: self.log_offsets[idx].load(Ordering::SeqCst) as LogID,
            res_len: len,
            buf_offset: buf_offset,
            buf: self.bufs[idx].clone(),
            last_hv: last_hv,
            header: self.headers[idx].clone(),
            next_header: self.headers[(idx + 1) % N_BUFS].clone(),
            current_buf: self.current_buf.clone(),
            written_bufs: self.written_bufs.clone(),
            plunger: self.plunger.clone(),
            idx: idx,
            cur_log_offset: self.log_offsets[idx].clone(),
            next_log_offset: self.log_offsets[(idx + 1) % N_BUFS].clone(),
        };
    }

    fn write(&self, buf: &[u8]) -> LogID {
        let res = self.reserve(buf.len() as u32);
        res._write(buf, true)
    }

    fn shutdown(&self) {
        for _ in 0..N_WRITER_THREADS {
            self.plunger.push(ResOrShutdown::Shutdown);
        }
    }
}

impl Reservation {
    /// cancel the reservation, placing a failed flush on disk
    pub fn abort(self) {
        // fills lease with a FailedFlush
        self._seal();
        self._write(&[], false);
    }

    /// complete the reservation, placing the buffer on disk at the log_id
    pub fn complete(self, buf: &[u8]) -> LogID {
        self._seal();
        self._write(buf, true)
    }

    /// get the log_id for accessing this buffer in the future
    pub fn log_id(&self) -> LogID {
        self.base_disk_offset
    }

    fn len(&self) -> usize {
        self.res_len as usize
    }

    // NB this should only be called from here
    fn _seal(&self) {
        let mut hv = self.last_hv;
        while !ops::is_sealed(hv) &&
              seal_header_and_bump_offsets(&*self.header,
                                           &*self.next_header,
                                           hv,
                                           &*self.cur_log_offset,
                                           &*self.next_log_offset,
                                           &*self.current_buf)
            .is_err() {
            hv = self.header.load(Ordering::SeqCst) as u32;
        }
    }

    fn _write(self, buf: &[u8], valid: bool) -> LogID {
        let mut out_buf = unsafe { (*self.buf.get()).as_mut_slice() };

        let size_bytes = ops::usize_to_array(self.len() - HEADER_LEN).to_vec();
        let (valid_bytes, crc16_bytes) = if valid {
            (vec![1u8], crc16_arr(&buf))
        } else {
            (vec![0u8], [0u8; 2])
        };

        let start = self.buf_offset as usize;
        let valid_start = start;
        let valid_end = start + valid_bytes.len();
        let size_start = valid_end;
        let size_end = valid_end + size_bytes.len();
        let crc16_start = size_end;
        let crc16_end = size_end + crc16_bytes.len();
        let data_start = start + HEADER_LEN;
        let data_end = start + self.len(); // NB self.len() includes HEADER_LEN

        (out_buf)[valid_start..valid_end].copy_from_slice(&*valid_bytes);
        (out_buf)[size_start..size_end].copy_from_slice(&*size_bytes);
        (out_buf)[crc16_start..crc16_end].copy_from_slice(&crc16_bytes);

        if buf.len() > 0 && valid {
            assert_eq!(buf.len() + HEADER_LEN, self.res_len as usize);
            (out_buf)[data_start..data_end].copy_from_slice(buf);
        } else if !valid {
            assert_eq!(buf.len(), 0);
            // no need to actually write zeros, the next seek will punch a hole
        }

        self.decr_writers_maybe_slam()
    }

    fn decr_writers_maybe_slam(self) -> LogID {
        let mut hv = self.last_hv;

        assert_ne!(self.base_disk_offset as usize,
                   std::usize::MAX,
                   "created reservation for uninitialized slot");
        let ret = self.base_disk_offset + self.buf_offset as LogID;

        // FIXME this feels broken, but maybe isn't because we
        // can never be sealed and increase writers?
        if ops::n_writers(hv) == 0 && ops::is_sealed(hv) {
            // println!("slamming no-writer buf down pipe, idx {}", self.idx);
            self.slam_down_pipe();
            return ret;
        }

        // decr writer count, retrying
        let mut spins = 0;
        loop {
            spins += 1;
            if spins > 1000 {
                println!("{:?} have spun >1000x in decr", thread::current().name());
                spins = 0;
            }
            let new_hv = ops::decr_writers(hv) as usize;
            let old_hv = self.header.compare_and_swap(hv as usize, new_hv, Ordering::SeqCst);
            if old_hv == hv as usize {
                if ops::n_writers(new_hv as u32) == 0 {
                    // println!("decr succeeded from {} to {} on index {}", ops::n_writers(hv), ops::n_writers(new_hv as u32), self.idx);
                }

                if ops::n_writers(new_hv as u32) == 0 && ops::is_sealed(new_hv as u32) {
                    // println!("slamming our buf down pipe, idx {}", self.idx);
                    self.slam_down_pipe();
                }

                return ret;
            }

            // we failed to decr, reload and retry
            hv = old_hv as u32;

            // if this is 0, it means too many decr's have happened
            // or too few incr's have happened
            assert_ne!(ops::n_writers(hv), 0);
        }
    }

    fn slam_down_pipe(self) {
        let plunger = self.plunger.clone();
        plunger.push(ResOrShutdown::Res(self));
    }

    fn stabilize(&self, log: &mut fs::File) -> LogID {
        // put the buf identified by idx on disk
        let data = unsafe { (*self.buf.get()).as_mut_slice() };
        let data_bytes = &data[0..self.res_len as usize];
        log.seek(SeekFrom::Start(self.base_disk_offset)).unwrap();
        log.write_all(&data_bytes).unwrap();

        assert_eq!(self.log_id(),
                   self.base_disk_offset as u64,
                   "disk offset should be 1:1 with log id");

        // TODO this really shouldn't be necessary, but
        // asserts are still failing this "taint"
        self.cur_log_offset.store(std::usize::MAX, Ordering::SeqCst);

        // println!("deinitialized idx {}", self.idx);

        // bump self.written by 1
        self.written_bufs.fetch_add(1, Ordering::SeqCst);
        // println!("writer offset now {}", (new_writer_offset + 1) % N_BUFS);

        self.base_disk_offset
    }
}

struct LogWriter {
    receiver: Arc<MsQueue<ResOrShutdown>>,
    open_offset: LogID,
    stable: Arc<AtomicUsize>,
    path: String,
}

impl LogWriter {
    fn new(path: String,
           receiver: Arc<MsQueue<ResOrShutdown>>,
           stable: Arc<AtomicUsize>)
           -> LogWriter {
        // TODO make log file configurable
        // NB we make the default ID 1 so that we can use 0 as a null LogID in
        // AtomicUsize's elsewhere throughout the codebase

        let cur_id = fs::metadata(path.clone()).map(|m| m.len()).unwrap_or(0);
        stable.store(cur_id as usize, Ordering::SeqCst);

        LogWriter {
            receiver: receiver,
            open_offset: cur_id, // + 1, // we add 1 here to add space on startup from stable
            stable: stable,
            path: path,
        }
    }

    fn run(self) {
        let mut written_intervals = vec![];

        let (interval_tx, interval_rx) = channel();

        for i in 0..N_WRITER_THREADS {
            let name = format!("log IO writer {}", i);
            let path = self.path.clone();
            let rx = self.receiver.clone();
            let tx = interval_tx.clone();
            thread::Builder::new()
                .name(name)
                .spawn(move || {
                    process_reservation(path, rx, tx);
                })
                .unwrap();
        }

        for interval in interval_rx.iter() {
            written_intervals.push(interval);
            written_intervals.sort();

            while let Some(&(low, high)) = written_intervals.get(0) {
                let cur_stable = self.stable.load(Ordering::SeqCst) as u64;
                // println!("cs: {}, low: {}, high: {}, n_pending: {}", cur_stable, low, high, written_intervals.len());
                // println!("{:?}", written_intervals);

                if cur_stable == low {
                    // println!("bumping");
                    let old = self.stable.swap(high as usize, Ordering::SeqCst);
                    assert_eq!(old, cur_stable as usize);
                    written_intervals.remove(0);
                } else {
                    // println!("break!");
                    break;
                }
            }
        }
    }
}

enum ResOrShutdown {
    Res(Reservation),
    Shutdown,
}

#[inline(always)]
fn seal_header_and_bump_offsets(header: &AtomicUsize,
                                next_header: &AtomicUsize,
                                hv: u32,
                                cur_log_offset: &AtomicUsize,
                                next_log_offset: &AtomicUsize,
                                current_buf: &AtomicUsize)
                                -> Result<u32, ()> {
    if ops::is_sealed(hv) {
        // don't want to double seal, since we should change critical offsets only once
        return Err(());
    }
    let sealed = ops::mk_sealed(hv);
    if header.compare_and_swap(hv as usize, sealed as usize, Ordering::SeqCst) == hv as usize {
        // println!("sealed buf with {} writers", ops::n_writers(sealed));
        // We succeeded in setting seal,
        // so we get to bump cur and log_offset.

        // NB This is effectively a global lock until self.current_buf gets bumped.

        // NB Set next offset before bumping self.current_buf.

        // Also note that written_bufs may be incremented before we reach
        // the increment of current_buf below, as a writing thread
        // sees the seal. This is why we initialize current_buf to 1 and
        // written_bufs to 0.

        let our_log_offset = cur_log_offset.load(Ordering::SeqCst);
        assert_ne!(our_log_offset, std::usize::MAX);

        let next_offset = our_log_offset + ops::offset(sealed) as usize;

        // !! setup new slot
        next_header.store(0, Ordering::SeqCst);

        let mut spins = 0;
        loop {
            spins += 1;
            if spins > 100_000 {
                println!("{:?} have spun >100,000x in seal of buf {}",
                         thread::current().name(),
                         ops::n_writers(sealed));
                spins = 0;
            }
            // FIXME panicked at 'assertion failed: `(left == right)` (left: `2089005254`, right:
            // `18446744073709551615`)
            let old =
                next_log_offset.compare_and_swap(std::usize::MAX, next_offset, Ordering::SeqCst);
            if old == std::usize::MAX {
                break;
            }
        }

        // !! open new slot
        current_buf.fetch_add(1, Ordering::SeqCst);
        // println!("setting next buf to {}", (next_buf + 1) % N_BUFS);

        Ok(sealed)
    } else {
        Err(())
    }
}

#[inline(always)]
fn process_reservation(path: String,
                       res_rx: Arc<MsQueue<ResOrShutdown>>,
                       interval_tx: Sender<(LogID, LogID)>) {
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true);
    let mut log = options.open(path).unwrap();
    loop {
        let res_or_shutdown = res_rx.pop();
        match res_or_shutdown {
            ResOrShutdown::Res(res) => {
                // println!("logwriter starting write of idx {}", res.idx);
                let header = res.header.load(Ordering::SeqCst);
                let interval = (res.base_disk_offset,
                                res.base_disk_offset + ops::offset(header as u32) as u64);
                // println!("disk_offset: {} len: {} buf_offset: {}", res.base_disk_offset, res.len(), res.buf_offset);

                res.stabilize(&mut log);

                interval_tx.send(interval).unwrap();
                // println!("finished writing idx of {}", res.idx);
            }
            ResOrShutdown::Shutdown => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn more_reservations_than_buffers() {
        let log = Log::start_system("test_more_reservations_than_buffers.log".to_owned());
        let mut reservations = vec![];
        for i in 0..N_BUFS + 1 {
            reservations.push(log.reserve(MAX_BUF_SZ - HEADER_LEN))
        }
        for res in reservations.into_iter().rev() {
            // abort in reverse order
            res.abort();
        }
        log.shutdown();
    }

    #[test]
    fn non_contiguous_flush() {
        let log = Log::start_system("test_non_contiguous_flush.log".to_owned());
        let res1 = log.reserve(MAX_BUF_SZ - HEADER_LEN);
        let res2 = log.reserve(MAX_BUF_SZ - HEADER_LEN);
        let id = res2.log_id();
        res2.abort();
        res1.abort();
        log.make_stable(id);
        log.shutdown();
    }

    #[test]
    fn basic_functionality() {
        // TODO linearize res bufs, verify they are correct
        let log = Log::start_system("test_basic_functionality.log".to_owned());
        let iobs2 = log.clone();
        let iobs3 = log.clone();
        let iobs4 = log.clone();
        let iobs5 = log.clone();
        let iobs6 = log.clone();
        let log7 = log.clone();
        let t1 = thread::Builder::new()
            .name("c1".to_string())
            .spawn(move || {
                for i in 0..5_000 {
                    let buf = vec![1; i % 8192];
                    log.write(&*buf);
                }
            })
            .unwrap();
        let t2 = thread::Builder::new()
            .name("c2".to_string())
            .spawn(move || {
                for i in 0..5_000 {
                    let buf = vec![2; i % 8192];
                    iobs2.write(&*buf);
                }
            })
            .unwrap();
        let t3 = thread::Builder::new()
            .name("c3".to_string())
            .spawn(move || {
                for i in 0..5_000 {
                    let buf = vec![3; i % 8192];
                    iobs3.write(&*buf);
                }
            })
            .unwrap();
        let t4 = thread::Builder::new()
            .name("c4".to_string())
            .spawn(move || {
                for i in 0..5_000 {
                    let buf = vec![4; i % 8192];
                    iobs4.write(&*buf);
                }
            })
            .unwrap();
        let t5 = thread::Builder::new()
            .name("c5".to_string())
            .spawn(move || {
                for i in 0..5_000 {
                    let buf = vec![5; i % 8192];
                    iobs5.write(&*buf);
                }
            })
            .unwrap();

        let t6 = thread::Builder::new()
            .name("c6".to_string())
            .spawn(move || {
                for i in 0..5_000 {
                    let buf = vec![6; i % 8192];
                    let res = iobs6.reserve(buf.len());
                    let id = res.log_id();
                    res.complete(&*buf);
                    iobs6.make_stable(id);
                }
            })
            .unwrap();


        t1.join();
        t2.join();
        t3.join();
        t4.join();
        t5.join();
        t6.join();
        log7.shutdown();
    }

    fn test_write(log: &Log) {
        let data_bytes = b"yoyoyoyo";
        let res = log.reserve(data_bytes.len());
        let id = res.log_id();
        res.complete(data_bytes);
        log.make_stable(id);
        let read_buf = log.read(id).unwrap().unwrap();
        assert_eq!(read_buf, data_bytes);
    }

    fn test_abort(log: &Log) {
        let res = log.reserve(5);
        let id = res.log_id();
        res.abort();
        log.make_stable(id);
        match log.read(id) {
            Ok(None) => (), // good
            _ => {
                panic!("sucessfully read an aborted request! BAD! SAD!");
            }
        }
    }

    #[test]
    fn test_log_aborts() {
        let log = Log::start_system("test_aborts.log".to_owned());
        test_write(&log);
        test_abort(&log);
        test_write(&log);
        test_abort(&log);
        test_write(&log);
        test_abort(&log);
        log.shutdown();
    }

    #[test]
    fn test_hole_punching() {
        let log = Log::start_system("test_hole_punching.log".to_owned());

        let data_bytes = b"yoyoyoyo";
        let res = log.reserve(data_bytes.len());
        let id = res.log_id();
        res.complete(data_bytes);
        log.make_stable(id);
        log.read(id).unwrap();

        log.punch_hole(id);

        assert_eq!(log.read(id).unwrap(), None);

        // TODO figure out if physical size of log is actually smaller now

        log.shutdown();
    }
}
