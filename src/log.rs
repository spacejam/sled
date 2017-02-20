use std::fmt::{self, Debug};
use std::fs;
use std::io::{self, Read, Write, Seek, SeekFrom, Error, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::{UnsafeCell, RefCell};
use std::thread;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::os::unix::io::AsRawFd;

use libc::{fallocate, FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE};

use super::*;

const HEADER_LEN: usize = 7;
const MAX_BUF_SZ: usize = 1_000_000;

thread_local! {
    static LOCAL_READER: RefCell<fs::File> = RefCell::new(open_log_for_reading());
    static LOCAL_PUNCHER: RefCell<fs::File> = RefCell::new(open_log_for_punching());
}

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
#[derive(Clone)]
pub struct Log {
    iobufs: IOBufs,
    stable: Arc<AtomicUsize>,
}

unsafe impl Send for Log {}

unsafe impl Sync for Log {}

impl Log {
    pub fn start_system() -> Log {
        let stable = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = channel();
        let lw = LogWriter::new(rx, stable.clone());
        let offset = lw.open_offset;
        thread::spawn(move || {
            lw.run();
        });
        Log {
            iobufs: IOBufs::new(tx, offset as usize),
            stable: stable,
        }
    }

    pub fn reserve(&self, sz: usize) -> Reservation {
        assert_eq!(sz >> 32, 0);
        assert!(sz <= MAX_BUF_SZ - HEADER_LEN);
        self.iobufs.reserve(sz as u32)
    }

    pub fn write(&self, buf: Vec<u8>) -> LogID {
        self.iobufs.write(buf)
    }

    pub fn read(&self, id: LogID) -> io::Result<LogData> {
        LOCAL_READER.with(|f| {
            let mut f = f.borrow_mut();
            f.seek(SeekFrom::Start(id))?;
            let mut valid = [0u8; 1];
            f.read_exact(&mut valid)?;
            if valid[0] == 0 {
                return Err(Error::new(ErrorKind::Other, "tried to read failed flush"));
            }
            let mut len_buf = [0u8; 4];
            f.read_exact(&mut len_buf)?;
            let len = ops::array_to_usize(len_buf);
            let mut crc16_buf = [0u8; 2];
            f.read_exact(&mut crc16_buf)?;
            let mut buf = Vec::with_capacity(len);
            unsafe {
                buf.set_len(len);
            }
            f.read_exact(&mut buf)?;
            if crc16_arr(&buf) != crc16_buf {
                return Err(Error::new(ErrorKind::Other, "read data failed crc16 checksum"));
            }
            ops::from_binary::<LogData>(buf)
                .map_err(|_| Error::new(ErrorKind::Other, "failed to deserialize LogData"))
        })
    }

    pub fn make_stable(&self, id: LogID) {
        loop {
            // FIXME this is buggy, because log segments are not
            // necessarily flushed to disk in-order
            // solutions:
            //  1. force serial writing, eat perf hit
            //  2. enable non-contiguous id tracking, eat complexity hit
            //  TODO investigate mitigations to downsides of each...
            let cur = self.stable.load(Ordering::SeqCst) as LogID;
            if cur >= id {
                return;
            }
        }
    }

    pub fn shutdown(self) {
        self.iobufs.shutdown();
    }

    pub fn punch_hole(&self, id: LogID) {
        LOCAL_PUNCHER.with(|f| {
            let mut f = f.borrow_mut();
            f.seek(SeekFrom::Start(id + 1)).unwrap();
            let mut len_buf = [0u8; 4];
            f.read_exact(&mut len_buf).unwrap();

            let len = ops::array_to_usize(len_buf);
            let mode = FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE;
            let fd = f.as_raw_fd();

            unsafe {
                fallocate(fd, mode, id as i64 + HEADER_LEN as i64, len as i64);
            }
        });
    }
}

fn open_log_for_reading() -> fs::File {
    let mut options = fs::OpenOptions::new();
    options.create(true);
    options.read(true);
    // TODO make logfile configurable
    options.open("rsdb.log").unwrap()
}

fn open_log_for_punching() -> fs::File {
    let mut options = fs::OpenOptions::new();
    options.create(true);
    options.read(true);
    options.write(true);
    // TODO make logfile configurable
    options.open("rsdb.log").unwrap()
}

#[derive(Clone)]
struct IOBufs {
    bufs: Vec<Arc<UnsafeCell<Vec<u8>>>>,
    headers: Vec<Arc<AtomicUsize>>,
    log_offsets: Vec<Arc<AtomicUsize>>,
    current_buf: Arc<AtomicUsize>,
    written_bufs: Arc<AtomicUsize>,
    plunger: Sender<ResOrShutdown>,
}

impl Debug for IOBufs {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let current_buf = self.current_buf.load(Ordering::SeqCst);
        let written = self.written_bufs.load(Ordering::SeqCst);
        let slow_writers = current_buf - written >= N_TX_THREADS;
        let idx = current_buf % N_TX_THREADS;
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

unsafe impl Sync for IOBufs {}

#[derive(Clone)]
pub struct Reservation {
    base_disk_offset: LogID,
    res_len: u32, // this may be different from header, due to concurrent access
    buf_offset: u32,
    buf: Arc<UnsafeCell<Vec<u8>>>,
    last_hv: u32, // optimization to avoid more atomic loads
    header: Arc<AtomicUsize>,
    current_buf: Arc<AtomicUsize>,
    written_bufs: Arc<AtomicUsize>,
    plunger: Sender<ResOrShutdown>,
    idx: usize,
    cur_log_offset: Arc<AtomicUsize>,
    next_log_offset: Arc<AtomicUsize>,
}

unsafe impl Send for Reservation {}

impl IOBufs {
    fn new(plunger: Sender<ResOrShutdown>, disk_offset: usize) -> IOBufs {
        let bufs = rep_no_copy![Arc::new(UnsafeCell::new(vec![0; MAX_BUF_SZ])); N_TX_THREADS];
        let headers = rep_no_copy![Arc::new(AtomicUsize::new(0)); N_TX_THREADS];
        let log_offsets = rep_no_copy![Arc::new(AtomicUsize::new(disk_offset)); N_TX_THREADS];
        IOBufs {
            bufs: bufs,
            headers: headers,
            log_offsets: log_offsets,
            // NB we set current_buf one ahead of written_bufs
            // to account for the fact that written indicates number
            // of completed writes, and we have not yet completed
            // writing #1.
            current_buf: Arc::new(AtomicUsize::new(1)),
            written_bufs: Arc::new(AtomicUsize::new(0)),
            plunger: plunger,
        }
    }

    fn reserve(&self, len: u32) -> Reservation {
        let len = len + HEADER_LEN as u32;
        loop {
            // load atomic progress counters
            let current_buf = self.current_buf.load(Ordering::SeqCst);
            let written = self.written_bufs.load(Ordering::SeqCst);
            let idx = current_buf % N_TX_THREADS;

            // println!("using buf {}", idx);

            // if written is too far behind, we need to
            // spin while it catches up to avoid overlap
            if current_buf - written >= N_TX_THREADS {
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
            let offset = ops::offset(hv);
            if offset + len > MAX_BUF_SZ as u32 {
                // attempt seal once, then start over
                // NB we also bump writers here to simplify write() logic
                let sealed = ops::mk_sealed(hv);
                if header.compare_and_swap(hv as usize, sealed as usize, Ordering::SeqCst) ==
                   hv as usize {
                    // println!("- buf {} sealed with {} writers", idx, ops::n_writers(sealed));
                    // We succeeded in setting seal,
                    // so we get to bump cur and log_offset.

                    // NB This is effectively a global lock until self.current_buf gets bumped.

                    // NB Set next offset before bumping self.current_buf.

                    // Also note that written_bufs may be incremented before we reach
                    // the increment of current_buf below, as a writing thread
                    // sees the seal. This is why we initialize current_buf to 1 and
                    // written_bufs to 0.
                    let new_log_offset = self.log_offsets[idx].load(Ordering::SeqCst) +
                                         offset as usize;
                    self.log_offsets[(idx + 1) % N_TX_THREADS]
                        .store(new_log_offset, Ordering::SeqCst);
                    self.current_buf.fetch_add(1, Ordering::SeqCst);

                    // if writers is now 1 (from our previous incr),
                    // it's our responsibility to flush it
                    if ops::n_writers(sealed) == 0 {
                        // nobody else is going to flush this, so we need to
                        let res = self.reservation(offset, HEADER_LEN as u32, idx, sealed);
                        res.write(vec![], true);
                    }
                }
                continue;
            } else {
                // attempt to claim
                let claimed = ops::incr_writers(ops::bump_offset(hv, len));
                if header.compare_and_swap(hv as usize, claimed as usize, Ordering::SeqCst) !=
                   hv as usize {
                    // CAS failed, start over
                    continue;
                }
                hv = claimed;
            }

            // if we're giving out a reservation,
            // the writer count should be positive
            assert!(ops::n_writers(hv) != 0);

            return self.reservation(offset, len, idx, hv);
        }
    }

    fn reservation(&self, offset: u32, len: u32, idx: usize, last_hv: u32) -> Reservation {
        return Reservation {
            base_disk_offset: self.log_offsets[idx].load(Ordering::SeqCst) as LogID,
            res_len: len,
            buf_offset: offset,
            buf: self.bufs[idx].clone(),
            last_hv: last_hv,
            header: self.headers[idx].clone(),
            current_buf: self.current_buf.clone(),
            written_bufs: self.written_bufs.clone(),
            plunger: self.plunger.clone(),
            idx: idx,
            cur_log_offset: self.log_offsets[idx].clone(),
            next_log_offset: self.log_offsets[(idx + 1) % N_TX_THREADS].clone(),
        };
    }

    pub fn write(&self, buf: Vec<u8>) -> LogID {
        let res = self.reserve(buf.len() as u32);
        res.write(buf, true)
    }

    pub fn shutdown(self) {
        self.plunger.send(ResOrShutdown::Shutdown);
    }
}

impl Reservation {
    pub fn abort(self) {
        // fills lease with a FailedFlush
        self.seal();
        self.write(vec![], false);
    }

    pub fn complete(self, buf: Vec<u8>) -> LogID {
        self.seal();
        self.write(buf, true)
    }

    fn len(&self) -> usize {
        self.res_len as usize
    }

    fn seal(&self) {
        self.header.fetch_or(1 << 31, Ordering::SeqCst);
        let new_log_offset = self.cur_log_offset.load(Ordering::SeqCst) + self.len();
        self.next_log_offset.store(new_log_offset, Ordering::SeqCst);
        self.current_buf.fetch_add(1, Ordering::SeqCst);
    }

    fn write(self, buf: Vec<u8>, valid: bool) -> LogID {
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
        // FIXME "index 1000003 out of range for slice of length 1000000"
        (out_buf)[size_start..size_end].copy_from_slice(&*size_bytes);
        (out_buf)[crc16_start..crc16_end].copy_from_slice(&crc16_bytes);

        if buf.len() > 0 && valid {
            assert_eq!(buf.len() + HEADER_LEN, self.res_len as usize);
            (out_buf)[data_start..data_end].copy_from_slice(&*buf);
        } else if !valid {
            assert_eq!(buf.len(), 0);
            // no need to actually write zeros, the next seek will punch a hole
        }

        let mut hv = self.last_hv;

        let ret = self.base_disk_offset + self.buf_offset as LogID;

        if ops::n_writers(hv) == 0 && ops::is_sealed(hv) {
            self.slam_down_pipe();
            return ret;
        }

        // decr writer count, retrying
        loop {
            let hv2 = ops::decr_writers(hv);
            if self.header.compare_and_swap(hv as usize, hv2 as usize, Ordering::SeqCst) ==
               hv as usize {
                if ops::is_sealed(hv2) {
                    // println!("- decr worked on sealed buf {}, n_writers: {}", self.idx, ops::n_writers(hv2));
                }
                if ops::n_writers(hv2) == 0 && ops::is_sealed(hv2) {
                    self.slam_down_pipe();
                }
                return ret;
            }
            // we failed, reload and retry
            hv = self.header.load(Ordering::SeqCst) as u32;
            // if this is 0, it means too many decr's have happened
            // or too few incr's have happened
            assert_ne!(ops::n_writers(hv), 0);
        }
    }

    fn slam_down_pipe(self) {
        let plunger = self.plunger.clone();
        plunger.send(ResOrShutdown::Res(self)).unwrap();
    }

    fn stabilize(&self, log: &mut fs::File) -> LogID {
        // put the buf identified by idx on disk
        let data = unsafe { (*self.buf.get()).as_mut_slice() };
        let data_bytes = &data[0..self.res_len as usize];
        log.seek(SeekFrom::Start(self.base_disk_offset)).unwrap();
        log.write_all(&data_bytes).unwrap();

        // TODO fix mapping with offset prediction
        // assert_eq!(lid, self.base_disk_offset as u64);

        // reset the header, no need to zero the buf
        self.header.store(0, Ordering::SeqCst);

        // bump self.written by 1
        self.written_bufs.fetch_add(1, Ordering::SeqCst);
        // println!("+ buf {} stabilized, cur is {}", self.idx, self.current_buf.load(Ordering::SeqCst) % N_TX_THREADS);

        self.base_disk_offset
    }

    pub fn log_id(&self) -> LogID {
        self.base_disk_offset
    }
}

struct LogWriter {
    receiver: Receiver<ResOrShutdown>,
    log: fs::File,
    open_offset: LogID,
    stable: Arc<AtomicUsize>,
}

impl LogWriter {
    fn new(receiver: Receiver<ResOrShutdown>, stable: Arc<AtomicUsize>) -> LogWriter {
        // TODO make log file configurable
        // NB we make the default ID 1 so that we can use 0 as a null LogID in
        // AtomicUsize's elsewhere throughout the codebase

        let mut options = fs::OpenOptions::new();
        options.write(true).create(true);
        let file = options.open("rsdb.log").unwrap();
        let cur_id = file.metadata().map(|m| m.len()).unwrap_or(1);
        stable.store(cur_id as usize, Ordering::SeqCst);

        LogWriter {
            receiver: receiver,
            log: file,
            open_offset: cur_id + 1, // we add 1 here to add space on startup from stable
            stable: stable,
        }
    }

    fn run(mut self) {
        for res_or_shutdown in self.receiver.iter() {
            match res_or_shutdown {
                ResOrShutdown::Res(res) => {
                    let stable = res.stabilize(&mut self.log);
                    let old = self.stable.swap(stable as usize, Ordering::SeqCst);
                    // FIXME 'assertion failed: old <= (stable as usize)'
                    // triggered by using the shufnice.sh script in hack with cargo test
                    assert!(old <= stable as usize);
                }
                ResOrShutdown::Shutdown => return,
            }
        }
    }
}

enum ResOrShutdown {
    Res(Reservation),
    Shutdown,
}

#[test]
fn non_contiguous_flush() {
    let log = Log::start_system();
    let res1 = log.reserve(MAX_BUF_SZ - HEADER_LEN);
    let res2 = log.reserve(MAX_BUF_SZ - HEADER_LEN);
    res2.abort();
    res1.abort();
    log.shutdown();
}

#[test]
fn basic_functionality() {
    // TODO linearize res bufs, verify they are correct
    let log = Log::start_system();
    let iobs2 = log.clone();
    let iobs3 = log.clone();
    let iobs4 = log.clone();
    let iobs5 = log.clone();
    let iobs6 = log.clone();
    let t1 = thread::spawn(move || {
        for i in 0..5_000 {
            let buf = vec![1; i % 8192];
            log.write(buf);
        }
    });
    let t2 = thread::spawn(move || {
        for i in 0..5_000 {
            let buf = vec![2; i % 8192];
            iobs2.write(buf);
        }
    });
    let t3 = thread::spawn(move || {
        for i in 0..5_000 {
            let buf = vec![3; i % 8192];
            iobs3.write(buf);
        }
    });
    let t4 = thread::spawn(move || {
        for i in 0..5_000 {
            let buf = vec![4; i % 8192];
            iobs4.write(buf);
        }
    });
    let t5 = thread::spawn(move || {
        for i in 0..5_000 {
            let buf = vec![5; i % 8192];
            iobs5.write(buf);
        }
    });
    let t6 = thread::spawn(move || {
        for i in 0..5_000 {
            let buf = vec![6; i % 8192];
            iobs6.write(buf);
        }
    });
    t1.join().unwrap();
    t2.join().unwrap();
    t3.join().unwrap();
    t4.join().unwrap();
    t5.join().unwrap();
    t6.join().unwrap();
}

fn test_delta(log: &Log) {
    let deltablock = LogData::Deltas(vec![]);
    let data_bytes = ops::to_binary(&deltablock);
    let res = log.reserve(data_bytes.len());
    let id = res.log_id();
    res.complete(data_bytes);
    log.make_stable(id);
    let read = log.read(id).unwrap();
    assert_eq!(read, deltablock);
}

fn test_abort(log: &Log) {
    let res = log.reserve(5);
    let id = res.log_id();
    res.abort();
    log.make_stable(id);
    match log.read(id) {
        Err(_) => (), // good
        Ok(_) => {
            panic!("sucessfully read an aborted request! BAD! SAD!");
        }
    }
}

#[test]
fn log_rt() {
    let log = Log::start_system();
    test_delta(&log);
    test_abort(&log);
    test_delta(&log);
    test_abort(&log);
    test_delta(&log);
    test_abort(&log);
}
