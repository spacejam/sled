use std::fmt::{self, Debug};
use std::fs;
use std::io::{self, Read, Write, Seek, SeekFrom, Error, ErrorKind};
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::{UnsafeCell, RefCell};
use std::thread;
use std::sync::mpsc::{channel, Receiver, Sender};

use super::*;

const MAX_BUF_SZ: usize = 1_000_000;
const N_BUFS: usize = 8;

thread_local! {
    static LOCAL_READER: RefCell<fs::File> = RefCell::new(open_log_for_reading());
}

fn open_log_for_reading() -> fs::File {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    options.open("rsdb.log").unwrap()
}

fn read(id: LogID) -> io::Result<LogData> {
    LOCAL_READER.with(|f| {
        let mut f = f.borrow_mut();
        f.seek(SeekFrom::Start(id))?;
        let mut len_buf = [0u8; 4];
        f.read_exact(&mut len_buf)?;
        let len = ops::array_to_usize(len_buf);
        let mut buf = Vec::with_capacity(len);
        unsafe {
            buf.set_len(len);
        }
        f.read_exact(&mut buf)?;
        ops::from_binary::<LogData>(buf)
            .map_err(|_| Error::new(ErrorKind::Other, "failed to deserialize LogData"))
    })
}

pub struct IOBufs {
    bufs: [Arc<UnsafeCell<Vec<u8>>>; N_BUFS],
    headers: [Arc<AtomicUsize>; N_BUFS],
    log_offsets: [Arc<AtomicUsize>; N_BUFS],
    current_buf: Arc<AtomicUsize>,
    written_bufs: Arc<AtomicUsize>,
    plunger: Sender<Reservation>,
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

impl Clone for IOBufs {
    fn clone(&self) -> IOBufs {
        IOBufs {
            bufs: [self.bufs[0].clone(),
                   self.bufs[1].clone(),
                   self.bufs[2].clone(),
                   self.bufs[3].clone(),
                   self.bufs[4].clone(),
                   self.bufs[5].clone(),
                   self.bufs[6].clone(),
                   self.bufs[7].clone()],
            headers: [self.headers[0].clone(),
                      self.headers[1].clone(),
                      self.headers[2].clone(),
                      self.headers[3].clone(),
                      self.headers[4].clone(),
                      self.headers[5].clone(),
                      self.headers[6].clone(),
                      self.headers[7].clone()],
            log_offsets: [self.log_offsets[0].clone(),
                          self.log_offsets[1].clone(),
                          self.log_offsets[2].clone(),
                          self.log_offsets[3].clone(),
                          self.log_offsets[4].clone(),
                          self.log_offsets[5].clone(),
                          self.log_offsets[6].clone(),
                          self.log_offsets[7].clone()],
            current_buf: self.current_buf.clone(),
            written_bufs: self.written_bufs.clone(),
            plunger: self.plunger.clone(),
        }
    }
}

unsafe impl Send for IOBufs {}

unsafe impl Sync for IOBufs {}

#[derive(Clone)]
struct Reservation {
    base_disk_offset: LogID,
    res_len: u32, // this may be different from header, due to concurrent access
    buf_offset: u32,
    buf: Arc<UnsafeCell<Vec<u8>>>,
    last_hv: u32, // optimization to avoid more atomic loads
    header: Arc<AtomicUsize>,
    current_buf: Arc<AtomicUsize>,
    written_bufs: Arc<AtomicUsize>,
    plunger: Sender<Reservation>,
    idx: usize,
}

unsafe impl Send for Reservation {}

impl IOBufs {
    fn new(plunger: Sender<Reservation>) -> IOBufs {
        let mut bufs = [Arc::new(UnsafeCell::new(vec![0; MAX_BUF_SZ])),
                        Arc::new(UnsafeCell::new(vec![0; MAX_BUF_SZ])),
                        Arc::new(UnsafeCell::new(vec![0; MAX_BUF_SZ])),
                        Arc::new(UnsafeCell::new(vec![0; MAX_BUF_SZ])),
                        Arc::new(UnsafeCell::new(vec![0; MAX_BUF_SZ])),
                        Arc::new(UnsafeCell::new(vec![0; MAX_BUF_SZ])),
                        Arc::new(UnsafeCell::new(vec![0; MAX_BUF_SZ])),
                        Arc::new(UnsafeCell::new(vec![0; MAX_BUF_SZ]))];
        let headers = [Arc::new(AtomicUsize::new(0)),
                       Arc::new(AtomicUsize::new(0)),
                       Arc::new(AtomicUsize::new(0)),
                       Arc::new(AtomicUsize::new(0)),
                       Arc::new(AtomicUsize::new(0)),
                       Arc::new(AtomicUsize::new(0)),
                       Arc::new(AtomicUsize::new(0)),
                       Arc::new(AtomicUsize::new(0))];
        let log_offsets = [Arc::new(AtomicUsize::new(0)),
                           Arc::new(AtomicUsize::new(0)),
                           Arc::new(AtomicUsize::new(0)),
                           Arc::new(AtomicUsize::new(0)),
                           Arc::new(AtomicUsize::new(0)),
                           Arc::new(AtomicUsize::new(0)),
                           Arc::new(AtomicUsize::new(0)),
                           Arc::new(AtomicUsize::new(0))];
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
        loop {
            // load atomic progress counters
            let current_buf = self.current_buf.load(Ordering::SeqCst);
            let written = self.written_bufs.load(Ordering::SeqCst);
            let idx = current_buf % N_BUFS;

            // if written is too far behind, we need to
            // spin while it catches up to avoid overlap
            if current_buf - written >= N_BUFS {
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
                    self.log_offsets[(idx + 1) % N_BUFS].store(new_log_offset, Ordering::SeqCst);
                    self.current_buf.fetch_add(1, Ordering::SeqCst);

                    // if writers is now 1 (from our previous incr),
                    // it's our responsibility to flush it
                    if ops::n_writers(sealed) == 0 {
                        // nobody else is going to flush this, so we need to
                        let res = self.reservation(offset, 0, idx, sealed);
                        res.write(vec![]);
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
            base_disk_offset: 4 + self.log_offsets[idx].load(Ordering::SeqCst) as LogID,
            res_len: len,
            buf_offset: offset,
            buf: self.bufs[idx].clone(),
            last_hv: last_hv,
            header: self.headers[idx].clone(),
            current_buf: self.current_buf.clone(),
            written_bufs: self.written_bufs.clone(),
            plunger: self.plunger.clone(),
            idx: idx,
        };
    }

    pub fn write(&self, buf: Vec<u8>) {
        let res = self.reserve(buf.len() as u32);
        res.write(buf);
    }

    pub fn shutdown(&self) {
        unimplemented!()
    }
}

impl Reservation {
    fn abort_tx(&self) {
        // fills lease with a FailedFlush
    }

    fn commit_tx(&self) {}

    fn write(self, buf: Vec<u8>) {
        assert_eq!(buf.len(), self.res_len as usize);

        let range = self.buf_offset as usize..(self.buf_offset as usize + self.res_len as usize);
        unsafe {
            let mut out_buf = (*self.buf.get()).as_mut_slice();
            (out_buf)[range].copy_from_slice(&*buf);
        }

        let mut hv = self.last_hv;

        if ops::n_writers(hv) == 0 && ops::is_sealed(hv) {
            self.slam_down_pipe();
            return;
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
                return;
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
        plunger.send(self);
    }

    pub fn stabilize(&self, log: &mut fs::File) {
        // put the buf identified by idx on disk
        // let data = unsafe { (*self.buf.get()).as_mut_slice() };
        // let data_bytes = &data[0..self.res_len as usize];
        // let len_bytes = ops::usize_to_array(data_bytes.len());
        // log.seek(SeekFrom::Start(self.base_disk_offset));
        // log.write_all(&len_bytes).unwrap();
        // log.write_all(&data_bytes).unwrap();

        // TODO fix mapping with offset prediction
        // assert_eq!(lid, self.base_disk_offset as u64);

        // reset the header, no need to zero the buf
        self.header.store(0, Ordering::SeqCst);

        // bump self.written by 1
        self.written_bufs.fetch_add(1, Ordering::SeqCst);
        // println!("+ buf {} stabilized, cur is {}", self.idx, self.current_buf.load(Ordering::SeqCst) % N_BUFS);
    }
}

struct LogWriter {
    receiver: Receiver<Reservation>,
    log: fs::File,
    open_offset: LogID,
}

impl LogWriter {
    fn new(receiver: Receiver<Reservation>) -> LogWriter {
        let cur_id = fs::metadata("rsdb.log").map(|m| m.len()).unwrap_or(0);

        let mut options = fs::OpenOptions::new();
        options.write(true).create(true);
        let file = options.open("rsdb.log").unwrap();

        LogWriter {
            receiver: receiver,
            log: file,
            open_offset: cur_id,
        }
    }

    fn run(mut self) {
        for res in self.receiver.iter() {
            res.stabilize(&mut self.log);
        }
    }
}

#[test]
fn basic_functionality() {
    // TODO linearize res bufs, verify they are correct
    let (tx, rx) = channel();
    let stabilizer = thread::spawn(move || {
        LogWriter::new(rx).run();
    });
    let iobs1 = IOBufs::new(tx);
    let iobs2 = iobs1.clone();
    let iobs3 = iobs1.clone();
    let iobs4 = iobs1.clone();
    let iobs5 = iobs1.clone();
    let iobs6 = iobs1.clone();
    let t1 = thread::spawn(move || {
        for i in 0..5_000 {
            let buf = vec![1; i % 8192];
            iobs1.write(buf);
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
    t1.join();
    t2.join();
    t3.join();
    t4.join();
    t5.join();
    t6.join();
}

#[test]
fn log_rt() {
    let deltablock = LogData::Deltas(vec![]);
    let data_bytes = ops::to_binary(&deltablock);
    let t1 = thread::spawn(move || {
    });
    t1.join();

}
