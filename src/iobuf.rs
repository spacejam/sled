use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::UnsafeCell;
use std::thread;
use std::sync::mpsc::{channel, Receiver, Sender};

use super::log;

const MAX_BUF_SZ: usize = 1_000_000;
const N_BUFS: usize = 8;

struct IOBufs {
    bufs: [Arc<UnsafeCell<Vec<u8>>>; N_BUFS],
    headers: [Arc<AtomicUsize>; N_BUFS],
    log_offsets: [Arc<AtomicUsize>; N_BUFS],
    current_buf: Arc<AtomicUsize>,
    written_bufs: Arc<AtomicUsize>,
    plunger: Sender<Reservation>,
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
    base_disk_offset: usize,
    res_len: u32, // this may be different from header, due to concurrent access
    buf_offset: u32,
    buf: Arc<UnsafeCell<Vec<u8>>>,
    last_hv: u32, // optimization to avoid more atomic loads
    header: Arc<AtomicUsize>,
    current_buf: Arc<AtomicUsize>,
    written_bufs: Arc<AtomicUsize>,
    plunger: Sender<Reservation>,
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
            current_buf: Arc::new(AtomicUsize::new(0)),
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
                continue;
            }

            // load current header value
            let header = self.headers[idx].clone();
            let mut hv = header.load(Ordering::SeqCst) as u32;

            // skip if already sealed
            if ops::is_sealed(hv) {
                // already sealed, start over and hope cur
                // has already been bumped by sealer.
                continue;
            }

            // try to claim space, seal otherwise
            let offset = ops::offset(hv);
            if offset + len > MAX_BUF_SZ as u32 {
                // attempt seal once, then start over
                let sealed = ops::mk_sealed(hv);
                if header.compare_and_swap(hv as usize, sealed as usize, Ordering::SeqCst) ==
                   hv as usize {
                    // we succeeded in setting seal,
                    // so we get to bump cur and log_offset
                    // NB this is effectively a global lock until self.current_buf gets bumped
                    // NB set next offset before bumping self.current_buf
                    let new_log_offset = self.log_offsets[idx].load(Ordering::SeqCst) +
                                         offset as usize;
                    self.log_offsets[(idx + 1) % N_BUFS].store(new_log_offset, Ordering::SeqCst);
                    self.current_buf.fetch_add(1, Ordering::SeqCst);

                    // if writers is now 0, it's time to flush it
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
            base_disk_offset: 4 + self.log_offsets[idx].load(Ordering::SeqCst),
            res_len: len,
            buf_offset: offset,
            buf: self.bufs[idx].clone(),
            last_hv: last_hv,
            header: self.headers[idx].clone(),
            current_buf: self.current_buf.clone(),
            written_bufs: self.written_bufs.clone(),
            plunger: self.plunger.clone(),
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
        if ops::n_writers(hv) <= 1 && ops::is_sealed(hv) {
            // this was triggered by a not-yet-satisfied reservation
            // question: can decr and zero step on toes?
            //  CAS to 0 writers below may trigger 0 check in reserve()?

            // we need to slam down pipe here, rather than CAS to 0,
            // because in reserve() we check for sealed bufs with 0
            // writers and trigger an alternative flush path for
            // buffers sealed after the last writer successfully
            // finished using it
            return self.slam_down_pipe();
        }

        // decr writer count, retrying
        loop {
            let hv2 = ops::decr_writers(hv);
            if self.header.compare_and_swap(hv as usize, hv2 as usize, Ordering::SeqCst) ==
               hv as usize {
                if ops::n_writers(hv2) == 0 && ops::is_sealed(hv2) {
                    self.slam_down_pipe();
                }
                return;
            } else {
                // we failed, reload and retry
                hv = self.header.load(Ordering::SeqCst) as u32;
            }
        }
    }

    fn slam_down_pipe(self) {
        let plunger = self.plunger.clone();
        plunger.send(self);
    }

    pub fn stabilize(&self, log: &mut log::Log) {
        // put the buf identified by idx on disk
        let data = unsafe { (*self.buf.get()).as_mut_slice() };
        let lid = log.append(&data[0..self.res_len as usize]).unwrap();
        // TODO fix mapping with offset prediction
        // assert_eq!(lid, self.base_disk_offset as u64);

        // reset the header, no need to zero the buf
        self.header.store(0, Ordering::SeqCst);

        // bump self.written by 1
        self.written_bufs.fetch_add(1, Ordering::SeqCst);
    }
}

mod ops {
    pub fn is_sealed(v: u32) -> bool {
        v >> 31 == 1
    }

    pub fn mk_sealed(v: u32) -> u32 {
        v | 1 << 31
    }

    pub fn n_writers(v: u32) -> u32 {
        v << 1 >> 25
    }

    pub fn incr_writers(v: u32) -> u32 {
        assert!(n_writers(v) != 127);
        v + (1 << 24)
    }

    pub fn decr_writers(v: u32) -> u32 {
        assert!(n_writers(v) != 0);
        v - (1 << 24)
    }

    pub fn offset(v: u32) -> u32 {
        v << 8 >> 8
    }

    pub fn bump_offset(v: u32, by: u32) -> u32 {
        assert!(by >> 24 == 0);
        v + by
    }
}

struct Stabilizer {
    receiver: Receiver<Reservation>,
    log: log::Log,
}

impl Stabilizer {
    fn new(receiver: Receiver<Reservation>, log: log::Log) -> Stabilizer {
        Stabilizer {
            receiver: receiver,
            log: log,
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
        Stabilizer::new(rx, log::Log::open()).run();
    });
    let iobs1 = Arc::new(IOBufs::new(tx));
    let iobs2 = iobs1.clone();
    let iobs3 = iobs1.clone();
    let iobs4 = iobs1.clone();
    let t1 = thread::spawn(move || {
        for i in 0..50_000 {
            let buf = vec![1; i % 8192];
            iobs1.write(buf);
        }
    });
    let t2 = thread::spawn(move || {
        for i in 0..50_000 {
            let buf = vec![2; i % 8192];
            iobs2.write(buf);
        }
    });
    let t3 = thread::spawn(move || {
        for i in 0..50_000 {
            let buf = vec![3; i % 8192];
            iobs3.write(buf);
        }
    });
    let t4 = thread::spawn(move || {
        for i in 0..50_000 {
            let buf = vec![4; i % 8192];
            iobs4.write(buf);
        }
    });
    t1.join();
    t2.join();
    t3.join();
    t4.join();
}
