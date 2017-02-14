use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::UnsafeCell;
use std::thread;

const MAX_BUF_SZ: usize = 1_000_000;
const N_BUFS: usize = 8;

struct IOBufs {
    log_offset: Arc<AtomicUsize>,
    bufs: [Arc<UnsafeCell<Vec<u8>>>; N_BUFS],
    headers: [Arc<AtomicUsize>; N_BUFS],
    current_buf: Arc<AtomicUsize>,
    written_bufs: Arc<AtomicUsize>,
}

impl Clone for IOBufs {
    fn clone(&self) -> IOBufs {
        IOBufs {
            log_offset: self.log_offset.clone(),
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
            current_buf: self.current_buf.clone(),
            written_bufs: self.written_bufs.clone(),
        }
    }
}

unsafe impl Send for IOBufs {}

unsafe impl Sync for IOBufs {}

struct Reservation {
    offset: u32,
    len: u32, // this may be different from header, due to concurrent access
    buf: Arc<UnsafeCell<Vec<u8>>>,
    last_hv: u32,
    header: Arc<AtomicUsize>,
    current_buf: Arc<AtomicUsize>,
    written_bufs: Arc<AtomicUsize>,
}

impl Default for IOBufs {
    fn default() -> IOBufs {
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
        IOBufs {
            log_offset: Arc::new(AtomicUsize::new(0)),
            bufs: bufs,
            headers: headers,
            current_buf: Arc::new(AtomicUsize::new(0)),
            written_bufs: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl IOBufs {
    fn reserve(&self, len: u32) -> Reservation {
        loop {
            // load atomic progress counters
            let current_buf = self.current_buf.load(Ordering::SeqCst);
            let written = self.written_bufs.load(Ordering::SeqCst);
            let idx = current_buf % N_BUFS;

            // if written is too far behind, we need to
            // spin while it catches up
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
            assert!(len >> 24 == 0);
            if offset + len > MAX_BUF_SZ as u32 {
                // attempt seal once, then start over
                let sealed = ops::mk_sealed(hv);
                if header.compare_and_swap(hv as usize, sealed as usize, Ordering::SeqCst) ==
                   hv as usize {
                    // we succeeded in setting seal, so we get to bump cur
                    self.current_buf.fetch_add(1, Ordering::SeqCst);
                    let n_writers = ops::n_writers(hv);
                    if n_writers == 0 {
                        // nobody else is going to flush this, so we need to
                        let res = self.reservation(offset, len, idx, sealed, header);
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

            let n_writers = ops::n_writers(hv);
            assert!(n_writers != 0);

            return self.reservation(offset, len, idx, hv, header);
        }
    }

    fn reservation(&self,
                   offset: u32,
                   len: u32,
                   idx: usize,
                   last_hv: u32,
                   header: Arc<AtomicUsize>)
                   -> Reservation {
        return Reservation {
            offset: offset,
            len: len,
            buf: self.bufs[idx].clone(),
            last_hv: last_hv,
            header: header,
            current_buf: self.current_buf.clone(),
            written_bufs: self.written_bufs.clone(),
        };
    }

    pub fn write(&self, buf: Vec<u8>) {
        let res = self.reserve(buf.len() as u32);
        res.write(buf);
    }
}

impl Reservation {
    fn abort_tx(&self) {
        // fills lease with a FailedFlush
    }

    fn commit_tx(&self) {}

    fn write(&self, buf: Vec<u8>) {
        let range = (self.offset as usize..(self.offset as usize + buf.len()));
        unsafe {
            let mut out_buf = (*self.buf.get()).as_mut_slice();
            (out_buf)[range].copy_from_slice(&*buf);
        }

        let mut hv = self.last_hv;
        let n_writers = ops::n_writers(hv);
        let sealed = ops::is_sealed(hv);
        if sealed && n_writers == 0 {
            return self.slam_down_pipe();
        }

        // decr writer count, retrying
        loop {
            let n_writers = ops::n_writers(hv);
            let hv2 = ops::decr_writers(hv);
            if self.header.compare_and_swap(hv as usize, hv2 as usize, Ordering::SeqCst) ==
               hv as usize {
                // we succeeded, free to exit
                // TODO cleanup
                let n_writers = ops::n_writers(hv2);
                let sealed = ops::is_sealed(hv2);
                if sealed && n_writers == 0 {
                    self.slam_down_pipe();
                }
                return;
            } else {
                // we failed, reload and retry
                hv = self.header.load(Ordering::SeqCst) as u32;
            }
        }
    }

    fn slam_down_pipe(&self) {
        // put the buf identified by idx on disk
        // TODO

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

#[test]
fn basic_functionality() {
    let iobs1 = Arc::new(IOBufs::default());
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
