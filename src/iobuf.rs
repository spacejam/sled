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
            if hv >> 31 == 1 {
                // already sealed, start over and hope cur
                // has already been bumped by sealer.
                continue;
            }

            // try to claim space, seal otherwise
            let offset = hv << 8 >> 8;
            assert!(len >> 24 == 0);
            if offset + len > MAX_BUF_SZ as u32 {
                // attempt seal once, then start over
                let sealed = hv | (1 << 31);
                if header.compare_and_swap(hv as usize, sealed as usize, Ordering::SeqCst) ==
                   hv as usize {
                    // TODO cleanup
                    // we succeeded in setting seal, so we get to bump cur
                    self.current_buf.fetch_add(1, Ordering::SeqCst);
                    let n_writers = hv << 1 >> 25;
                    if n_writers == 0 {
                        // nobody else is going to flush this, so we need to
                        let res = Reservation {
                            offset: offset,
                            buf: self.bufs[idx].clone(),
                            last_hv: sealed,
                            header: header,
                            current_buf: self.current_buf.clone(),
                            written_bufs: self.written_bufs.clone(),
                        };
                        res.write(vec![]);
                    }
                } else {
                }
                continue;
            } else {
                // attempt to claim
                let writer_inc = 1 << 24;
                let claimed = hv + len + writer_inc;
                if header.compare_and_swap(hv as usize, claimed as usize, Ordering::SeqCst) !=
                   hv as usize {
                    // CAS failed, start over
                    continue;
                }
                hv = claimed;
            }

            let n_writers = hv << 1 >> 25;
            assert!(n_writers != 0);

            return Reservation {
                offset: offset,
                buf: self.bufs[idx].clone(),
                last_hv: hv,
                header: header,
                current_buf: self.current_buf.clone(),
                written_bufs: self.written_bufs.clone(),
            };
        }
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

        // decr writer count, retrying
        let mut hv = self.last_hv;
        loop {
            let n_writers = hv << 1 >> 25;
            let sealed = hv >> 31 == 1;
            if sealed && n_writers == 0 {
                // if sealed is set, and you are last decr,
                // don't decr, that's writer's resp
                self.slam_down_pipe();
                return;
            }

            let hv2 = hv - (1 << 24);
            if self.header.compare_and_swap(hv as usize, hv2 as usize, Ordering::SeqCst) ==
               hv as usize {
                // we succeeded, free to exit
                // TODO cleanup
                let n_writers = hv2 << 1 >> 25;
                let sealed = hv2 >> 31 == 1;
                if sealed && n_writers == 0 {
                    // if sealed is set, and you are last decr,
                    // don't decr, that's writer's resp
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

#[test]
fn basic_functionality() {
    let iobs1 = Arc::new(IOBufs::default());
    let iobs2 = iobs1.clone();
    let iobs3 = iobs1.clone();
    let t1 = thread::spawn(move || {
        for i in 0..50_000 {
            let buf = vec![1; 512];
            iobs1.write(buf);
        }
    });
    let t2 = thread::spawn(move || {
        for i in 0..50_000 {
            let buf = vec![2; 512];
            iobs2.write(buf);
        }
    });
    let t3 = thread::spawn(move || {
        for i in 0..50_000 {
            let buf = vec![3; 512];
            iobs3.write(buf);
        }
    });
    t1.join();
    t2.join();
    t3.join();
}
