use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::UnsafeCell;
use std::thread;

const MAX_BUF_SZ: usize = 1_000_000;
const N_BUFS: usize = 8;

struct IOBufs {
    log_offset: Arc<AtomicUsize>,
    bufs: [Arc<UnsafeCell<[u8; MAX_BUF_SZ]>>; N_BUFS],
    headers: [Arc<AtomicUsize>; N_BUFS],
    current_buf: Arc<AtomicUsize>,
    written_bufs: Arc<AtomicUsize>,
}

struct Reservation {
    offset: u32,
    buf: Arc<UnsafeCell<[u8; MAX_BUF_SZ]>>,
    last_hv: u32,
    header: Arc<AtomicUsize>,
    current_buf: Arc<AtomicUsize>,
    written_bufs: Arc<AtomicUsize>,
}

unsafe impl Sync for IOBufs {}

impl Default for IOBufs {
    fn default() -> IOBufs {
        let mut bufs = [Arc::new(UnsafeCell::new([0; MAX_BUF_SZ])),
                        Arc::new(UnsafeCell::new([0; MAX_BUF_SZ])),
                        Arc::new(UnsafeCell::new([0; MAX_BUF_SZ])),
                        Arc::new(UnsafeCell::new([0; MAX_BUF_SZ])),
                        Arc::new(UnsafeCell::new([0; MAX_BUF_SZ])),
                        Arc::new(UnsafeCell::new([0; MAX_BUF_SZ])),
                        Arc::new(UnsafeCell::new([0; MAX_BUF_SZ])),
                        Arc::new(UnsafeCell::new([0; MAX_BUF_SZ]))];
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
            println!("reserve loop");
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
            let hv = header.load(Ordering::SeqCst) as u32;

            // skip if already sealed
            if hv >> 31 == 1 {
                // already sealed, start over and hope cur
                // has already been bumped by sealer.
                continue;
            }

            // try to claim space, seal otherwise
            let offset = hv << 8 >> 8;
            assert!(len >> 24 == 0);
            assert!(offset + len <= MAX_BUF_SZ as u32);
            if offset + len > MAX_BUF_SZ as u32 {
                // attempt seal once, then start over
                let new = hv | (1 << 31);
                if header.compare_and_swap(hv as usize, new as usize, Ordering::SeqCst) ==
                   hv as usize {
                    // we succeeded in setting seal, so we get to bump cur
                    self.current_buf.fetch_add(1, Ordering::SeqCst);
                }
                continue;
            } else {
                // attempt to claim
                let new = hv + len;
                if header.compare_and_swap(hv as usize, new as usize, Ordering::SeqCst) !=
                   hv as usize {
                    // CAS failed, start over
                    continue;
                }
            }

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
        println!("IOBufs write");
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
        unsafe {
            let mut out_buf = self.buf.get();
            (*out_buf)[self.offset as usize..].copy_from_slice(&*buf);
        }

        // decr writer count, retrying
        let mut hv = self.last_hv;
        loop {
            let n_writers = hv << 1 >> 25;
            assert!(n_writers != 0);
            let sealed = hv >> 31 == 1;

            if sealed && n_writers == 1 {
                // if sealed is set, and you are last decr,
                // don't decr, that's writer's resp
                self.slam_down_pipe();
                return;
            }

            let hv2 = hv - (1 << 24);
            if self.header.compare_and_swap(hv as usize, hv2 as usize, Ordering::SeqCst) ==
               hv as usize {
                // we succeeded, free to exit
                return;
            } else {
                // we failed, reload and retry
                hv = self.header.load(Ordering::SeqCst) as u32;
            }
        }
    }

    fn slam_down_pipe(&self) {
        let mut out_buf = unsafe { self.buf.get() };

        // put the buf identified by idx on disk
        // TODO

        // zero the buf
        unsafe {
            (*out_buf)[..].copy_from_slice(&[0u8; MAX_BUF_SZ]);
        }

        // bump self.written by 1
        self.written_bufs.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn basic_functionality() {
    let iobs = IOBufs::default();
    for i in 1..5_000 {
        let buf = vec![1; 1024];
        iobs.write(buf);
    }
    println!("yo");
}
