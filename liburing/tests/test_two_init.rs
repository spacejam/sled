use std::io::Error;
use std::mem;

use liburing::*;

const QUEUE_DEPTH: u32 = 4;

#[test]
fn test_io_uring_two_init() {
    fn init() -> io_uring {
        unsafe {
            let mut s = mem::MaybeUninit::<io_uring>::uninit();
            let ret = io_uring_queue_init(QUEUE_DEPTH, s.as_mut_ptr(), 0);
            if ret < 0 {
                panic!(
                    "io_uring_queue_init: {:?}",
                    Error::from_raw_os_error(ret)
                );
            }
            s.assume_init()
        }
    }

    let ring1: io_uring = init();
    let ring2: io_uring = init();

    assert_ne!(ring1.sq.khead, ring2.sq.khead);
    assert_ne!(ring1.sq.ktail, ring2.sq.ktail);
    assert_ne!(ring1.cq.khead, ring2.cq.khead);
    assert_ne!(ring1.cq.ktail, ring2.cq.ktail);

    // println!(
    //     "ring1.sq.khead = {:p}, ring1.sq.ktail = {:p}",
    //     ring1.sq.khead, ring1.sq.ktail
    // );
    // println!(
    //     "ring2.sq.khead = {:p}, ring2.sq.ktail = {:p}",
    //     ring2.sq.khead, ring2.sq.ktail
    // );

    unsafe {
        for r in [ring1, ring2].iter_mut() {
            io_uring_queue_exit(r);
        }
    };
}
