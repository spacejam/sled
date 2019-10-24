use std::fs::File;
use std::io::Error;
use std::mem;
use std::os::unix::io::AsRawFd;

use libc::off_t;
use liburing::*;

const QUEUE_DEPTH: u32 = 4;
const READ_SIZE: usize = 128;

#[test]
fn test_io_uring_read_file() {
    let mut ring = unsafe {
        let mut s = mem::MaybeUninit::<io_uring>::uninit();
        let ret = io_uring_queue_init(QUEUE_DEPTH, s.as_mut_ptr(), 0);
        if ret < 0 {
            panic!("io_uring_queue_init: {:?}", Error::from_raw_os_error(ret));
        }
        s.assume_init()
    };

    let file = File::open("/proc/self/exe").unwrap();

    let mut iovecs: Vec<libc::iovec> =
        vec![unsafe { mem::zeroed() }; QUEUE_DEPTH as usize];
    for iov in iovecs.iter_mut() {
        let buf = unsafe {
            let mut s = mem::MaybeUninit::<*mut libc::c_void>::uninit();
            if libc::posix_memalign(s.as_mut_ptr(), 4096, READ_SIZE) != 0 {
                panic!("can't allocate");
            }
            s.assume_init()
        };
        iov.iov_base = buf;
        iov.iov_len = READ_SIZE;
    }

    let mut offset: usize = 0;
    for i in 0.. {
        let sqe = unsafe { io_uring_get_sqe(&mut ring) };
        if sqe == std::ptr::null_mut() {
            break;
        }
        unsafe {
            io_uring_prep_readv(
                sqe,
                file.as_raw_fd(),
                &mut iovecs[i],
                1,
                offset as off_t,
            )
        };
        offset += READ_SIZE;
    }
    let ret = unsafe { io_uring_submit(&mut ring) };
    if ret < 0 {
        panic!("io_uring_submit: {:?}", Error::from_raw_os_error(ret));
    }

    let mut cqe: *mut io_uring_cqe = unsafe { std::mem::zeroed() };
    let mut done = 0;
    let pending = ret;
    for _ in 0..pending {
        let ret = unsafe { io_uring_wait_cqe(&mut ring, &mut cqe) };
        if ret < 0 {
            panic!("io_uring_wait_cqe: {:?}", Error::from_raw_os_error(ret));
        }
        done += 1;
        if unsafe { (*cqe).res } != READ_SIZE as i32 {
            eprintln!("(*cqe).res = {}", unsafe { (*cqe).res });
        }
        unsafe { io_uring_cqe_seen(&mut ring, cqe) };
    }

    // println!("Submitted={}, completed={}", pending, done);
    unsafe { io_uring_queue_exit(&mut ring) };
}
