#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use std::mem::transmute;

use libc::{c_int, c_uint, c_ushort, c_void, off_t};

pub const LIBURING_UDATA_TIMEOUT: u64 = 0xFFFFFFFF_FFFFFFFF; // -1

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

macro_rules! io_uring_barrier {
    () => {
        std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
    };
}

macro_rules! io_uring_smp_store_release {
    ($ptr:expr, $val:expr) => {
        io_uring_barrier!();
        std::ptr::write_volatile($ptr, $val);
    };
}

macro_rules! io_uring_smp_load_acquire {
    ($ptr:expr, $res:expr) => {
        $res = std::ptr::read_volatile($ptr);
        io_uring_barrier!();
    };
}

/*
 * Must be called after io_uring_for_each_cqe()
 */
pub unsafe fn io_uring_cq_advance(ring: *mut io_uring, nr: u32) {
    if nr > 0 {
        let cq: *mut io_uring_cq = &mut (*ring).cq;

        /*
         * Ensure that the kernel only sees the new value of the head
         * index after the CQEs have been read.
         */
        io_uring_smp_store_release!((*cq).khead, *(*cq).khead + nr);
    }
}

/*
 * Must be called after io_uring_{peek,wait}_cqe() after the cqe has
 * been processed by the application.
 */
pub unsafe fn io_uring_cqe_seen(ring: *mut io_uring, cqe: *mut io_uring_cqe) {
    if !cqe.is_null() {
        io_uring_cq_advance(ring, 1);
    }
}

/*
struct msghdr {
    void         *msg_name;       /* optional address */
    socklen_t     msg_namelen;    /* size of address */
    struct iovec *msg_iov;        /* scatter/gather array */
    size_t        msg_iovlen;     /* # elements in msg_iov */
    void         *msg_control;    /* ancillary data, see below */
    size_t        msg_controllen; /* ancillary data buffer len */
    int           msg_flags;      /* flags on received message */
};
 */

/*
 * Command prep helpers
 */
pub unsafe fn io_uring_sqe_set_data(sqe: *mut io_uring_sqe, data: *mut c_void) {
    (*sqe).user_data = data as u64;
}

pub unsafe fn io_uring_cqe_get_data(cqe: *mut io_uring_cqe) -> *mut c_void {
    return (*cqe).user_data as *mut c_void;
}

pub unsafe fn io_uring_sqe_set_flags(sqe: *mut io_uring_sqe, flags: u8) {
    (*sqe).flags = flags;
}

pub unsafe fn io_uring_prep_rw(
    op: c_uint,
    sqe: *mut io_uring_sqe,
    fd: c_int,
    addr: *const c_void,
    len: c_uint,
    offset: off_t,
) {
    (*sqe).opcode = op as u8;
    (*sqe).flags = 0;
    (*sqe).ioprio = 0;
    (*sqe).fd = fd;
    (*sqe).off = transmute(offset);
    (*sqe).addr = transmute(addr);
    (*sqe).len = len;
    (*sqe).__bindgen_anon_1.rw_flags = 0;
    (*sqe).user_data = 0;
    (*sqe).__bindgen_anon_2.__pad2[0] = 0;
    (*sqe).__bindgen_anon_2.__pad2[1] = 0;
    (*sqe).__bindgen_anon_2.__pad2[2] = 0;
}

pub unsafe fn io_uring_prep_readv(
    sqe: *mut io_uring_sqe,
    fd: c_int,
    iovecs: *const libc::iovec,
    nr_vecs: u32,
    offset: off_t,
) {
    io_uring_prep_rw(
        IORING_OP_READV,
        sqe,
        fd,
        transmute(iovecs),
        nr_vecs,
        offset,
    );
}

pub unsafe fn io_uring_prep_read_fixed(
    sqe: *mut io_uring_sqe,
    fd: i32,
    buf: *mut c_void,
    nbytes: u32,
    offset: off_t,
    buf_index: c_ushort,
) {
    io_uring_prep_rw(IORING_OP_READ_FIXED, sqe, fd, buf, nbytes, offset);
    (*sqe).__bindgen_anon_2.buf_index = buf_index;
}

pub unsafe fn io_uring_prep_writev(
    sqe: *mut io_uring_sqe,
    fd: i32,
    iovecs: *const libc::iovec,
    nr_vecs: u32,
    offset: off_t,
) {
    io_uring_prep_rw(
        IORING_OP_WRITEV,
        sqe,
        fd,
        transmute(iovecs),
        nr_vecs,
        offset,
    );
}

pub unsafe fn io_uring_prep_write_fixed(
    sqe: *mut io_uring_sqe,
    fd: i32,
    buf: *mut c_void,
    nbytes: u32,
    offset: off_t,
    buf_index: u16,
) {
    io_uring_prep_rw(IORING_OP_WRITE_FIXED, sqe, fd, buf, nbytes, offset);
    (*sqe).__bindgen_anon_2.buf_index = buf_index;
}

pub unsafe fn io_uring_prep_recvmsg(
    sqe: *mut io_uring_sqe,
    fd: i32,
    msg: *mut c_void,
    flags: u32,
) {
    io_uring_prep_rw(IORING_OP_RECVMSG, sqe, fd, msg, 1, 0);
    (*sqe).__bindgen_anon_1.msg_flags = flags;
}

pub unsafe fn io_uring_prep_sendmsg(
    sqe: *mut io_uring_sqe,
    fd: i32,
    msg: *const c_void,
    flags: u32,
) {
    io_uring_prep_rw(IORING_OP_SENDMSG, sqe, fd, msg, 1, 0);
    (*sqe).__bindgen_anon_1.msg_flags = flags;
}

pub unsafe fn io_uring_prep_poll_add(
    sqe: *mut io_uring_sqe,
    fd: i32,
    poll_mask: u16,
) {
    io_uring_prep_rw(IORING_OP_POLL_ADD, sqe, fd, std::ptr::null(), 0, 0);
    (*sqe).__bindgen_anon_1.poll_events = poll_mask;
}

pub unsafe fn io_uring_prep_poll_remove(
    sqe: *mut io_uring_sqe,
    user_data: *mut c_void,
) {
    io_uring_prep_rw(IORING_OP_POLL_REMOVE, sqe, 0, user_data, 0, 0);
}

pub unsafe fn io_uring_prep_fsync(
    sqe: *mut io_uring_sqe,
    fd: i32,
    fsync_flags: u32,
) {
    io_uring_prep_rw(IORING_OP_FSYNC, sqe, fd, std::ptr::null(), 0, 0);
    (*sqe).__bindgen_anon_1.fsync_flags = fsync_flags;
}

pub unsafe fn io_uring_prep_nop(sqe: *mut io_uring_sqe) {
    io_uring_prep_rw(IORING_OP_NOP, sqe, 0, std::ptr::null(), 0, 0);
}

pub unsafe fn io_uring_prep_timeout(
    sqe: *mut io_uring_sqe,
    ts: *mut __kernel_timespec,
    count: off_t,
) {
    io_uring_prep_rw(IORING_OP_TIMEOUT, sqe, 0, transmute(ts), 1, count);
}

pub unsafe fn io_uring_sq_space_left(ring: *const io_uring) -> u32 {
    return (*ring).sq.kring_entries as u32
        - ((*ring).sq.sqe_tail - (*ring).sq.sqe_head);
}

pub unsafe fn io_uring_cq_ready(ring: *mut io_uring) -> u32 {
    let tail: c_uint;
    io_uring_smp_load_acquire!((*ring).cq.ktail, tail);
    return tail - *(*ring).cq.khead;
}

unsafe fn __io_uring_peek_cqe(
    ring: *mut io_uring,
    cqe_ptr: *mut *mut io_uring_cqe,
) -> c_int {
    let mut cqe: *mut io_uring_cqe;
    let mut err: c_int = 0;

    loop {
        /*
         * io_uring_smp_load_acquire() enforces the order of tail
         * and CQE reads.
         */
        let head = *(*ring).cq.khead;
        let tail: c_uint;
        io_uring_smp_load_acquire!((*ring).cq.ktail, tail);
        cqe = if head != tail {
            (*ring).cq.cqes.offset((head & *(*ring).cq.kring_mask) as isize)
        } else {
            std::ptr::null_mut()
        };

        if !cqe.is_null() {
            if (*cqe).user_data == LIBURING_UDATA_TIMEOUT {
                if (*cqe).res < 0 {
                    err = (*cqe).res;
                }

                io_uring_cq_advance(ring, 1);

                if err == 0 {
                    continue;
                }

                cqe = std::ptr::null_mut();
            }
        }

        break;
    }

    *cqe_ptr = cqe;

    return err;
}

/*
 * Return an IO completion, if one is readily available. Returns 0 with
 * cqe_ptr filled in on success, -errno on failure.
 */
pub unsafe fn io_uring_peek_cqe(
    ring: *mut io_uring,
    cqe_ptr: *mut *mut io_uring_cqe,
) -> c_int {
    let err: i32 = __io_uring_peek_cqe(ring, cqe_ptr);
    if err != 0 {
        return err;
    }

    return __io_uring_get_cqe(ring, cqe_ptr, 0, 0, std::ptr::null_mut());
}

/*
 * Return an IO completion, waiting for it if necessary. Returns 0 with
 * cqe_ptr filled in on success, -errno on failure.
 */
pub unsafe fn io_uring_wait_cqe(
    ring: *mut io_uring,
    cqe_ptr: *mut *mut io_uring_cqe,
) -> c_int {
    let err = __io_uring_peek_cqe(ring, cqe_ptr);
    if err != 0 {
        return err;
    }

    return __io_uring_get_cqe(ring, cqe_ptr, 0, 1, std::ptr::null_mut());
}

#[cfg(test)]
mod tests {
    use std::io::Error;
    use std::mem;

    use crate::*;

    const QUEUE_DEPTH: u32 = 4;

    #[test]
    fn test_io_uring_queue_init() {
        let mut ring = unsafe {
            let mut s = mem::MaybeUninit::<io_uring>::uninit();
            let ret = io_uring_queue_init(QUEUE_DEPTH, s.as_mut_ptr(), 0);
            if ret < 0 {
                panic!(
                    "io_uring_queue_init: {:?}",
                    Error::from_raw_os_error(ret)
                );
            }
            s.assume_init()
        };

        loop {
            let sqe = unsafe { io_uring_get_sqe(&mut ring) };
            if sqe == std::ptr::null_mut() {
                break;
            }
            unsafe { io_uring_prep_nop(sqe) };
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
                panic!(
                    "io_uring_wait_cqe: {:?}",
                    Error::from_raw_os_error(ret)
                );
            }
            done += 1;
            if unsafe { (*cqe).res } < 0 {
                eprintln!("(*cqe).res = {}", unsafe { (*cqe).res });
            }
            unsafe { io_uring_cqe_seen(&mut ring, cqe) };
        }
        println!("Submitted={}, completed={}", pending, done);
        unsafe { io_uring_queue_exit(&mut ring) };
    }
}
