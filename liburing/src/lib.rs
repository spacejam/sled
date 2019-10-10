#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use libc::{c_int, c_uint, c_ushort, c_void, off_t};
use std::mem::transmute;

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

/*
 * Must be called after io_uring_for_each_cqe()
 */
// pub unsafe fn io_uring_cq_advance(struct io_uring *ring, unsigned nr)

/*
 * Must be called after io_uring_{peek,wait}_cqe() after the cqe has
 * been processed by the application.
 */
// pub unsafe fn io_uring_cqe_seen(struct io_uring *ring,
// struct io_uring_cqe *cqe)
// {
//     if (cqe)
//         io_uring_cq_advance(ring, 1);
// }

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
    op: u32,
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

/*
pub unsafe fn io_uring_prep_writev(sqe: *mut io_uring_sqe, fd: i32,
iovecs: *const iovec,
nr_vecs: u32, offset: u64)
{
io_uring_prep_rw(IORING_OP_WRITEV, sqe, fd, iovecs, nr_vecs, offset);
}

pub unsafe fn io_uring_prep_write_fixed(sqe: *mut io_uring_sqe, fd: i32,
const void *buf, nbytes: u32,
offset: u64, int buf_index)
{
io_uring_prep_rw(IORING_OP_WRITE_FIXED, sqe, fd, buf, nbytes, offset);
(*sqe).buf_index = buf_index;
}

pub unsafe fn io_uring_prep_recvmsg(sqe: *mut io_uring_sqe, fd: i32,
struct msghdr *msg, unsigned flags)
{
io_uring_prep_rw(IORING_OP_RECVMSG, sqe, fd, msg, 1, 0);
(*sqe).msg_flags = flags;
}

pub unsafe fn io_uring_prep_sendmsg(sqe: *mut io_uring_sqe, fd: i32,
const struct msghdr *msg, unsigned flags)
{
io_uring_prep_rw(IORING_OP_SENDMSG, sqe, fd, msg, 1, 0);
(*sqe).msg_flags = flags;
}

pub unsafe fn io_uring_prep_poll_add(sqe: *mut io_uring_sqe, fd: i32,
short poll_mask)
{
io_uring_prep_rw(IORING_OP_POLL_ADD, sqe, fd, NULL, 0, 0);
(*sqe).poll_events = poll_mask;
}

pub unsafe fn io_uring_prep_poll_remove(sqe: *mut io_uring_sqe,
void *user_data)
{
io_uring_prep_rw(IORING_OP_POLL_REMOVE, sqe, 0, user_data, 0, 0);
}

pub unsafe fn io_uring_prep_fsync(sqe: *mut io_uring_sqe, fd: i32,
unsigned fsync_flags)
{
io_uring_prep_rw(IORING_OP_FSYNC, sqe, fd, NULL, 0, 0);
(*sqe).fsync_flags = fsync_flags;
}

pub unsafe fn io_uring_prep_nop(sqe: *mut io_uring_sqe)
{
io_uring_prep_rw(IORING_OP_NOP, sqe, 0, NULL, 0, 0);
}

pub unsafe fn io_uring_prep_timeout(sqe: *mut io_uring_sqe,
struct __kernel_timespec *ts,
unsigned count)
{
io_uring_prep_rw(IORING_OP_TIMEOUT, sqe, 0, ts, 1, count);
}

static inline unsigned io_uring_sq_space_left(struct io_uring *ring)
{
return *ring->sq.kring_entries - (ring->sq.sqe_tail - ring->sq.sqe_head);
}*/
