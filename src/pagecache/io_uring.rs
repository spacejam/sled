#![cfg(all(unix, feature = "io_uring"))]

use libc::off_t;
use std::convert::TryFrom;
use std::fs::File;
use std::io;
use std::mem;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;

use crate::LogOffset;
use crate::{Error, Result};

use liburing::*;

/// TODO: support for SQPOLL with idle timeout
pub(crate) struct Uring<T> {
    /// Mutable unsafe FFI struct from liburing::
    ring: io_uring,

    /// File
    file: Arc<File>,

    /// IOVec structures that hold information about buffers
    /// used in SQEs.
    iovecs: Vec<libc::iovec>,

    /// iovec buffs. this is specifically needed to preserve buf
    /// ptr lifetime because all write operations are async.
    buf: Vec<Option<Arc<T>>>,

    /// List of free slots: iovecs, user data, etc.
    free_slots: Vec<usize>,
}

/// Pointers can't be passed safely through threads boundaries.
/// `Uring` must enforce it, yet it is not Sync.
#[allow(unsafe_code)]
unsafe impl<T> Send for Uring<T> {}

impl<T> Uring<T> {
    /// Create and initialize new `io_uring` structure with
    /// `size` queue length and `flags` specified properties.
    ///
    /// Available flags from `io_uring.h`:
    /// - `IORING_SETUP_IOPOLL`	(1U << 0)	/* `io_context` is polled */
    /// - `IORING_SETUP_SQPOLL`	(1U << 1)	/* SQ poll thread */
    /// - `IORING_SETUP_SQ_AFF`	(1U << 2)	/* `sq_thread_cpu` is valid */
    /// - `IORING_SETUP_CQSIZE`	(1U << 3)	/* app defines CQ size */
    pub fn new(
        file: Arc<File>,
        size: usize,
        flags: libc::c_uint,
    ) -> Result<Self> {
        if size & 1 != 0 || size < 2 {
            return Err(Error::Unsupported("invalid queue size".into()));
        }

        let ring = unsafe {
            let mut s = mem::MaybeUninit::<io_uring>::uninit();
            let ret = io_uring_queue_init(
                u32::try_from(size).unwrap(),
                s.as_mut_ptr(),
                flags,
            );
            if ret < 0 {
                return Err(Error::Io(io::Error::from_raw_os_error(-ret)));
            }

            s.assume_init()
        };

        let mut uring = Uring::<T> {
            ring,
            file,
            iovecs: Vec::with_capacity(size),
            buf: Vec::with_capacity(size),
            free_slots: Vec::with_capacity(size),
        };

        unsafe {
            let ret = io_uring_register_files(
                &mut uring.ring,
                [uring.file.as_raw_fd()].as_ptr(),
                1,
            );
            if ret < 0 {
                return Err(Error::Io(io::Error::from_raw_os_error(-ret)));
            }
        };

        uring.iovecs.resize(
            size,
            libc::iovec { iov_base: std::ptr::null_mut(), iov_len: 0 },
        );
        uring.buf.resize(size, None);

        // index free elements
        for i in 0..size {
            uring.free_slots.push(i);
        }

        Ok(uring)
    }

    unsafe fn drain_cqe(&mut self) -> Result<()> {
        loop {
            let mut cqe: *mut io_uring_cqe = std::ptr::null_mut();
            let ret = io_uring_peek_cqe(&mut self.ring, &mut cqe);
            if ret == -libc::EAGAIN {
                // peek found nothing, no completions to drain
                return Ok(());
            }
            if ret < 0 {
                return Err(Error::Io(io::Error::from_raw_os_error(-ret)));
            }
            let i = usize::try_from((*cqe).user_data).unwrap();
            if (*cqe).res < 0 {
                // ignore failed fsyncs with invalid argument due to the
                // overlap with executing ones: 2 drain fsync do not exec twice.
                if (*cqe).res != -libc::EINVAL
                    || i >= self.free_slots.capacity()
                    || self.iovecs[i].iov_len != 0
                {
                    return Err(Error::Io(io::Error::from_raw_os_error(
                        -(*cqe).res,
                    )));
                }
            }
            if i < self.free_slots.capacity() {
                // println!("release i = {}", i);
                self.free_slots.push(i);
                // release
                self.iovecs[i].iov_base = std::ptr::null_mut();
                self.iovecs[i].iov_len = 0;
                // drop arc buf
                self.buf[i] = None;
            }
            io_uring_cqe_seen(&mut self.ring, cqe);
        }
    }

    pub fn pwrite_all(
        &mut self,
        buf: &mut [u8],
        data: Arc<T>,
        offset: LogOffset,
        fsync: bool,
    ) -> Result<()> {
        unsafe {
            // 1. drain completed operations
            self.drain_cqe()?;

            // it is easier to track queue len if num
            // of iovecs will be the same as sqes
            let mut reserve = 1;
            if fsync {
                reserve += 1;
            }

            // 2. reserve SQE slots
            if self.free_slots.len() < reserve {
                let ret = io_uring_submit_and_wait(
                    &mut self.ring,
                    libc::c_uint::try_from(reserve).unwrap(),
                );
                if ret < 0 {
                    return Err(Error::Io(io::Error::from_raw_os_error(-ret)));
                }
                self.drain_cqe()?;
                if self.free_slots.len() < reserve {
                    return Err(Error::Io(io::Error::new(
                        io::ErrorKind::Other,
                        "wait for free sqes",
                    )));
                }
            }

            // 3. build write() SQE
            {
                let sqe = io_uring_get_sqe(&mut self.ring);
                if sqe.is_null() {
                    return Err(Error::Io(io::Error::new(
                        io::ErrorKind::Other,
                        "unexpected lack of sqes",
                    )));
                }

                let i = self.free_slots.pop().unwrap();
                self.buf[i] = Some(data);
                self.iovecs[i].iov_base =
                    buf.as_mut_ptr() as *mut std::ffi::c_void;
                self.iovecs[i].iov_len = buf.len();

                io_uring_prep_writev(
                    sqe,
                    // for registered file at index 0
                    // self.file.as_raw_fd(),
                    0,
                    &self.iovecs[i],
                    1,
                    off_t::try_from(offset).unwrap(),
                );
                io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
                (*sqe).user_data = u64::try_from(i).unwrap();
                // println!("write with i = {}", i);
            }

            // 4. build fsync() SQE
            if fsync {
                let sqe = io_uring_get_sqe(&mut self.ring);
                if sqe.is_null() {
                    return Err(Error::Io(io::Error::new(
                        io::ErrorKind::Other,
                        "unexpected lack of sqes",
                    )));
                }

                let i = self.free_slots.pop().unwrap();
                // mark explicitly that this is fsync op and it has no data.
                // drain_cqe must ignore fsyncs that got EINVAL due to the
                // overlap with the existing executing fsyncs.
                self.iovecs[i].iov_len = 0;

                io_uring_prep_fsync(
                    sqe,
                    // for registered file at index 0
                    // self.file.as_raw_fd(),
                    0,
                    IORING_FSYNC_DATASYNC,
                );
                // io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
                io_uring_sqe_set_flags(sqe, IOSQE_IO_DRAIN | IOSQE_FIXED_FILE);
                (*sqe).user_data = u64::try_from(i).unwrap();
                // println!("fsync with i = {}", i);
            }

            // 5. submit
            let ret = io_uring_submit(&mut self.ring);
            if ret < 0 {
                return Err(Error::Io(io::Error::from_raw_os_error(-ret)));
            }
        };

        Ok(())
    }
}

impl<T> Drop for Uring<T> {
    fn drop(&mut self) {
        unsafe {
            // flush unfinished ops
            if self.free_slots.len() < self.iovecs.len() {
                let ret = io_uring_submit_and_wait(
                    &mut self.ring,
                    u32::try_from(self.iovecs.len() - self.free_slots.len())
                        .unwrap(),
                );
                if ret < 0 {
                    panic!(Error::Io(io::Error::from_raw_os_error(-ret)));
                }
            }

            self.drain_cqe().unwrap();

            // auto on exit: io_uring_unregister_files(&mut self.ring);
            io_uring_queue_exit(&mut self.ring);
        };
    }
}
