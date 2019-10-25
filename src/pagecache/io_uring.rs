#![cfg(all(unix, feature = "io_uring"))]

use libc::off_t;
use std::cell::Cell;
use std::convert::TryFrom;
use std::io;
use std::mem;
use std::os::unix::io::{AsRawFd, RawFd};

use crate::LogOffset;
use crate::Mutex;
use crate::{Error, Result};

use liburing::*;

pub(crate) struct URing {
    /// Mutable unsafe FFI struct from liburing::
    ring: Cell<io_uring>,

    /// File fd
    fd: RawFd,

    /// Submit queue mutex. Threads must synchronize receiving,
    /// filling up and submitting SQEs.
    sm: Mutex<()>,

    /// IOVec structures that hold information about buffers
    /// used in SQEs.
    iovecs: Vec<libc::iovec>,

    /// List of free slots: iovecs, user data, etc.
    free_slots: Vec<usize>,
}

impl URing {
    /// Create and initialize new io_uring structure with
    /// `size` queue length and `flags` specified properties.
    ///
    /// Available flags from io_uring.h:
    ///     - IORING_SETUP_IOPOLL	(1U << 0)	/* io_context is polled */
    ///     - IORING_SETUP_SQPOLL	(1U << 1)	/* SQ poll thread */
    ///     - IORING_SETUP_SQ_AFF	(1U << 2)	/* sq_thread_cpu is valid */
    ///     - IORING_SETUP_CQSIZE	(1U << 3)	/* app defines CQ size */
    pub fn new(
        file: std::fs::File,
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
                return Err(Error::Io(io::Error::from_raw_os_error(ret)));
            }

            s.assume_init()
        };

        let mut uring = URing {
            ring: Cell::new(ring),
            fd: file.as_raw_fd(),
            sm: Mutex::new(()),
            iovecs: Vec::with_capacity(size),
            free_slots: Vec::with_capacity(size),
        };

        unsafe {
            let ret = io_uring_register_files(
                uring.ring.as_ptr(),
                [uring.fd].as_ptr(),
                1,
            );
            if ret < 0 {
                return Err(Error::Io(io::Error::from_raw_os_error(ret)));
            }
        };

        uring.iovecs.resize(
            size,
            libc::iovec { iov_base: std::ptr::null_mut(), iov_len: 0 },
        );

        // index free elements
        for i in 0..size - 1 {
            uring.free_slots.push(i);
        }

        Ok(uring)
    }

    fn drain_cqe(&mut self) -> Result<()> {
        unsafe {
            loop {
                let mut cqe: *mut io_uring_cqe = std::mem::zeroed();
                let ret = io_uring_peek_cqe(self.ring.as_ptr(), &mut cqe);
                if ret < 0 {
                    return Err(Error::Io(io::Error::from_raw_os_error(ret)));
                }
                let i = (*cqe).user_data;
                if i >= 0 && i < self.free_slots.len() as u64 {
                    self.free_slots.push(i as usize);
                }
                io_uring_cqe_seen(self.ring.as_ptr(), cqe);
            }
        }
    }

    pub fn write_at(
        &mut self,
        buf: &mut [u8],
        offset: LogOffset,
        fsync: bool,
    ) -> Result<()> {
        let _m = self.sm.lock();

        unsafe {
            // self.drain_cqe()?;
            //   ^-- immutable borrow happened by _m
            // drain cqe
            loop {
                let mut cqe: *mut io_uring_cqe = std::mem::zeroed();
                let ret = io_uring_peek_cqe(self.ring.as_ptr(), &mut cqe);
                if ret < 0 {
                    return Err(Error::Io(io::Error::from_raw_os_error(ret)));
                }
                let i = (*cqe).user_data;
                if i >= 0 && i < self.free_slots.len() as u64 {
                    self.free_slots.push(i as usize);
                }
                io_uring_cqe_seen(self.ring.as_ptr(), cqe);
            }

            // it is easier to track queue len if num
            // of iovecs will be the same as sqes
            let mut reserve = 1;
            if fsync {
                reserve += 1;
            }

            if self.free_slots.len() < reserve {
                let ret = io_uring_submit_and_wait(
                    self.ring.as_ptr(),
                    reserve as libc::c_uint,
                );
                if ret < 0 {
                    return Err(Error::Io(io::Error::from_raw_os_error(ret)));
                }
                if ret < reserve as libc::c_int {
                    return Err(Error::Io(io::Error::new(
                        io::ErrorKind::Other,
                        "wait for free sqes",
                    )));
                }

                self.drain_cqe()?;
            }

            // build write() sqe
            let sqe = io_uring_get_sqe(self.ring.as_ptr());
            if sqe == std::ptr::null_mut() {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::Other,
                    "unexpected lack of sqes",
                )));
            }

            let i = self.free_slots.pop().unwrap();
            self.iovecs[i].iov_base = buf.as_mut_ptr() as *mut std::ffi::c_void;
            self.iovecs[i].iov_len = buf.len();

            io_uring_prep_writev(
                sqe,
                self.fd,
                &mut self.iovecs[i],
                1,
                offset as off_t,
            );
            (*sqe).user_data = u64::try_from(i).unwrap();

            // build fsync
            if fsync {
                let sqe = io_uring_get_sqe(self.ring.as_ptr());
                if sqe == std::ptr::null_mut() {
                    return Err(Error::Io(io::Error::new(
                        io::ErrorKind::Other,
                        "unexpected lack of sqes",
                    )));
                }

                self.free_slots.pop().unwrap();

                io_uring_prep_fsync(sqe, self.fd, IORING_FSYNC_DATASYNC);
                io_uring_sqe_set_flags(sqe, IOSQE_IO_DRAIN);
                (*sqe).user_data = u64::try_from(i).unwrap();
            }

            // submit
            let ret = io_uring_submit(self.ring.as_ptr());
            if ret < 0 {
                return Err(Error::Io(io::Error::from_raw_os_error(ret)));
            }
        };

        Ok(())
    }
}

impl Drop for URing {
    fn drop(&mut self) {
        unsafe {
            io_uring_unregister_files(self.ring.as_ptr());
            io_uring_queue_exit(self.ring.as_ptr());
        };
    }
}
