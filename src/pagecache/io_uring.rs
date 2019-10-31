#![cfg(all(unix, feature = "io_uring"))]

use libc::off_t;
use std::convert::TryFrom;
use std::io;
use std::mem;
use std::os::unix::io::RawFd;

use crate::LogOffset;
use crate::{Error, Result};

use liburing::*;

/// TODO: rewrite for internal mutability (no &mut self + UnsafeCell)
/// TODO: keep &File, not fd
/// TODO: fix bugs
pub(crate) struct URing {
    /// Mutable unsafe FFI struct from liburing::
    ring: io_uring,

    /// File fd
    fd: RawFd,

    /// IOVec structures that hold information about buffers
    /// used in SQEs.
    /// TODO: keep track of objects refs that must live while async ops are
    iovecs: Vec<libc::iovec>,

    /// List of free slots: iovecs, user data, etc.
    free_slots: Vec<usize>,
}

/// Pointers can't be passed safely through threads boundaries.
/// URing must enforce it, yet it is not Sync.
#[allow(unsafe_code)]
unsafe impl Send for URing {}

impl URing {
    /// Create and initialize new io_uring structure with
    /// `size` queue length and `flags` specified properties.
    ///
    /// Available flags from io_uring.h:
    ///     - IORING_SETUP_IOPOLL	(1U << 0)	/* io_context is polled */
    ///     - IORING_SETUP_SQPOLL	(1U << 1)	/* SQ poll thread */
    ///     - IORING_SETUP_SQ_AFF	(1U << 2)	/* sq_thread_cpu is valid */
    ///     - IORING_SETUP_CQSIZE	(1U << 3)	/* app defines CQ size */
    pub fn new(fd: RawFd, size: usize, flags: libc::c_uint) -> Result<Self> {
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
            ring,
            fd,
            iovecs: Vec::with_capacity(size),
            free_slots: Vec::with_capacity(size),
        };

        unsafe {
            let ret = io_uring_register_files(
                &mut uring.ring,
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

    unsafe fn drain_cqe(&mut self) -> Result<()> {
        loop {
            let mut cqe: *mut io_uring_cqe = std::mem::zeroed();
            let ret = io_uring_peek_cqe(&mut self.ring, &mut cqe);
            if ret == -libc::EAGAIN {
                // peek found nothing, no completions to drain
                return Ok(());
            }
            if ret < 0 {
                return Err(Error::Io(io::Error::from_raw_os_error(ret)));
            }
            let i = (*cqe).user_data;
            if i < self.free_slots.len() as u64 {
                self.free_slots.push(i as usize);
            }
            io_uring_cqe_seen(&mut self.ring, cqe);
        }
    }

    pub fn write_at(
        &mut self,
        buf: &mut [u8],
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

            // 3. build write() SQE
            {
                let sqe = io_uring_get_sqe(&mut self.ring);
                if sqe == std::ptr::null_mut() {
                    return Err(Error::Io(io::Error::new(
                        io::ErrorKind::Other,
                        "unexpected lack of sqes",
                    )));
                }

                let i = self.free_slots.pop().unwrap();
                self.iovecs[i].iov_base =
                    buf.as_mut_ptr() as *mut std::ffi::c_void;
                self.iovecs[i].iov_len = buf.len();

                io_uring_prep_writev(
                    sqe,
                    self.fd,
                    &mut self.iovecs[i],
                    1,
                    offset as off_t,
                );
                (*sqe).user_data = u64::try_from(i).unwrap();
            }

            // 4. build fsync() SQE
            if fsync {
                let sqe = io_uring_get_sqe(&mut self.ring);
                if sqe == std::ptr::null_mut() {
                    return Err(Error::Io(io::Error::new(
                        io::ErrorKind::Other,
                        "unexpected lack of sqes",
                    )));
                }

                let i = self.free_slots.pop().unwrap();

                io_uring_prep_fsync(sqe, self.fd, IORING_FSYNC_DATASYNC);
                io_uring_sqe_set_flags(sqe, IOSQE_IO_DRAIN);
                (*sqe).user_data = u64::try_from(i).unwrap();
            }

            // 5. submit
            let ret = io_uring_submit(&mut self.ring);
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
            io_uring_unregister_files(&mut self.ring);
            io_uring_queue_exit(&mut self.ring);
        };
    }
}