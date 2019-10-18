use crate::{pagecache::*, *};

/// A pending log reservation which can be aborted or completed.
/// NB the holder should quickly call `complete` or `abort` as
/// taking too long to decide will cause the underlying IO
/// buffer to become blocked.
pub struct Reservation<'a> {
    pub(super) log: &'a Log,
    pub(super) iobuf: Arc<IoBuf>,
    pub(super) buf: &'a mut [u8],
    pub(super) flushed: bool,
    pub(super) pointer: DiskPtr,
    pub(super) lsn: Lsn,
    pub(super) is_blob_rewrite: bool,
}

impl<'a> Drop for Reservation<'a> {
    fn drop(&mut self) {
        // We auto-abort if the user never uses a reservation.
        if !self.flushed {
            let _written = self.flush(false).unwrap();
        }
    }
}

impl<'a> Reservation<'a> {
    /// Cancel the reservation, placing a failed flush on disk, returning
    /// the (cancelled) log sequence number and file offset.
    pub fn abort(mut self) -> Result<(Lsn, DiskPtr)> {
        if self.pointer.is_blob() && !self.is_blob_rewrite {
            // we don't want to remove this blob if something
            // else may still be using it.

            trace!(
                "removing blob for aborted reservation at lsn {}",
                self.pointer
            );

            remove_blob(self.pointer.blob().1, &self.log.config)?;
        }

        self.flush(false)
    }

    /// Complete the reservation, placing the buffer on disk. returns
    /// the log sequence number of the write, and the file offset.
    pub fn complete(mut self) -> Result<(Lsn, DiskPtr)> {
        self.flush(true)
    }

    /// Get the log file offset for reading this buffer in the future.
    pub fn lid(&self) -> LogOffset {
        self.pointer.lid()
    }

    /// Get the log sequence number for this update.
    pub fn lsn(&self) -> Lsn {
        self.lsn
    }

    /// Get the underlying storage location for the written value.
    /// Note that an blob write still has a pointer in the
    /// log at the provided lid location.
    pub fn pointer(&self) -> DiskPtr {
        self.pointer
    }

    /// Returns the length of the on-log reservation.
    pub fn reservation_len(&self) -> usize {
        self.buf.len()
    }

    /// Refills the reservation buffer with new data.
    /// Must supply a buffer of an identical length
    /// as the one initially provided. Don't use this
    /// on messages subject to compression etc...
    ///
    /// # Panics
    ///
    /// Will panic if the reservation is not the correct
    /// size to hold a serialized Lsn.
    #[doc(hidden)]
    pub fn mark_writebatch(&mut self, lsn: Lsn) {
        trace!(
            "writing batch required stable lsn {} into \
             BatchManifest at lid {} lsn {}",
            lsn,
            self.pointer.lid(),
            self.lsn
        );

        self.buf[0] = MessageKind::BatchManifest.into();

        let buf = u64_to_arr(u64::try_from(lsn).unwrap());

        let dst = &mut self.buf[MSG_HEADER_LEN..];

        dst.copy_from_slice(&buf);
    }

    fn flush(&mut self, valid: bool) -> Result<(Lsn, DiskPtr)> {
        if self.flushed {
            panic!("flushing already-flushed reservation!");
        }

        self.flushed = true;

        if !valid {
            // don't actually zero the message, still check its hash
            // on recovery to find corruption.
            self.buf[0] = MessageKind::Cancelled.into();
        }

        // the order of hashing must be the
        // same here as during calls to
        // LogReader::read_message
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.buf[MSG_HEADER_LEN..]);
        hasher.update(&self.buf[..MSG_HEADER_LEN]);
        let crc32 = hasher.finalize();
        let crc32_arr = u32_to_arr(crc32 ^ 0xFFFF_FFFF);

        #[allow(unsafe_code)]
        unsafe {
            std::ptr::copy_nonoverlapping(
                crc32_arr.as_ptr(),
                self.buf
                    .as_mut_ptr()
                    .add(MSG_HEADER_LEN - std::mem::size_of::<u32>()),
                std::mem::size_of::<u32>(),
            );
        }
        self.log.exit_reservation(&self.iobuf)?;

        Ok((self.lsn(), self.pointer()))
    }
}
