use super::*;

/// A pending log reservation which can be aborted or completed.
/// NB the holder should quickly call `complete` or `abort` as
/// taking too long to decide will cause the underlying IO
/// buffer to become blocked.
pub struct Reservation<'a> {
    pub(super) iobufs: &'a IoBufs,
    pub(super) idx: usize,
    pub(super) data: Vec<u8>,
    pub(super) destination: &'a mut [u8],
    pub(super) flushed: bool,
    pub(super) lsn: Lsn,
    pub(super) lid: LogId,
    pub(super) is_blob: bool,
    pub(super) is_blob_rewrite: bool,
}

impl<'a> Drop for Reservation<'a> {
    fn drop(&mut self) {
        // We auto-abort if the user never uses a reservation.
        let should_flush = !self.data.is_empty() && !self.flushed;
        if should_flush {
            self.flush(false).unwrap();
        }
    }
}

impl<'a> Reservation<'a> {
    /// Cancel the reservation, placing a failed flush on disk, returning
    /// the (cancelled) log sequence number and file offset.
    pub fn abort(mut self) -> Result<(Lsn, DiskPtr), ()> {
        if self.is_blob {
            let blob_ptr = self.blob_ptr().unwrap();

            if !self.is_blob_rewrite {
                // we don't want to remove this blob if something
                // else may still be using it.

                trace!(
                    "removing blob for aborted reservation at lsn {}",
                    blob_ptr
                );

                remove_blob(blob_ptr, &self.iobufs.0.config)?;
            }
        }

        self.flush(false)
    }

    /// Complete the reservation, placing the buffer on disk. returns
    /// the log sequence number of the write, and the file offset.
    pub fn complete(mut self) -> Result<(Lsn, DiskPtr), ()> {
        self.flush(true)
    }

    /// Get the log file offset for reading this buffer in the future.
    pub fn lid(&self) -> LogId {
        self.lid
    }

    /// Get the log sequence number for this update.
    pub fn lsn(&self) -> Lsn {
        self.lsn
    }

    /// Get the underlying storage location for the written value.
    /// Note that an blob write still has a pointer in the
    /// log at the provided lid location.
    pub fn ptr(&self) -> DiskPtr {
        if let Some(blob_ptr) = self.blob_ptr() {
            DiskPtr::new_blob(self.lid, blob_ptr)
        } else {
            DiskPtr::new_inline(self.lid)
        }
    }

    fn blob_ptr(&self) -> Option<BlobPointer> {
        if self.is_blob {
            let blob_ptr = arr_to_u64(&self.data[MSG_HEADER_LEN..]) as BlobPointer;

            Some(blob_ptr)
        } else {
            None
        }
    }

    fn flush(&mut self, valid: bool) -> Result<(Lsn, DiskPtr), ()> {
        if self.flushed {
            panic!("flushing already-flushed reservation!");
        }

        self.flushed = true;

        if !valid {
            self.data[0] = FAILED_FLUSH;
            // don't actually zero the message, still check its hash
            // on recovery to find corruption.
        }

        unsafe {
            std::ptr::copy_nonoverlapping(
                self.data.as_ptr(),
                self.destination.as_mut_ptr(),
                self.data.len(),
            );
        }

        self.iobufs.exit_reservation(self.idx)?;

        Ok((self.lsn(), self.ptr()))
    }
}
