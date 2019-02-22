use super::*;

/// A pending log reservation which can be aborted or completed.
/// NB the holder should quickly call `complete` or `abort` as
/// taking too long to decide will cause the underlying IO
/// buffer to become blocked.
pub struct Reservation<'a> {
    pub(super) iobufs: &'a IoBufs,
    pub(super) idx: usize,
    pub(super) success_byte: &'a mut u8,
    pub(super) flushed: bool,
    pub(super) ptr: DiskPtr,
    pub(super) lsn: Lsn,
    pub(super) is_blob_rewrite: bool,
}

impl<'a> Drop for Reservation<'a> {
    fn drop(&mut self) {
        // We auto-abort if the user never uses a reservation.
        if !self.flushed {
            self.flush(false).unwrap();
        }
    }
}

impl<'a> Reservation<'a> {
    /// Cancel the reservation, placing a failed flush on disk, returning
    /// the (cancelled) log sequence number and file offset.
    pub fn abort(mut self) -> Result<(Lsn, DiskPtr), ()> {
        if self.ptr.is_blob() {
            if !self.is_blob_rewrite {
                // we don't want to remove this blob if something
                // else may still be using it.

                trace!(
                    "removing blob for aborted reservation at lsn {}",
                    self.ptr
                );

                remove_blob(
                    self.ptr.blob().1,
                    &self.iobufs.0.config,
                )?;
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
        self.ptr.lid()
    }

    /// Get the log sequence number for this update.
    pub fn lsn(&self) -> Lsn {
        self.lsn
    }

    /// Get the underlying storage location for the written value.
    /// Note that an blob write still has a pointer in the
    /// log at the provided lid location.
    pub fn ptr(&self) -> DiskPtr {
        self.ptr.clone()
    }

    fn flush(&mut self, valid: bool) -> Result<(Lsn, DiskPtr), ()> {
        if self.flushed {
            panic!("flushing already-flushed reservation!");
        }

        self.flushed = true;

        if !valid {
            *self.success_byte = FAILED_FLUSH;
            // don't actually zero the message, still check its hash
            // on recovery to find corruption.
        }

        self.iobufs.exit_reservation(self.idx)?;

        Ok((self.lsn(), self.ptr()))
    }
}
