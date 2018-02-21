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
    pub(super) lid: LogID,
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
    pub fn abort(mut self) -> CacheResult<(Lsn, LogID), ()> {
        self.flush(false)
    }

    /// Complete the reservation, placing the buffer on disk. returns
    /// the log sequence number of the write, and the file offset.
    pub fn complete(mut self) -> CacheResult<(Lsn, LogID), ()> {
        self.flush(true)
    }

    /// Get the log file offset for reading this buffer in the future.
    pub fn lid(&self) -> LogID {
        self.lid
    }

    /// Get the log sequence number for this update.
    pub fn lsn(&self) -> Lsn {
        self.lsn
    }

    fn flush(&mut self, valid: bool) -> CacheResult<(Lsn, LogID), ()> {
        if self.flushed {
            panic!("flushing already-flushed reservation!");
        }

        self.flushed = true;

        if !valid {
            self.data[0] = FAILED_FLUSH;
            // don't actually zero the message, still check its hash
            // on recovery to find corruption.
        }

        self.destination.copy_from_slice(&*self.data);

        self.iobufs.exit_reservation(self.idx)?;

        Ok((self.lsn(), self.lid()))
    }
}
