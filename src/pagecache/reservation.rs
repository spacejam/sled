#![forbid(unsafe_code)]

use crate::{pagecache::*, *};
use std::ops::Range;

/// A pending log reservation which can be aborted or completed.
/// NB the holder should quickly call `complete` or `abort` as
/// taking too long to decide will cause the underlying IO
/// buffer to become blocked.
#[derive(Debug)]
pub struct Reservation<'a> {
    pub(super) log: &'a Log,
    pub(super) iobuf: Arc<IoBuf>,
    pub(super) buf_range: Range<usize>,
    pub(super) flushed: bool,
    pub pointer: DiskPtr,
    pub lsn: Lsn,
    pub(super) is_heap_item_rewrite: bool,
    pub(super) header_len: usize,
}

impl<'a> Drop for Reservation<'a> {
    fn drop(&mut self) {
        // We auto-abort if the user never uses a reservation.
        if !self.flushed {
            if let Err(e) = self.flush(false) {
                self.log.iobufs.set_global_error(e);
            }
        }
    }
}

impl<'a> Reservation<'a> {
    #[inline]
    fn buf(&self) -> &mut [u8] {
        &mut self.iobuf.get_mut()[self.buf_range.clone()]
    }
    /// Cancel the reservation, placing a failed flush on disk, returning
    /// the (cancelled) log sequence number and file offset.
    pub fn abort(mut self) -> Result<(Lsn, DiskPtr)> {
        if self.pointer.is_heap_item() && !self.is_heap_item_rewrite {
            // we can instantly free this heap item because its pointer
            // is assumed to have failed to have been installed into
            // the pagetable, so we can assume nobody is operating
            // on it.

            trace!(
                "removing heap item for aborted reservation at lsn {}",
                self.pointer
            );

            self.log.config.heap.free(self.pointer.heap_id().unwrap());
        }

        self.flush(false)
    }

    /// Complete the reservation, placing the buffer on disk. returns
    /// the log sequence number of the write, and the file offset.
    pub fn complete(mut self) -> Result<(Lsn, DiskPtr)> {
        self.flush(true)
    }

    /// Refills the reservation buffer with new data.
    /// Must supply a buffer of an identical length
    /// as the one initially provided.
    ///
    /// # Panics
    ///
    /// Will panic if the reservation is not the correct
    /// size to hold a serialized Lsn.
    #[doc(hidden)]
    pub fn mark_writebatch(self, peg_lsn: Lsn) -> Result<(Lsn, DiskPtr)> {
        trace!(
            "writing batch required stable lsn {} into \
             BatchManifest at lid {:?} peg_lsn {}",
            peg_lsn,
            self.pointer.lid(),
            self.lsn
        );

        let this_buf = self.buf();
        if self.lsn == peg_lsn {
            // this can happen because high-level tree updates
            // may result in no work happening.
            self.abort()
        } else {
            this_buf[4] = MessageKind::BatchManifest.into();
            let dst = &mut this_buf[self.header_len..];

            let buf = lsn_to_arr(peg_lsn);
            dst.copy_from_slice(&buf);

            let mut intervals = self.log.iobufs.intervals.lock();
            intervals.mark_batch((self.lsn, peg_lsn));
            drop(intervals);

            self.complete()
        }
    }

    fn flush(&mut self, valid: bool) -> Result<(Lsn, DiskPtr)> {
        if self.flushed {
            panic!("flushing already-flushed reservation!");
        }

        self.flushed = true;
        let buf = self.buf();

        if !valid {
            buf[4] = MessageKind::Canceled.into();

            // zero the message contents to prevent UB
            buf[self.header_len..].fill(0);
        }

        // zero the crc bytes to prevent UB
        buf[0..4].fill(0);

        let crc32 = calculate_message_crc32(
            &buf[..self.header_len],
            &buf[self.header_len..],
        );
        let crc32_arr = u32_to_arr(crc32);

        buf[0..4].copy_from_slice(&crc32_arr);
        self.log.exit_reservation(&self.iobuf)?;

        Ok((self.lsn, self.pointer))
    }
}
