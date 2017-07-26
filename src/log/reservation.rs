use super::*;

pub struct Reservation<'a> {
    pub iobufs: &'a IOBufs,
    pub idx: usize,
    pub data: Vec<u8>,
    pub destination: &'a mut [u8],
    pub flushed: bool,
    pub base_disk_offset: LogID,
}

unsafe impl<'a> Send for Reservation<'a> {}

impl<'a> Drop for Reservation<'a> {
    fn drop(&mut self) {
        // We auto-abort if the user never uses a reservation.
        let should_flush = !self.data.is_empty() && !self.flushed;
        if should_flush {
            self.flush(false);
        }
    }
}

impl<'a> Reservation<'a> {
    /// cancel the reservation, placing a failed flush on disk
    pub fn abort(mut self) {
        self.flush(false);
    }

    /// complete the reservation, placing the buffer on disk at the log_id
    pub fn complete(mut self) -> LogID {
        self.flush(true)
    }

    /// get the log_id for accessing this buffer in the future
    pub fn log_id(&self) -> LogID {
        self.base_disk_offset
    }

    fn flush(&mut self, valid: bool) -> LogID {
        if self.flushed {
            panic!("flushing already-flushed reservation!");
        }

        self.flushed = true;

        if !valid {
            zero_failed_buf(&mut self.data);
        }

        self.destination.copy_from_slice(&*self.data);

        self.iobufs.exit_reservation(self.idx);

        self.log_id()
    }
}

#[inline(always)]
fn zero_failed_buf(buf: &mut [u8]) {
    if buf.len() < HEADER_LEN {
        panic!("somehow zeroing a buf shorter than HEADER_LEN");
    }

    // zero the valid byte, and the bytes after the size in the header
    buf[0] = 0;
    for mut c in buf[5..].iter_mut() {
        *c = 0;
    }
}
