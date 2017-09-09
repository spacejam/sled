use std::ptr;

use super::*;

pub struct Reservation<'a> {
    pub iobufs: &'a IoBufs,
    pub idx: usize,
    pub data: Vec<u8>,
    pub destination: &'a mut [u8],
    pub flushed: bool,
    pub reservation_offset: LogID,
}

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
    pub fn abort(mut self) -> LogID {
        self.iobufs.defer_hole_punch(vec![self.log_id()]);
        self.flush(false)
    }

    /// complete the reservation, placing the buffer on disk at the log_id
    pub fn complete(mut self) -> LogID {
        self.flush(true)
    }

    /// get the log_id for accessing this buffer in the future
    pub fn log_id(&self) -> LogID {
        self.reservation_offset
    }

    fn flush(&mut self, valid: bool) -> LogID {
        if self.flushed {
            panic!("flushing already-flushed reservation!");
        }

        self.flushed = true;

        if valid {
            self.destination.copy_from_slice(&*self.data);
        } else {
            // zero the bytes, as aborted reservations skip writing
            unsafe {
                ptr::write_bytes(self.destination.as_ptr() as *mut u8, 0, self.data.len());
            }
        }

        self.iobufs.exit_reservation(self.idx);

        self.log_id()
    }
}
