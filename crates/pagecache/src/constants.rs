use super::*;

/// Indicates that the following buffer corresponds
/// to a reservation for an in-memory operation that
/// failed to complete. It should be skipped during
/// recovery.
pub const FAILED_FLUSH: u8 = 0;

/// Indicates that the following buffer contains
/// valid data, stored inline.
pub const INLINE_FLUSH: u8 = 1;

/// Indicates that the following buffer contains
/// valid data, stored blobly.
pub const BLOB_FLUSH: u8 = 2;

/// Indicates that the following buffer is used
/// as padding to fill out the rest of the segment
/// before sealing it.
pub const SEGMENT_PAD: u8 = 3;

/// Indicates that the following buffer contains
/// an Lsn for the last write in an atomic writebatch.
pub const BATCH_MANIFEST: u8 = 4;

/// The EVIL_BYTE is written as a canary to help
/// detect torn writes.
pub const EVIL_BYTE: u8 = 6;

/// Log messages have a header of this length.
pub const MSG_HEADER_LEN: usize = 17;

/// Log segments have a header of this length.
pub const SEG_HEADER_LEN: usize = 12;

/// Log segments have a trailer of this length.
pub const SEG_TRAILER_LEN: usize = 20;

/// Log messages that are stored as external blobs
/// contain a value (in addition to their header)
/// of this length.
pub const BLOB_INLINE_LEN: usize = std::mem::size_of::<Lsn>();

/// The batch manifest stores the last Lsn
/// needed to recover a writebatch atomically.
pub const BATCH_MANIFEST_INLINE_LEN: usize = std::mem::size_of::<Lsn>();

/// The minimum number of items per segment.
/// Items larger than this fraction of an io_buf
/// will be stored as an off-log blob.
pub const MINIMUM_ITEMS_PER_SEGMENT: usize = 4;
