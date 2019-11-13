use super::*;

// kind: u8 1
// pid: u64 8
// lsn: i64 8
// len: u32 4
// crc: u32 4
/// Log messages have a header of this length.
pub const MSG_HEADER_LEN: usize = 25;

/// Log segments have a header of this length.
pub const SEG_HEADER_LEN: usize = 20;

/// Log messages that are stored as external blobs
/// contain a value (in addition to their header)
/// of this length.
pub const BLOB_INLINE_LEN: usize = std::mem::size_of::<Lsn>();

/// The batch manifest stores the last Lsn
/// needed to recover a writebatch atomically.
pub const BATCH_MANIFEST_INLINE_LEN: usize = std::mem::size_of::<Lsn>();

/// The minimum number of items per segment.
/// Items larger than this fraction of an `io_buf`
/// will be stored as an off-log blob.
pub const MINIMUM_ITEMS_PER_SEGMENT: usize = 4;

/// During testing, this should never be exceeded.
#[allow(unused)]
pub const MAX_SPACE_AMPLIFICATION: f64 = 30.;

pub(crate) const META_PID: PageId = 0;
pub(crate) const COUNTER_PID: PageId = 1;
pub(crate) const BATCH_MANIFEST_PID: PageId = PageId::max_value() - 666;

pub(crate) const PAGE_CONSOLIDATION_THRESHOLD: usize = 10;
