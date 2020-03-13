use super::*;

// crc: u32 4
// kind: u8 1
// seg num: u64 9 (varint)
// pid: u64 9 (varint)
// len: u64 9 (varint)
/// Log messages have a header that might eb up to this length.
pub const MAX_MSG_HEADER_LEN: usize = 32;

/// Log segments have a header of this length.
pub const SEG_HEADER_LEN: usize = 20;

/// The minimum number of items per segment.
/// Items larger than this fraction of an `io_buf`
/// will be stored as an off-log blob.
pub const MINIMUM_ITEMS_PER_SEGMENT: usize = 4;

/// During testing, this should never be exceeded.
#[allow(unused)]
pub const MAX_SPACE_AMPLIFICATION: f64 = 5.;

pub(crate) const META_PID: PageId = 0;
pub(crate) const COUNTER_PID: PageId = 1;
pub(crate) const BATCH_MANIFEST_PID: PageId = PageId::max_value() - 666;

pub(crate) const PAGE_CONSOLIDATION_THRESHOLD: usize = 10;
pub(crate) const SEGMENT_CLEANUP_THRESHOLD: usize = 50;

// Allows for around 1 trillion items to be stored
// 2^37 * (assuming 50% node fill, 8 items per leaf)
// and well below 1% of nodes being non-leaf nodes.
#[cfg(target_pointer_width = "64")]
pub(crate) const MAX_PID_BITS: usize = 37;

// Allows for around 32 billion items to be stored
// 2^32 * (assuming 50% node fill of 8 items per leaf)
// and well below 1% of nodes being non-leaf nodes.
// Assumed to be enough for a 32-bit system.
#[cfg(target_pointer_width = "32")]
pub(crate) const MAX_PID_BITS: usize = 32;
