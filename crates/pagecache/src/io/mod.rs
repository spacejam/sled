//! This module contains the systems that deal with files
//! directly.
use std::cell::UnsafeCell;
use std::fmt::{self, Debug};
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use bincode::{Infinite, deserialize, serialize};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::*;

mod iobuf;
mod iterator;
mod log;
mod materializer;
mod page_cache;
mod parallel_io;
mod reader;
mod reservation;
mod segment;
mod snapshot;

#[doc(hidden)]
pub use self::log::{LogRead, MSG_HEADER_LEN, SEG_HEADER_LEN, SEG_TRAILER_LEN};

pub(super) use self::reader::LogReader;

#[doc(hidden)]
pub use self::snapshot::{Snapshot, read_snapshot_or_default};

pub use self::log::Log;
pub use self::materializer::{Materializer, NullMaterializer};
pub use self::page_cache::{CacheEntry, PageCache, PageGet};
pub use self::reservation::Reservation;
pub use self::segment::SegmentMode;

use self::log::{MessageHeader, MessageKind, SegmentHeader, SegmentTrailer};
use self::iobuf::IoBufs;
use self::iterator::LogIter;
use self::page_cache::{LoggedUpdate, Update};
use self::parallel_io::Pio;
use self::segment::{SegmentAccountant, raw_segment_iter_from};
use self::snapshot::{PageState, advance_snapshot};

// The EVIL_BYTE is written to force detection of
// a corruption when dealing with unused segment space.
const EVIL_BYTE: u8 = 6;

// This message represents valid data.
const SUCCESSFUL_FLUSH: u8 = 1;

// This message should be skipped to preserve linearizability.
const FAILED_FLUSH: u8 = 0;

// This message represents a pad.
const SEGMENT_PAD: u8 = 2;
