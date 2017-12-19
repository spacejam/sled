//! This module contains the systems that deal with files
//! directly.
use std::cell::UnsafeCell;
use std::collections::BTreeMap;
use std::fmt::{self, Debug};
use std::io;
use std::sync::Arc;
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
mod periodic_flusher;
mod reader;
mod reservation;
mod segment;
mod snapshot;

#[doc(hidden)]
pub use self::log::{LogRead, MSG_HEADER_LEN, SEG_HEADER_LEN, SEG_TRAILER_LEN};

#[doc(hidden)]
pub use self::snapshot::Snapshot;

pub use self::log::Log;
pub use self::materializer::Materializer;
pub use self::page_cache::{CacheEntry, PageCache};
pub use self::reservation::Reservation;
pub use self::segment::SegmentMode;

use self::log::{MessageHeader, SegmentHeader, SegmentTrailer};
use self::iobuf::IoBufs;
use self::iterator::LogIter;
use self::page_cache::{LoggedUpdate, Update};
use self::parallel_io::Pio;
use self::segment::{Segment, SegmentAccountant, raw_segment_iter};
use self::snapshot::{advance_snapshot, read_snapshot_or_default};
