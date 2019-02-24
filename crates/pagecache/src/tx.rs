#![allow(rust_2018_idioms)]

use super::*;

use sled_sync::Guard;

/// A handle to an ongoing pagecache transaction. Ensures
/// that any state which is removed from a shared in-memory
/// data structure is not destroyed until all possible
/// readers have concluded.
pub struct Tx<'a, PM, P>
where
    PM: 'a,
    P: 'static + Send + Sync,
{
    _guard: Guard,
    _pc: &'a PageCache<PM, P>,
}
