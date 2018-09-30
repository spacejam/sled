use super::*;

use epoch::Guard;

/// A handle to an ongoing pagecache transaction. Ensures
/// that any state which is removed from a shared in-memory
/// data structure is not destroyed until all possible
/// readers have concluded.
pub struct Tx<'a, PM, P, R>
where
    PM: 'a,
    P: 'static + Send + Sync,
    R: 'a,
{
    _guard: Guard,
    _pc: &'a PageCache<PM, P, R>,
}
