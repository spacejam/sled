/// A user of a `PageCache` needs to provide a `Materializer` which
/// handles the merging of page fragments.
pub trait Materializer {
    /// The possibly fragmented page, written to log storage sequentially, and
    /// read in parallel from multiple locations on disk when serving
    /// a request to read the page. These will be merged to a single version
    /// at read time, and possibly cached.
    type PageFrag;

    /// The state returned by a call to `PageCache::recover`, as
    /// described by `Materializer::recover`
    type Recovery;

    /// Used to provide the `Materializer` with the previously recovered
    /// state before receiving new updates during recovery.
    fn initialize_with_previous_recovery(&self, &Self::Recovery) {}

    /// Used to compress long chains of partial pages into a condensed form
    /// during compaction.
    fn merge(&self, &[&Self::PageFrag]) -> Self::PageFrag;

    /// Used to feed custom recovery information back to a higher-level abstraction
    /// during startup. For example, a B-Link tree must know what the current
    /// root node is before it can start serving requests.
    fn recover(&self, &Self::PageFrag) -> Option<Self::Recovery>;
}
