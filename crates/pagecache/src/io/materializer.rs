use super::*;

/// A user of a `PageCache` needs to provide a `Materializer` which
/// handles the merging of page fragments.
pub trait Materializer {
    /// The possibly fragmented page, written to log storage sequentially, and
    /// read in parallel from multiple locations on disk when serving
    /// a request to read the page. These will be merged to a single version
    /// at read time, and possibly cached.
    type PageFrag;

    /// The higher-level recovery state, as
    /// described by `Materializer::recover`
    type Recovery;

    #[doc(hidden)]
    fn is_null() -> bool
        where Self: Sized
    {
        false
    }

    /// Create a new `Materializer` with the previously recovered
    /// state if any existed.
    fn new(Config, &Option<Self::Recovery>) -> Self where Self: Sized;

    /// Used to merge chains of partial pages into a form
    /// that is useful for the `PageCache` owner.
    fn merge(&self, &[&Self::PageFrag]) -> Self::PageFrag;

    /// Used to feed custom recovery information back to a higher-level abstraction
    /// during startup. For example, a B-Link tree must know what the current
    /// root node is before it can start serving requests.
    fn recover(&self, &Self::PageFrag) -> Option<Self::Recovery>;
}

/// A materializer for things that have nothing to
/// materialize or recover, like a standalone `Log`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NullMaterializer;

impl Materializer for NullMaterializer {
    type PageFrag = ();
    type Recovery = ();

    #[doc(hidden)]
    fn is_null() -> bool {
        true
    }

    fn new(_: Config, _: &Option<Self::Recovery>) -> Self {
        NullMaterializer
    }

    fn merge(&self, _: &[&Self::PageFrag]) -> Self::PageFrag {
        ()
    }

    fn recover(&self, _: &Self::PageFrag) -> Option<Self::Recovery> {
        None
    }
}
