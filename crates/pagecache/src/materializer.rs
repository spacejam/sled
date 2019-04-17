use super::*;

/// A user of a `PageCache` needs to provide a `Materializer` which
/// handles the merging of page fragments.
pub trait Materializer {
    /// The possibly fragmented page, written to log storage sequentially, and
    /// read in parallel from multiple locations on disk when serving
    /// a request to read the page. These will be merged to a single version
    /// at read time, and possibly cached.
    type PageFrag;

    #[doc(hidden)]
    fn is_null() -> bool
    where
        Self: Sized,
    {
        false
    }

    /// Used to merge chains of partial pages into a form
    /// that is useful for the `PageCache` owner.
    fn merge<'a, I>(frags: I, config: &Config) -> Self::PageFrag
    where
        I: IntoIterator<Item = &'a Self::PageFrag>,
        Self::PageFrag: 'a;

    /// Used to determine the size of the value for caching purposes.
    fn size_in_bytes(frag: &Self::PageFrag) -> usize;
}

/// A materializer for things that have nothing to
/// materialize or recover, like a standalone `Log`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NullMaterializer;

impl Materializer for NullMaterializer {
    type PageFrag = ();

    #[doc(hidden)]
    fn is_null() -> bool {
        true
    }

    fn merge<'a, I>(_frags: I, _config: &Config) -> Self::PageFrag
    where
        I: IntoIterator<Item = &'a Self::PageFrag>,
    {
    }

    fn size_in_bytes(_: &Self::PageFrag) -> usize {
        0
    }
}
