use super::*;

/// A user of a `PageCache` needs to provide a `Materializer` which
/// handles the merging of page fragments.
pub trait Materializer:
    'static + Debug + Clone + Serialize + DeserializeOwned + Send + Sync
{
    /// Used to merge chains of partial pages into a form
    /// that is useful for the `PageCache` owner.
    fn merge(&mut self, other: &Self);
}
