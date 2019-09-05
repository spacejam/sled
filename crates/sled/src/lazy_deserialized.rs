use std::sync::Arc;

use bincode::deserialize;

use super::*;

#[derive(
    Clone, Debug, Ord, Eq, PartialOrd, PartialEq, Deserialize, Serialize,
)]
pub(crate) enum LazyDeserialized<T: ?Sized> {
    Serialized(IVec),
    Deserialized(Arc<T>),
}

impl<T> LazyDeserialized<T> {
    fn read(&mut self) -> Result<&Arc<T>> {
        self.upgrade()?;
    }

    fn write<F>(&mut self, f: F) -> Result<()>
    where
        F: Fn(&mut T),
    {
        self.upgrade()?;
        if let LazyDeserialized::Deserialized(mut t) = self {
            let mutable_t = t.get_mut();
            f(mutable_t);
            Ok(())
        } else {
            panic!("should only write deserialized items");
        }
    }

    fn upgrade(&mut self) -> Result<()> {
        if let LazyDeserialized::Serialized(ivec) = self {
            let t = deserialize(&ivec)?;
            *self = LazyDeserialized::Deserialized(t);
        }
        Ok(())
    }
}
