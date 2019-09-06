use std::sync::Arc;
use std::result::Result as StdResult;

use bincode::deserialize;

use serde::de::DeserializeOwned;
use serde::{
    Deserialize, Serialize,
    {de::Deserializer, ser::Serializer},
};

use super::*;

#[derive(Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
pub(crate) enum LazyDeserialized<T>
where
    T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static
{
    Serialized(IVec),
    Deserialized(Arc<T>),
}

impl<T> Serialize for LazyDeserialized<T>
where
    T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static
{
    fn serialize<S: Serializer>(
        &self,
        serializer: S,
    ) -> StdResult<S::Ok, S::Error> {
        match self {
            LazyDeserialized::Serialized(bytes) =>
                serde_bytes::serialize(bytes.as_ref(), serializer),
            LazyDeserialized::Deserialized(arc_t) => {
                arc_t.serialize(serializer)
            }
        }
    }
}

impl<'de, T> Deserialize<'de> for LazyDeserialized<T>
where
    T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static
{
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D,
    ) -> StdResult<Self, D::Error> {
        let bytes: Box<[u8]> =
            serde_bytes::deserialize(deserializer)?;

        Ok(LazyDeserialized::Serialized(IVec::from(bytes)))
    }
}

impl<T> LazyDeserialized<T>
where
    T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static
{
    fn read(&mut self) -> Result<&Arc<T>> {
        self.upgrade()?;
        if let LazyDeserialized::Deserialized(t) = self {
            Ok(t)
        } else {
            panic!("should only write deserialized items");
        }
    }

    fn write<F>(&mut self, f: F) -> Result<()>
    where
        F: Fn(&mut T),
    {
        self.upgrade()?;
        if let LazyDeserialized::Deserialized(mut t) = self {
            let mutable_t = Arc::make_mut(&mut t);
            f(mutable_t);
            Ok(())
        } else {
            panic!("should only write deserialized items");
        }
    }

    fn upgrade(&mut self) -> Result<()> {
        if let LazyDeserialized::Serialized(ivec) = self {
            let t: T = deserialize(&ivec).map_err(|_| Error::IncorrectType)?;
            *self = LazyDeserialized::Deserialized(Arc::new(t));
        }
        Ok(())
    }
}
