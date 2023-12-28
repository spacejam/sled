use crate::*;

pub enum TriggerTransform {
    PassThrough,
    AccessDenied,
    TransformedValue { value: Option<InlineArray> },
}

#[allow(unused)]
pub trait Trigger: 'static + Send + Sync {
    fn read_trigger(
        &self,
        collection_id: CollectionId,
        key: &[u8],
        read_value: Option<&InlineArray>,
    ) -> TriggerTransform {
        TriggerTransform::PassThrough
    }

    fn write_trigger(
        &self,
        collection_id: CollectionId,
        key: &[u8],
        old_value: Option<&InlineArray>,
        new_value: Option<&InlineArray>,
    ) -> TriggerTransform {
        TriggerTransform::PassThrough
    }
}
