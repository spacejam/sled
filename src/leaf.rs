use crate::*;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct Leaf<const LEAF_FANOUT: usize> {
    pub lo: InlineArray,
    pub hi: Option<InlineArray>,
    pub prefix_length: usize,
    pub data: stack_map::StackMap<InlineArray, InlineArray, LEAF_FANOUT>,
    pub in_memory_size: usize,
    pub mutation_count: u64,
    #[serde(skip)]
    pub dirty_flush_epoch: Option<FlushEpoch>,
    #[serde(skip)]
    pub page_out_on_flush: Option<FlushEpoch>,
    #[serde(skip)]
    pub deleted: Option<FlushEpoch>,
    #[serde(skip)]
    pub max_unflushed_epoch: Option<FlushEpoch>,
}

impl<const LEAF_FANOUT: usize> Leaf<LEAF_FANOUT> {
    pub(crate) fn empty() -> Leaf<LEAF_FANOUT> {
        Leaf {
            lo: InlineArray::default(),
            hi: None,
            prefix_length: 0,
            data: stack_map::StackMap::default(),
            // this does not need to be marked as dirty until it actually
            // receives inserted data
            dirty_flush_epoch: None,
            in_memory_size: std::mem::size_of::<Leaf<LEAF_FANOUT>>(),
            mutation_count: 0,
            page_out_on_flush: None,
            deleted: None,
            max_unflushed_epoch: None,
        }
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub(crate) fn set_dirty_epoch(&mut self, epoch: FlushEpoch) {
        assert!(self.deleted.is_none());
        if let Some(current_epoch) = self.dirty_flush_epoch {
            assert!(current_epoch <= epoch);
        }
        if self.page_out_on_flush < Some(epoch) {
            self.page_out_on_flush = None;
        }
        self.dirty_flush_epoch = Some(epoch);
    }

    fn prefix(&self) -> &[u8] {
        assert!(self.deleted.is_none());
        &self.lo[..self.prefix_length]
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<&InlineArray> {
        assert!(self.deleted.is_none());
        let prefixed_key = if self.prefix_length == 0 {
            key
        } else {
            let prefix = self.prefix();
            assert!(key.starts_with(prefix));
            &key[self.prefix_length..]
        };
        self.data.get(prefixed_key)
    }

    pub(crate) fn insert(
        &mut self,
        key: InlineArray,
        value: InlineArray,
    ) -> Option<InlineArray> {
        assert!(self.deleted.is_none());
        let prefixed_key = if self.prefix_length == 0 {
            key
        } else {
            let prefix = self.prefix();
            assert!(key.starts_with(prefix));
            key[self.prefix_length..].into()
        };
        self.data.insert(prefixed_key, value)
    }

    pub(crate) fn remove(&mut self, key: &[u8]) -> Option<InlineArray> {
        assert!(self.deleted.is_none());
        let prefix = self.prefix();
        assert!(key.starts_with(prefix));
        let partial_key = &key[self.prefix_length..];
        self.data.remove(partial_key)
    }
}
