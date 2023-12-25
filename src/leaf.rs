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
}

impl<const LEAF_FANOUT: usize> Default for Leaf<LEAF_FANOUT> {
    fn default() -> Leaf<LEAF_FANOUT> {
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
        }
    }
}
