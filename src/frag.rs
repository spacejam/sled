use super::*;

/// Frag denotes a tree node or its modification fragment such as
/// key addition or removal.
///
///
/// TODO:
///     Merged
///     LeftMerge(head: Raw, rhs: PageId, hi: Bound)
///     ParentMerge(lhs: PageId, rhs: PageId)
///     TxBegin(TxID), // in-mem
///     TxCommit(TxID), // in-mem
///     TxAbort(TxID), // in-mem
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum Frag {
    Set(IVec, IVec),
    Del(IVec),
    Base(Node),
    ParentMergeIntention(PageId),
    ParentMergeConfirm,
    ChildMergeCap,
}

impl Frag {
    fn base(data: Data) -> Frag {
        Frag::Base(Node {
            data,
            next: None,
            lo: vec![].into(),
            hi: vec![].into(),
            merging_child: None,
            merging: false,
            prefix_len: 0,
        })
    }

    pub fn root(data: Data) -> Frag {
        assert!(data.is_index(), "root node must has index data type");
        Frag::base(data)
    }

    pub fn leaf(data: Data) -> Frag {
        assert!(data.is_leaf(), "leaf node must has leaf data type");
        Frag::base(data)
    }
}
