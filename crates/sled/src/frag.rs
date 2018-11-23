use super::*;

// TODO
// Merged
// LeftMerge(head: Raw, rhs: PageId, hi: Bound)
// ParentMerge(lhs: PageId, rhs: PageId)
// TxBegin(TxID), // in-mem
// TxCommit(TxID), // in-mem
// TxAbort(TxID), // in-mem

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum Frag {
    Set(Key, Value),
    Del(Key),
    Merge(Key, Value),
    /// The optional page in Base means this node has replaced
    /// the specified page as a new root.
    Base(Node, Option<PageId>),
    ChildSplit(ChildSplit),
    ParentSplit(ParentSplit),
    BumpCounter(usize),
    CounterBase(usize),
}

impl Frag {
    pub(super) fn unwrap_base(&self) -> &Node {
        if let Frag::Base(base, ..) = self {
            base
        } else {
            panic!("called unwrap_base_ptr on non-Base Frag!")
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ParentSplit {
    pub(crate) at: Bound,
    pub(crate) to: PageId,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ChildSplit {
    pub(crate) at: Bound,
    pub(crate) to: PageId,
}
