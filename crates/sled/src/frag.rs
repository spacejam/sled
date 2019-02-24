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
    Set(IVec, IVec),
    Del(IVec),
    Merge(IVec, IVec),
    Base(Node),
    ChildSplit(ChildSplit),
    ParentSplit(ParentSplit),
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
    pub(crate) at: IVec,
    pub(crate) to: PageId,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ChildSplit {
    pub(crate) at: IVec,
    pub(crate) to: PageId,
}
