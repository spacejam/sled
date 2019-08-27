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
    Base(Node<IVec>),
    ParentMergeIntention(PageId),
    ParentMergeConfirm,
    ChildMergeCap,
}
