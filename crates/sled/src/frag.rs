use super::*;

// TODO
// Merged
// LeftMerge(head: Raw, rhs: PageId, hi: Bound)
// ParentMerge(lhs: PageId, rhs: PageId)
// TxBegin(TxID), // in-mem
// TxCommit(TxID), // in-mem
// TxAbort(TxID), // in-mem

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Frag {
    Set(Key, Value),
    Del(Key),
    Merge(Key, Value),
    /// The optional page in Base means this node has replaced
    /// the specified page as a new root.
    Base(Node, Option<PageId>),
    ChildSplit(ChildSplit),
    ParentSplit(ParentSplit),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ParentSplit {
    pub at: Bound,
    pub to: PageId,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChildSplit {
    pub at: Bound,
    pub to: PageId,
}
