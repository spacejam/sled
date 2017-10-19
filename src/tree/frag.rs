use super::*;

// TODO
// Merged
// LeftMerge(head: Raw, rhs: PageID, hi: Bound)
// ParentMerge(lhs: PageID, rhs: PageID)
// TxBegin(TxID), // in-mem
// TxCommit(TxID), // in-mem
// TxAbort(TxID), // in-mem

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Frag {
    Set(Key, Value),
    Del(Key),
    /// The bool in Base means this node has been a root at some point.
    /// This is useful during recovery to figure out the current root.
    Base(Node, bool),
    ChildSplit(ChildSplit),
    ParentSplit(ParentSplit),
}

impl Frag {
    pub fn base(&self) -> Option<(Node, bool)> {
        match *self {
            Frag::Base(ref base, ref root) => Some((base.clone(), *root)),
            _ => None,
        }
    }

    pub fn into_base(self) -> Option<(Node, bool)> {
        match self {
            Frag::Base(base, root) => Some((base, root)),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ParentSplit {
    pub at: Bound,
    pub to: PageID,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChildSplit {
    pub at: Bound,
    pub to: PageID,
}
