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
    Merge(Key, Value),
    /// The optional page in Base means this node has replaced
    /// the specified page as a new root.
    Base(Node, Option<PageID>),
    ChildSplit(ChildSplit),
    ParentSplit(ParentSplit),
}

impl Frag {
    pub fn base(&self) -> Option<(Node, Option<PageID>)> {
        match *self {
            Frag::Base(ref base, ref root) => Some((base.clone(), *root)),
            _ => None,
        }
    }

    pub fn into_base(self) -> Option<(Node, Option<PageID>)> {
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
