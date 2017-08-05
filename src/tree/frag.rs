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
    Base(tree::Node),
    ChildSplit(ChildSplit),
    ParentSplit(ParentSplit),
}

impl Frag {
    pub fn base(&self) -> Option<tree::Node> {
        match *self {
            Frag::Base(ref base) => Some(base.clone()),
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
