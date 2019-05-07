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

    pub(crate) fn is_child_split(&self) -> bool {
        if let Frag::ChildSplit(_) = self {
            true
        } else {
            false
        }
    }

    pub(crate) fn size_in_bytes(&self) -> u64 {
        use Frag::*;

        std::mem::size_of::<Self>() as u64
            + match self {
                Set(k, v) => (k.len() + v.len()) as u64,
                Del(k) => k.len() as u64,
                Merge(k, v) => (k.len() + v.len()) as u64,
                Base(node) => node.size_in_bytes(),
                ChildSplit(_cs) => 0,
                ParentSplit(_ps) => 0,
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
