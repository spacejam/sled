use super::*;

/// Frag denotes a tree node or its modification fragment such as
/// key addition or removal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Frag {
    Set(IVec, IVec),
    Del(IVec),
    Base(Node),
    ParentMergeIntention(PageId),
    ParentMergeConfirm,
    ChildMergeCap,
}

impl Frag {
    pub fn merge(&mut self, other: &Self) {
        if let Frag::Base(ref mut base) = self {
            base.apply(other);
        } else {
            panic!("expected base to be the first node");
        }
    }

    fn base(data: Data) -> Frag {
        let mut node = Node::default();
        node.data = data;
        Frag::Base(node)
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
