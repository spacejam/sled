use std::marker::PhantomData;
use std::ops::Deref;

#[derive(Default, Clone, Debug)]
pub struct Art<T> {
    root: Node<T>,
}

impl<T> Deref for Art<T> {
    type Target = Node<T>;

    fn deref(&self) -> &Node<T> {
        &self.root
    }
}

#[derive(Clone, Debug)]
enum NodeOrLeaf<T> {
    Node(Node<T>),
    Leaf(T),
}

#[derive(Clone, Debug)]
pub struct Node<T> {
    prefix: Vec<u8>,
    data: NodeData<T>,
}

#[derive(Clone, Debug)]
pub enum NodeData<T> {
    Node4 {
        children: [u8; 4],
        ptrs: [Option<NodeOrLeaf<T>>; 4],
    },
    Node16 {
        children: [u8; 16],
        ptrs: [Option<NodeOrLeaf<T>>; 16],
    },
    Node48 {
        children: [u8; 256],
        ptrs: [Option<NodeOrLeaf<T>>; 48],
    },
    Node256([Option<NodeOrLeaf<T>>; 256]),
}

impl Default for NodeData<T> {
    fn default() -> NodeData<T> {
        NodeData::Node4 {
            children: [0, 0, 0, 0],
            ptrs: [None, None, None, None],
        }
    }
}

impl<T> Node<T> {
    pub fn insert(&self, k: Vec<u8>, v: T) {}

    pub fn get<'a>(&self, k: &[u8]) -> Option<&'a T> {
        None
    }
}
