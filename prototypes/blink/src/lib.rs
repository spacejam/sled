/// An in-memory b-link tree.

// notes
// * during all traversals, merges or splits may be encountered
// * if a partial SMO is encountered, complete it

macro_rules! rep_no_copy {
    ($e:expr; $n:expr) => {
        {
            let mut v = Vec::with_capacity($n);
            for _ in 0..$n {
                v.push($e);
            }
            v
        }
    };
}

use std::cmp::Ordering;
use std::mem;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

mod radix;
mod stack;

use radix::Radix;
use stack::Stack;

type PageID = usize;

const FANOUT: usize = 64;

#[derive(Clone, Debug, Ord, Eq, PartialEq)]
enum Bound {
    Inc(Vec<u8>),
    Non(Vec<u8>),
    Inf,
}

impl Bound {
    fn inner(&self) -> Option<Vec<u8>> {
        match self {
            &Bound::Inc(ref v) => Some(v.clone()),
            _ => None,
        }
    }
}

impl PartialOrd for Bound {
    fn partial_cmp(&self, other: &Bound) -> Option<Ordering> {
        use Bound::*;
        match *self {
            Inc(ref lhs) => {
                match *other {
                    Inf => Some(Ordering::Less),
                    Inc(ref rhs) => Some(lhs.cmp(rhs)),
                    Non(ref rhs) => {
                        if lhs < rhs {
                            Some(Ordering::Less)
                        } else {
                            Some(Ordering::Greater)
                        }
                    }
                }
            }
            Non(ref lhs) => {
                match *other {
                    Inf => Some(Ordering::Less),
                    Inc(ref rhs) => {
                        if lhs <= rhs {
                            Some(Ordering::Less)
                        } else {
                            Some(Ordering::Greater)
                        }
                    }
                    Non(ref rhs) => Some(lhs.cmp(&rhs)),
                }
            }
            Inf => {
                match *other {
                    Inf => Some(Ordering::Equal),
                    _ => Some(Ordering::Less),
                }
            }
        }
    }
}

struct Tree {
    pages: Radix<Node>,
    max_id: AtomicUsize,
    root: AtomicUsize,
}

#[derive(Clone, Debug)]
struct Node {
    id: PageID,
    data: Data,
    next: Option<PageID>,
    lo: Bound,
    hi: Bound,
}

impl Node {
    fn split_if_necessary(&self, parent: &Tree) -> Option<(Node, Node)> {
        match self.data {
            Data::Index(ref ptrs) => {
                if ptrs.len() > FANOUT {
                    let id = parent.new_id();
                    let (lhs, rhs) = ptrs.split_at(ptrs.len() / 2);
                    let split = rhs.first().unwrap().0.clone();
                    let left = Node {
                        id: self.id,
                        data: Data::Index(lhs.to_vec()),
                        next: Some(id),
                        lo: self.lo.clone(),
                        hi: Bound::Non(split.clone()),
                    };
                    let right = Node {
                        id: id,
                        data: Data::Index(rhs.to_vec()),
                        next: self.next.clone(),
                        lo: Bound::Inc(split.clone()),
                        hi: self.hi.clone(),
                    };
                    Some((left, right))
                } else {
                    None
                }
            }
            Data::Leaf(ref items) => {
                if items.len() > FANOUT {
                    let id = parent.new_id();
                    let (lhs, rhs) = items.split_at(items.len() / 2);
                    let split = rhs.first().unwrap().0.clone();
                    let left = Node {
                        id: self.id,
                        data: Data::Leaf(lhs.to_vec()),
                        next: Some(id),
                        lo: self.lo.clone(),
                        hi: Bound::Non(split.clone()),
                    };
                    let right = Node {
                        id: id,
                        data: Data::Leaf(rhs.to_vec()),
                        next: self.next.clone(),
                        lo: Bound::Inc(split.clone()),
                        hi: self.hi.clone(),
                    };
                    Some((left, right))
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
enum Data {
    Index(Vec<(Vec<u8>, PageID)>),
    Leaf(Vec<(Vec<u8>, Vec<u8>)>),
}

// general traversal hazards:
// encounter merged nodes

impl Tree {
    pub fn new() -> Tree {
        let root_id = 0;
        let leaf_id = 1;

        let leaf = Node {
            id: leaf_id,
            data: Data::Leaf(vec![]),
            next: None,
            lo: Bound::Inc(vec![]),
            hi: Bound::Inf,
        };
        let root = Node {
            id: root_id,
            data: Data::Index(vec![(vec![], leaf.id)]),
            next: None,
            lo: Bound::Inc(vec![]),
            hi: Bound::Inf,
        };
        let mut pages = Radix::default();
        pages.insert(root_id, Box::into_raw(Box::new(root))).unwrap();
        pages.insert(leaf_id, Box::into_raw(Box::new(leaf))).unwrap();
        Tree {
            pages: pages,
            max_id: AtomicUsize::new(leaf_id + 1),
            root: AtomicUsize::new(root_id),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut path = self.path_for_k(&*key);
        let node_ptr = self.pages.get(*path.last().unwrap()).unwrap();
        unsafe {
            match (*node_ptr).data {
                Data::Leaf(ref items) => {
                    let search = items.binary_search_by(|&(ref k, ref v)| (**k).cmp(key));
                    if let Ok(idx) = search {
                        return Some(items[idx].1.clone());
                    }
                }
                _ => panic!("last node in path is not leaf"),
            }
        }
        None
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) {
        // 1. traverse indexes & insert at leaf
        let mut path = self.path_for_k(&*key);
        let mut needs_split = false;
        let node_ptr = self.pages.get(*path.last().unwrap()).unwrap();
        unsafe {
            match (*node_ptr).data {
                Data::Leaf(ref mut items) => {
                    // println!("comparing leaf!");
                    let mut idx = 0;
                    for i in 0..items.len() {
                        if items[i].0 < key {
                            idx = i + 1;
                        }
                    }
                    items.insert(idx, (key.clone(), value.clone()));
                    if items.len() > FANOUT {
                        // println!("marking leaf node as in need of split");
                        needs_split = true;
                    }
                }
                _ => panic!("last node in path is not leaf"),
            }
        }

        if !needs_split {
            return;
        }

        // println!("needs split!");
        // to split, we pop the path, see if it's in need of split, recurse up
        // two-phase: (in prep for lock-free, not necessary for single threaded)
        //  1. half-split: install split on child, P
        //      a. allocate new right sibling page, Q
        //      b. locate split point
        //      c. create new consolidated pages for both sides
        //      d. add new node to pagetable
        //      e. append split delta to original page P with physical pointer to Q
        //  2. parent update: install new index term on parent
        //      a. append "index term delta record" to parent, containing:
        //          i. new bounds for P & Q
        //          ii. logical pointer to Q
        //
        //      (it's possible parent was merged in the mean-time, so if that's the
        //      case, we need to go up the path to the grandparent then down again
        //      or higher until it works)
        while let Some(cursor) = path.pop() {
            let node_ptr = self.pages.get(cursor).unwrap();

            unsafe {
                let ref mut node = *node_ptr;

                if let Some((mut lhs, rhs)) = node.split_if_necessary(self) {
                    // println!("processing split at {}, {:?}", cursor, path);
                    if let Some(parent_id) = path.last() {
                        // println!("pushing new node onto parent {}", parent_id);
                        let parent_ptr = self.pages.get(*parent_id).unwrap();
                        let ref mut parent = *parent_ptr;
                        if let Data::Index(ref mut ptrs) = parent.data {
                            let search = ptrs.binary_search_by(|&(ref k, ref v)| {
                                Bound::Inc(k.clone()).cmp(&lhs.lo)
                            });
                            if let Ok(idx) = search {
                                ptrs.remove(idx);
                            }
                            ptrs.push((lhs.lo.inner().unwrap(), lhs.id));
                            ptrs.push((rhs.lo.inner().unwrap(), rhs.id));
                            ptrs.sort_by(|a, b| a.0.cmp(&b.0));
                        }
                    } else {
                        // need to hoist a new root
                        // println!("$#@$#%!$%#@$%#@% hoisting new root");
                        // assert_eq!(lhs.id, root_id);
                        let root_id = self.new_id();
                        let root = Node {
                            id: root_id.clone(),
                            data: Data::Index(vec![(vec![], lhs.id),
                                                   (rhs.lo.inner().unwrap(), rhs.id)]),
                            next: None,
                            lo: Bound::Inc(vec![]),
                            hi: Bound::Inf,
                        };
                        self.pages.insert(root_id, Box::into_raw(Box::new(root))).unwrap();
                        let cas = self.root
                            .compare_and_swap(lhs.id, root_id, SeqCst);
                    }
                    // TODO observe half-split logic here
                    // println!("inserting lhs");
                    self.pages
                        .cas(lhs.id, node_ptr, Box::into_raw(Box::new(lhs)))
                        .unwrap();
                    // println!("inserting rhs");
                    self.pages.insert(rhs.id, Box::into_raw(Box::new(rhs))).unwrap();
                } else {
                    // println!("none necessary");
                }
            }
        }
    }

    pub fn del(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut path = self.path_for_k(&*key);
        let mut needs_merge = false;
        let mut ret = None;
        let node_ptr = self.pages.get(*path.last().unwrap()).unwrap();
        unsafe {
            match (*node_ptr).data {
                Data::Leaf(ref mut items) => {
                    // println!("comparing leaf!");
                    let search = items.binary_search_by(|&(ref k, ref v)| (**k).cmp(key));
                    if let Ok(idx) = search {
                        ret = Some(items.remove(idx).1);
                        if items.len() < FANOUT / 2 {
                            needs_merge = true;
                        }
                    }
                }
                _ => panic!("last node in path is not leaf"),
            }
        }

        // print!("{} ", path.len());

        // 2. recursively merge if necessary
        if !needs_merge {
            return ret;
        }

        // println!("needs merge!");
        // TODO merge
        ret
    }

    fn new_id(&self) -> PageID {
        self.max_id.fetch_add(1, SeqCst)
    }

    fn path_for_k(&self, key: &[u8]) -> Vec<PageID> {
        let mut cursor = self.root.load(SeqCst);
        let mut path = vec![cursor];
        // println!("traversing, using key {:?}", key);
        while let Some(node_ptr) = self.pages.get(cursor) {
            let old_cursor = cursor.clone();
            unsafe {
                match (*node_ptr).data {
                    Data::Index(ref ptrs) => {
                        // println!("level: {} pointers: {:?}", path.len(), ptrs);
                        for &(ref sep_k, ref ptr) in ptrs {
                            if &**sep_k <= &*key {
                                cursor = *ptr;
                            } else {
                                if cursor == old_cursor {
                                    panic!("set failed to traverse");
                                }
                                break; // we've found our next cursor
                            }
                        }
                        path.push(cursor.clone());
                    }
                    Data::Leaf(_) => {
                        // println!("found leaf!");
                        break;
                    }
                }
            }
        }

        // print!("{} ", path.len());

        path
    }
}

#[test]
fn test_bounds() {
    use Bound::*;
    assert!(Inf == Inf);
    assert!(Non(vec![]) == Non(vec![]));
    assert!(Inc(vec![]) == Inc(vec![]));
    assert!(Inc(b"hi".to_vec()) == Inc(b"hi".to_vec()));
    assert!(Non(b"hi".to_vec()) == Non(b"hi".to_vec()));
    assert!(Inc(b"hi".to_vec()) > Non(b"hi".to_vec()));
    assert!(Inc(vec![]) < Inf);
    assert!(Non(vec![]) < Inf);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        #[inline(always)]
        fn kv(i: usize) -> (Vec<u8>, Vec<u8>) {
            let k: [u8; 8] = unsafe { mem::transmute(i) };
            (k.to_vec(), k.to_vec())
        }
        let mut tree = Tree::new();
        for i in 0..100000 {
            let (k, v) = kv(i);
            tree.set(k.to_vec(), v.to_vec());
        }
        for i in 0..100000 {
            let (k, v) = kv(i);
            assert_eq!(tree.get(&*k), Some(v));
        }
        for i in 0..100000 {
            let (k, v) = kv(i);
            tree.del(&*k);
        }
        for i in 0..100000 {
            let (k, v) = kv(i);
            assert_eq!(tree.get(&*k), None);
        }
    }
}
