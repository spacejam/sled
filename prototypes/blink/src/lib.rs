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
use std::ptr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

mod radix;
mod stack;

use radix::Radix;
use stack::Stack;

type PageID = usize;

const FANOUT: usize = 64;

#[inline(always)]
fn raw<T>(t: T) -> *const T {
    Box::into_raw(Box::new(t)) as *const T
}

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
    fn should_split(&self) -> bool {
        match self.data {
            Data::Index(ref ptrs) => {
                if ptrs.len() > FANOUT {
                    true
                } else {
                    false
                }
            }
            Data::Leaf(ref items) => {
                if items.len() > FANOUT {
                    true
                } else {
                    false
                }
            }
        }
    }

    fn split(&self, id: PageID) -> (Node, Node) {
        let (split, lhs, rhs) = match self.data {
            Data::Index(ref ptrs) => {
                let (lhs, rhs) = ptrs.split_at(ptrs.len() / 2);
                let split = rhs.first().unwrap().0.clone();
                (split, Data::Index(lhs.to_vec()), Data::Index(rhs.to_vec()))
            }
            Data::Leaf(ref items) => {
                let (lhs, rhs) = items.split_at(items.len() / 2);
                let split = rhs.first().unwrap().0.clone();
                (split, Data::Leaf(lhs.to_vec()), Data::Leaf(rhs.to_vec()))
            }
        };
        let left = Node {
            id: self.id,
            data: lhs,
            next: Some(id),
            lo: self.lo.clone(),
            hi: Bound::Non(split.clone()),
        };
        let right = Node {
            id: id,
            data: rhs,
            next: self.next.clone(),
            lo: Bound::Inc(split.clone()),
            hi: self.hi.clone(),
        };
        (left, right)
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
        pages.insert(root_id, raw(root)).unwrap();
        pages.insert(leaf_id, raw(leaf)).unwrap();
        Tree {
            pages: pages,
            max_id: AtomicUsize::new(2),
            root: AtomicUsize::new(root_id),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let (node_ptr, mut path) = self.path_for_k(&*key);
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
        loop {
            let (node_ptr, mut path) = self.path_for_k(&*key);
            let pid = *path.last().unwrap();
            unsafe {
                let ref node = *node_ptr;
                let mut node2 = node.clone();
                match node2.data {
                    Data::Leaf(ref mut items) => {
                        // println!("comparing leaf!");
                        let mut idx = 0;
                        for i in 0..items.len() {
                            if items[i].0 < key {
                                idx = i + 1;
                            }
                        }
                        items.insert(idx, (key.clone(), value.clone()));
                    }
                    _ => panic!("last node in path is not leaf"),
                }
                let node2_ptr = raw(node2);
                if Ok(node_ptr) == self.pages.cas(pid, node_ptr, node2_ptr) {
                    // success
                    self.recursive_split(&mut path);
                    break;
                } else {
                    // failure, retry
                    continue;
                }
            }
        }
    }

    fn recursive_split(&self, path: &mut Vec<PageID>) {
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
                let ref node = *node_ptr;

                if node.should_split() {
                    let new_id = self.new_id();
                    let (lhs, rhs) = node.split(new_id);
                    self.pages
                        .cas(lhs.id, node_ptr, raw(lhs.clone()))
                        .unwrap();
                    self.pages.insert(rhs.id, raw(rhs.clone())).unwrap();

                    // fix-up parent or hoist new root
                    if let Some(parent_id) = path.last() {
                        let parent_ptr = self.pages.get(*parent_id).unwrap();
                        let mut parent = (*parent_ptr).clone();
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
                        let parent_ptr2 = raw(parent);
                        if Ok(node_ptr) == self.pages.cas(*parent_id, parent_ptr, parent_ptr2) {
                            // success
                        } else {
                            // failure
                        }
                    } else {
                        // need to hoist a new root
                        // println!("$#@$#%!$%#@$%#@% hoisting new root");
                        let root_id = self.new_id();
                        let root = Node {
                            id: root_id.clone(),
                            data: Data::Index(vec![(vec![], lhs.id),
                                                   (rhs.lo.inner().unwrap(), rhs.id)]),
                            next: None,
                            lo: Bound::Inc(vec![]),
                            hi: Bound::Inf,
                        };
                        self.pages.insert(root_id, raw(root)).unwrap();
                        let cas = self.root
                            .compare_and_swap(lhs.id, root_id, SeqCst);
                    }
                } else {
                    return;
                }
            }
        }
    }

    pub fn del(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut ret = None;
        loop {
            let (node_ptr, path) = self.path_for_k(&*key);
            let pid = *path.last().unwrap();
            unsafe {
                let ref node = *node_ptr;
                let mut node2 = node.clone();
                match node2.data {
                    Data::Leaf(ref mut items) => {
                        // println!("comparing leaf!");
                        let search = items.binary_search_by(|&(ref k, ref v)| (**k).cmp(key));
                        if let Ok(idx) = search {
                            ret = Some(items.remove(idx).1);
                        }
                    }
                    _ => panic!("last node in path is not leaf"),
                }
                let node2_ptr = raw(node2);
                if Ok(node_ptr) == self.pages.cas(pid, node_ptr, node2_ptr) {
                    // success
                    break;
                } else {
                    // failure, retry
                    continue;
                }
            }
        }

        // print!("{} ", path.len());

        // TODO merge

        ret
    }

    fn new_id(&self) -> PageID {
        self.max_id.fetch_add(1, SeqCst)
    }

    fn path_for_k(&self, key: &[u8]) -> (*const Node, Vec<PageID>) {
        let mut cursor = self.root.load(SeqCst);
        let mut path = vec![cursor];
        let mut ptr = ptr::null();
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
                        ptr = node_ptr;
                        break;
                    }
                }
            }
        }

        // print!("{} ", path.len());

        (ptr, path)
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
