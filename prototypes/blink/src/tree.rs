use std::mem;
use std::ptr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use super::*;

const FANOUT: usize = 3;

pub struct Tree {
    pages: Pages,
    root: AtomicUsize,
}

impl Tree {
    pub fn new() -> Tree {
        let pages = Pages::default();
        let (root_ptr, root_id) = pages.allocate();
        let (leaf_ptr, leaf_id) = pages.allocate();

        let leaf = Frag::Base(Node {
            id: leaf_id,
            data: Data::Leaf(vec![]),
            next: None,
            lo: Bound::Inc(vec![]),
            hi: Bound::Inf,
        });
        let root = Frag::Base(Node {
            id: root_id,
            data: Data::Index(vec![(vec![], leaf_id)]),
            next: None,
            lo: Bound::Inc(vec![]),
            hi: Bound::Inf,
        });
        pages.cap(root_id, root_ptr, raw(root)).unwrap();
        pages.cap(leaf_id, leaf_ptr, raw(leaf)).unwrap();
        Tree {
            pages: pages,
            root: AtomicUsize::new(root_id),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Value> {
        let (_, ret) = self.get_internal(key);
        ret
    }

    pub fn cas(&self, key: Key, old: Option<Value>, new: Value) -> Result((), Option<Value>> {
        let (path, cur) = self.get_internal(&*key);
        if old != cur {
            return Err(cur);
        }
        let frag = Frag::Set(key, new);
        let frag_ptr = raw(frag);

        let stack = path.last().unwrap();
        let pid = stack.pid;
        let head = stack.head;
        if Ok(()) == self.pages.cap(pid, head, frag_ptr) {
        }
    }

    fn get_internal(&self, key: &[u8]) -> (Vec<SeekMeta>, Option<Value>) {
        let (path, partial_seek) = self.path_for_k(&*key);
        use self::PartialSeek::*;
        match partial_seek {
            ShortCircuit(None) => {
                // we've encountered a deletion, so just return
                (path, None)
            }
            ShortCircuit(old) => {
                // try to cap it out with a del frag
                (path, old)
            }
            Base(node_ptr) => {
                // set old to it if it exists, return if not
                unsafe {
                    match (*node_ptr).data {
                        Data::Leaf(ref mut items) => {
                            // println!("comparing leaf!");
                            let search = items.binary_search_by(|&(ref k, ref _v)| (**k).cmp(key));
                            if let Ok(idx) = search {
                                // cap a del frag below
                                (path, Some(items[idx].1.clone()))
                            } else {
                                // key does not exist
                                (path, None)
                            }
                        }
                        _ => panic!("last node in path is not leaf"),
                    }
                }
            }
        }
    }

    pub fn set(&self, key: Key, value: Value) {
        let frag = Frag::Set(key, value);
        let frag_ptr = raw(frag);
        loop {
            let (path, partial_seek) = self.path_for_k(&*key);
            let stack = path.last().unwrap();
            let pid = stack.pid;
            let head = stack.head;
            if Ok(()) == self.pages.cap(pid, head, frag_ptr) {
                // success
                break;
            } else {
                // failure, retry
                continue;
            }
        }
    }

    pub fn del(&self, key: &[u8]) -> Option<Value> {
        // try to get, if none, do nothing
        let mut ret = None;
        loop {
            let (path, partial_seek) = self.path_for_k(&*key);
            use self::PartialSeek::*;
            match partial_seek {
                ShortCircuit(None) => {
                    // we've encountered a deletion, so just return
                    return None;
                }
                ShortCircuit(old) => {
                    // try to cap it out with a del frag
                    ret = old;
                }
                Base(node_ptr) => {
                    // set old to it if it exists, return if not
                    unsafe {
                        match (*node_ptr).data {
                            Data::Leaf(ref mut items) => {
                                // println!("comparing leaf!");
                                let search =
                                    items.binary_search_by(|&(ref k, ref _v)| (**k).cmp(key));
                                if let Ok(idx) = search {
                                    // cap a del frag below
                                    ret = Some(items[idx].1.clone());
                                } else {
                                    // key does not exist
                                    return None;
                                }
                            }
                            _ => panic!("last node in path is not leaf"),
                        }
                    }
                }
            }

            let frag = Frag::Del(key.to_vec());
            let frag_ptr = raw(frag);
            let stack = path.last().unwrap();
            let pid = stack.pid;
            let head = stack.head;
            if Ok(()) == self.pages.cap(pid, head, frag_ptr) {
                // success
                break;
            } else {
                // failure, retry
                continue;
            }
        }

        ret
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
        //      f. if failed, free the new page
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
                            let search = ptrs.binary_search_by(|&(ref k, ref _v)| {
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

    /// returns the traversal path,
    fn path_for_k(&self, key: &[u8]) -> (Vec<SeekMeta>, PartialSeek) {
        use SeekRes::*;
        let mut cursor = self.root.load(SeqCst);
        let mut path = vec![];
        loop {
            let (res, meta) = self.pages.seek(cursor, key.to_vec());
            path.push(meta);
            match res {
                ShortCircuitSome(ref value) => {
                    return (path, PartialSeek::ShortCircuit(Some(*value)));
                }
                Node(ref node_ptr) => {
                    let old_cursor = cursor.clone();
                    unsafe {
                        match (*node_ptr).data {
                            Data::Index(ref ptrs) => {
                                for &(ref sep_k, ref ptr) in ptrs {
                                    if &**sep_k <= &*key {
                                        cursor = *ptr;
                                    } else {
                                        break; // we've found our next cursor
                                    }
                                }
                                if cursor == old_cursor {
                                    panic!("stuck in pid loop");
                                }
                            }
                            Data::Leaf(_) => {
                                return (path, PartialSeek::Base(node_ptr));
                            }
                        }
                    }

                }
                ShortCircuitNone => {
                    return (path, PartialSeek::ShortCircuit(None));
                }
                Split(ref to) => {
                    cursor = *to;
                }
                Merge => {
                    unimplemented!();
                }
            }
        }
    }
}

enum PartialSeek {
    ShortCircuit(Option<Value>),
    Base(*const Node),
}

#[derive(Clone, Debug)]
enum Data {
    Index(Vec<(Key, PageID)>),
    Leaf(Vec<(Key, Value)>),
}

impl Data {
    fn len(&self) -> usize {
        match *self {
            Data::Index(ref ptrs) => ptrs.len(),
            Data::Leaf(ref items) => items.len(),
        }
    }

    fn split(&self) -> (Key, Data, Data) {
        fn split_inner<T>(xs: &Vec<(Key, T)>) -> (Key, &[(Key, T)], &[(Key, T)]) {
            let (lhs, rhs) = xs.split_at(xs.len() / 2);
            let split = rhs.first().unwrap().0.clone();
            (split, lhs, rhs)
        }
        match *self {
            Data::Index(ref ptrs) => {
                let (split, lhs, rhs) = split_inner(ptrs);
                (split, Data::Index(lhs.to_vec()), Data::Index(rhs.to_vec()))
            }
            Data::Leaf(ref items) => {
                let (split, lhs, rhs) = split_inner(items);
                (split, Data::Leaf(lhs.to_vec()), Data::Leaf(rhs.to_vec()))
            }
        }
    }
}


#[derive(Clone, Debug)]
pub struct Node {
    id: PageID,
    data: Data,
    next: Option<PageID>,
    lo: Bound,
    hi: Bound,
}

impl Node {
    fn should_split(&self) -> bool {
        if self.data.len() > FANOUT {
            true
        } else {
            false
        }
    }

    fn split(&self, id: PageID) -> (Node, Node) {
        let (split, lhs, rhs) = self.data.split();
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
