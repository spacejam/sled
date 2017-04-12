use std::mem;
use std::ptr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use {raw, Bound, PageID, Radix};

const FANOUT: usize = 64;

pub struct Tree {
    pages: Radix<Node>,
    max_id: AtomicUsize,
    root: AtomicUsize,
}

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
        let pages = Radix::default();
        pages.insert(root_id, raw(root)).unwrap();
        pages.insert(leaf_id, raw(leaf)).unwrap();
        Tree {
            pages: pages,
            max_id: AtomicUsize::new(2),
            root: AtomicUsize::new(root_id),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let (node_ptr, _) = self.path_for_k(&*key);
        unsafe {
            match (*node_ptr).data {
                Data::Leaf(ref items) => {
                    let search = items.binary_search_by(|&(ref k, ref _v)| (**k).cmp(key));
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
                        let search = items.binary_search_by(|&(ref k, ref _v)| (**k).cmp(key));
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

#[derive(Clone, Debug)]
enum Data {
    Index(Vec<(Vec<u8>, PageID)>),
    Leaf(Vec<(Vec<u8>, Vec<u8>)>),
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
