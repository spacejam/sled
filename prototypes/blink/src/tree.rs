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
        let root_id = pages.allocate();
        let leaf_id = pages.allocate();

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
        pages.insert(root_id, raw(root)).unwrap();
        pages.insert(leaf_id, raw(leaf)).unwrap();
        Tree {
            pages: pages,
            root: AtomicUsize::new(root_id),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Value> {
        let (_, ret) = self.get_internal(key);
        ret
    }

    pub fn cas(&self, key: Key, old: Option<Value>, new: Value) -> Result<(), Option<Value>> {
        let (path, cur) = self.get_internal(&*key);
        if old != cur {
            return Err(cur);
        }
        let frag = Frag::Set(key, new);
        let frag_ptr = raw(frag);

        let stack = path.last().unwrap();
        let pid = stack.pid;
        let head = stack.head;
        self.pages.cap(pid, head, frag_ptr).map_err(|_| cur)
    }

    fn get_internal(&self, key: &[u8]) -> (Vec<FragView>, Option<Value>) {
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
                        Data::Leaf(ref items) => {
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
        let frag = Frag::Set(key.clone(), value);
        let frag_ptr = raw(frag);
        loop {
            let (path, partial_seek) = self.path_for_k(&*key);
            let last = path.last().unwrap();
            let pid = last.pid;
            let head = last.head;
            if Ok(()) == self.pages.cap(pid, head, frag_ptr) {
                // success
                if last.stack_len() > FANOUT {
                    self.recursive_split(&path);
                }
                break;
            } else {
                // failure, retry
                continue;
            }
        }
    }

    // TODO tunable: pessimistic delete, vs just appending without knowing if it's there
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
                            Data::Leaf(ref items) => {
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

    fn consolidate(&self, pid: PageID) -> Node {
        // TODO
        unimplemented!()
    }

    fn recursive_split(&self, path: &Vec<FragView>) {
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
        //  3. any traversing nodes that witness #1 but not #2 try to complete it
        let mut frags = path.clone();
        frags.reverse();
        while let Some(frag_view) = frags.pop() {
            if frag_view.should_split() {
                let new_pid = self.pages.allocate();

                // returns new entire stacks for sides, frag for parent
                let (lhs, rhs, parent_split) = frag_view.split(new_pid);

                // install new rhs
                self.pages.inner.insert(new_pid, rhs).unwrap();

                // child split
                let cap = self.pages.inner.cas(frag_view.pid, frag_view.stack, lhs);

                if cap.is_err() {
                    // child split failed, don't retry
                    // TODO nuke GC
                    continue;
                }

                // TODO add frag_view.stack to GC

                // parent split or root hoist
                if let Some(parent_frag_view) = path.last() {
                    // install parent split
                    let raw_parent_split = raw(Frag::ParentSplit(parent_split));
                    let cap = unsafe {
                        (*parent_frag_view.stack).cap(parent_frag_view.head, raw_parent_split)
                    };

                    if cap.is_err() {
                        // TODO think how we should respond, maybe parent was merged/split
                        continue;
                    }
                } else {
                    // no parent, we just split the root, so install new one
                    // println!("$#@$#%!$%#@$%#@% hoisting new root");
                    let root_id = self.pages.allocate();
                    let root = Frag::Base(Node {
                        id: root_id.clone(),
                        data: Data::Index(vec![(vec![], parent_split.from),
                                               (parent_split.at, parent_split.to)]),
                        next: None,
                        lo: Bound::Inc(vec![]),
                        hi: Bound::Inf,
                    });
                    self.pages.insert(root_id, raw(root)).unwrap();
                    let cas = self.root
                        .compare_and_swap(parent_split.from, root_id, SeqCst);
                }
            }
        }
    }

    /// returns the traversal path,
    fn path_for_k(&self, key: &[u8]) -> (Vec<FragView>, PartialSeek) {
        use SeekRes::*;
        let mut cursor = self.root.load(SeqCst);
        let mut path = vec![];
        loop {
            let (res, meta) = self.pages.seek(cursor, key.to_vec());
            path.push(meta);
            match res {
                ShortCircuitSome(ref value) => {
                    return (path, PartialSeek::ShortCircuit(Some(value.clone())));
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
                                let last = path.last().cloned().unwrap();
                                if last.stack_len() > FANOUT {
                                    self.recursive_split(&path);
                                }
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
