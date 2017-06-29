use std::fmt::{self, Debug};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::thread;

use super::*;

const FANOUT: usize = 2;

#[derive(Clone)]
pub struct Tree {
    pages: Arc<Pages>,
    root: Arc<AtomicUsize>,
}

impl Tree {
    pub fn new() -> Tree {
        let pages = Pages::default();
        let root_id = pages.allocate();
        let leaf_id = pages.allocate();

        let leaf = Frag::Base(Node {
            id: leaf_id,
            data: Data::Leaf(Vec::with_capacity(FANOUT * 2)),
            next: None,
            lo: Bound::Inc(vec![]),
            hi: Bound::Inf,
        });

        let mut root_index_vec = Vec::with_capacity(FANOUT * 2);
        root_index_vec.push((vec![], leaf_id));
        let root = Frag::Base(Node {
            id: root_id,
            data: Data::Index(root_index_vec),
            next: None,
            lo: Bound::Inc(vec![]),
            hi: Bound::Inf,
        });

        pages.insert(root_id, root).unwrap();
        pages.insert(leaf_id, leaf).unwrap();
        Tree {
            pages: Arc::new(pages),
            root: Arc::new(AtomicUsize::new(root_id)),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Value> {
        // println!("starting get");
        let (_, ret) = self.get_internal(key);
        // println!("done get");
        ret
    }

    pub fn cas(&self, key: Key, old: Option<Value>, new: Value) -> Result<(), Option<Value>> {
        // we need to retry caps until old != cur, since just because
        // cap fails it doesn't mean our value was changed.
        let frag = Frag::Set(key.clone(), new);
        let frag_ptr = raw(frag);
        loop {
            let (mut path, cur) = self.get_internal(&*key);
            if old != cur {
                return Err(cur);
            }

            let stack = path.last_mut().unwrap();
            if let Ok(_) = stack.cap(frag_ptr).map(|_| ()).map_err(|_| cur) {
                return Ok(());
            }
        }
    }

    fn get_internal(&self, key: &[u8]) -> (Vec<Frags>, Option<Value>) {
        let path = self.path_for_key(&*key);
        match path.last().unwrap().node.data.clone() {
            Data::Leaf(ref items) => {
                // println!("comparing leaf! items: {:?}", items.len());
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

    pub fn set(&self, key: Key, value: Value) {
        // println!("starting set of {:?} -> {:?}", key, value);
        let frag = Frag::Set(key.clone(), value);
        let frag_ptr = raw(frag);
        loop {
            let mut path = self.path_for_key(&*key);
            let mut last = path.pop().unwrap();
            // println!("last before: {:?}", last);
            if let Ok(_) = last.cap(frag_ptr) {
                // println!("last after: {:?}", last);
                let should_split = last.should_split();
                path.push(last);
                // success
                if should_split {
                    // println!("need to split {:?}", pid);
                    self.recursive_split(&path);
                }
                break;
            } else {
                // failure, retry
                continue;
            }
        }
        // println!("done set of {:?}", key);
    }

    pub fn del(&self, key: &[u8]) -> Option<Value> {
        let mut ret: Option<Value>;
        loop {
            let mut path = self.path_for_key(&*key);
            let mut leaf_frags = path.pop().unwrap();
            match leaf_frags.node.data {
                Data::Leaf(ref items) => {
                    let search = items.binary_search_by(|&(ref k, ref _v)| (**k).cmp(key));
                    if let Ok(idx) = search {
                        ret = Some(items[idx].1.clone());
                    } else {
                        return None;
                    }
                }
                _ => panic!("last node in path is not leaf"),
            }

            let frag = Frag::Del(key.to_vec());
            let frag_ptr = raw(frag);
            if leaf_frags.cap(frag_ptr).is_ok() {
                // success
                break;
            } else {
                // failure, retry
                continue;
            }
        }

        ret
    }

    fn recursive_split(&self, path: &Vec<Frags>) {
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
        //
        //  root is special case, where we need to hoist a new root
        let mut all_frags = path.clone();
        let mut root_frag = all_frags.remove(0);

        let this_thread = thread::current();
        let name = this_thread.name().unwrap();
        while let Some(mut frags) = all_frags.pop() {
            let pid = frags.node.id;
            if frags.should_split() {
                let new_pid = self.pages.allocate();

                // returns new entire stacks for sides, frag for parent
                let (lhs, rhs, parent_split) = frags.split(new_pid);

                let raw_parent_split = raw(Frag::ParentSplit(parent_split.clone()));

                // install new rhs
                self.pages.insert(new_pid, rhs).unwrap();

                // child split
                let cap = frags.cas(lhs);

                if cap.is_err() {
                    // child split failed, don't retry
                    // TODO nuke GC
                    println!("{}: {}|{} @ {:?} -", name, pid, new_pid, parent_split.at);

                    self.pages.free(new_pid);
                    continue;
                }
                // println!("{}: {}|{} @ {:?} +", name, pid, new_pid, parent_split.at);

                // TODO add frags.stack to GC

                // parent split
                let mut parent_frags = all_frags.last_mut().unwrap_or(&mut root_frag);
                // println!("parent splitting node {:?}", parent_frags.pid);
                // install parent split
                let cap = parent_frags.cap(raw_parent_split);

                if cap.is_err() {
                    println!("{}: {} <- {}|{} -",
                             name,
                             parent_frags.node.id,
                             pid,
                             new_pid);
                    continue;
                }

                // println!("{}: {} <- {}|{} +", name, parent_frags.node.id, pid, new_pid);
                // println!("{:?}", self);
            }
        }

        if root_frag.should_split() {
            // println!("{}: hoisting root {}", name, root_frag.node.id);

            // split the root into 2 pieces

            let new_pid = self.pages.allocate();
            let (lhs, rhs, parent_split) = root_frag.split(new_pid);
            // println!("splitting root:\n\told: {:?}\n\tlhs: {:?}\n\trhs: {:?}", root_frag.node, lhs, rhs);
            self.pages.insert(new_pid, rhs).unwrap();

            // child split
            let cap = root_frag.cas(lhs);

            // println!("root child cap {:?} {:?} {:?}", root_frag.pid, root_frag.stack, lhs);
            if let Err(actual) = cap {
                // child split failed, don't retry
                // TODO nuke GC
                self.pages.free(new_pid);
                println!("{} root child split at {} failed, should have been {:?}",
                         name,
                         root_frag.node.id,
                         actual);
                return;
            }

            // hoist new root, pointing to lhs & rhs
            let new_root_pid = self.pages.allocate();
            let mut new_root_vec = Vec::with_capacity(FANOUT * 2);
            new_root_vec.push((vec![], root_frag.node.id));
            new_root_vec.push((parent_split.at.inner().unwrap(), parent_split.to));
            let root = Frag::Base(Node {
                id: new_root_pid.clone(),
                data: Data::Index(new_root_vec),
                next: None,
                lo: Bound::Inc(vec![]),
                hi: Bound::Inf,
            });
            self.pages.insert(new_root_pid, root).unwrap();
            // println!("split is {:?}", parent_split);
            // println!("trying to cas root at {:?} with real value {:?}", path.first().unwrap().pid, self.root.load(SeqCst));
            // println!("root_id is {}", root_id);
            let cas = self.root
                .compare_and_swap(path.first().unwrap().node.id, new_root_pid, SeqCst);
            if cas == path.first().unwrap().node.id {
                println!("{}: root hoist of {} +", name, root_frag.node.id);
                // TODO GC it
            } else {
                self.pages.free(new_root_pid);
                println!("root hoist of {} -", root_frag.node.id);
            }
        }
        // println!("after:\n{:?}\n", self);
    }

    pub fn key_debug_str(&self, key: &[u8]) -> String {
        let path = self.path_for_key(key);
        let mut ret = String::new();
        for frags in path.into_iter() {
            ret.push_str(&*format!("\n{:?}", frags.node));
        }
        ret
    }

    /// returns the traversal path, completing any observed
    /// partially complete splits or merges along the way.
    fn path_for_key(&self, key: &[u8]) -> Vec<Frags> {
        let key_bound = Bound::Inc(key.into());
        let mut cursor = self.root.load(SeqCst);
        let mut path = vec![];

        // unsplit_parent is used for tracking need
        // to complete partial splits.
        let mut unsplit_parent: Option<Frags> = None;

        loop {
            let frags = self.pages.get(cursor);

            // TODO this may need to change when handling (half) merges
            assert!(frags.node.lo <= key_bound, "overshot key somehow");

            // half-complete split detect & completion
            if frags.node.hi <= key_bound {
                // println!("{:?} is hi, looking for {:?}", frags.node.hi, key);
                // we have encountered a child split, without
                // having hit the parent split above.
                cursor = frags.node.next.unwrap();
                if unsplit_parent.is_none() {
                    unsplit_parent = path.last().cloned();
                }
                continue;
            } else if let Some(mut parent) = unsplit_parent.take() {
                // we have found the proper page for
                // our split.

                let ps = Frag::ParentSplit(ParentSplit {
                    at: frags.node.lo.clone(),
                    to: frags.node.id.clone(),
                });
                let res = parent.cap(raw(ps));
                println!("coopreative parent split: {:?}", res);
            }

            path.push(frags.clone());

            match frags.node.data {
                Data::Index(ref ptrs) => {
                    let old_cursor = cursor;
                    for &(ref sep_k, ref ptr) in ptrs {
                        if &**sep_k <= &*key {
                            cursor = *ptr;
                        } else {
                            break; // we've found our next cursor
                        }
                    }
                    if cursor == old_cursor {
                        println!("seps: {:?}", ptrs);
                        println!("looking for sep <= {:?}", &*key);
                        panic!("stuck in pid loop, didn't find proper key");
                    }
                }
                Data::Leaf(_) => {
                    return path;
                }
            }
        }
    }
}

impl Debug for Tree {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let mut pid = self.root.load(SeqCst);
        let mut left_most = pid.clone();
        let mut level = 0;

        f.write_str("Tree: \n\t").unwrap();
        self.pages.fmt(f).unwrap();
        f.write_str("\tlevel 0:\n").unwrap();

        loop {
            let view = self.pages.get(pid);
            let node = view.node;

            f.write_str("\t\t").unwrap();
            node.fmt(f).unwrap();
            f.write_str("\n").unwrap();

            if let Some(next_pid) = node.next {
                pid = next_pid;
            } else {
                // we've traversed our level, time to bump down
                let left_view = self.pages.get(left_most);
                let left_node = left_view.node;

                match left_node.data {
                    Data::Index(ptrs) => {
                        if let Some(&(ref _sep, ref next_pid)) = ptrs.first() {
                            pid = next_pid.clone();
                            left_most = next_pid.clone();
                            level += 1;
                            f.write_str(&*format!("\n\tlevel {}:\n", level)).unwrap();
                        } else {
                            panic!("trying to debug print empty index node");
                        }
                    }
                    Data::Leaf(_items) => {
                        // we've reached the end of our tree, all leafs are on
                        // the lowest level.
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum Data {
    Index(Vec<(Key, PageID)>),
    Leaf(Vec<(Key, Value)>),
}

impl Data {
    fn index_of(&self, pid: PageID) -> usize {
        match *self {
            Data::Index(ref ptrs) => ptrs.binary_search_by(|&(ref _k, ref p)| p.cmp(&pid)).unwrap(),
            Data::Leaf(_) => panic!("can't parent merge a leaf"),
        }
    }

    fn len(&self) -> usize {
        match *self {
            Data::Index(ref ptrs) => ptrs.len(),
            Data::Leaf(ref items) => items.len(),
        }
    }

    fn merge(&mut self, mut other: Data) {
        match *self {
            Data::Index(ref mut lptrs) => {
                match other {
                    Data::Index(ref mut rptrs) => lptrs.append(rptrs),
                    Data::Leaf(_) => panic!("can't merge an index with a leaf"),
                }
            }
            Data::Leaf(ref mut litems) => {
                match other {
                    Data::Index(_) => panic!("can't merge an index with a leaf"),
                    Data::Leaf(ref mut ritems) => litems.append(ritems),
                }
            }
        }
    }

    fn split(&self) -> (Key, Data, Data) {
        fn split_inner<T>(xs: &Vec<(Key, T)>) -> (Key, Vec<(Key, T)>, Vec<(Key, T)>)
            where T: Clone + Debug
        {
            let (lhs, rhs) = xs.split_at(xs.len() / 2 + 1);
            let split = rhs.first().unwrap().0.clone();

            // println!("split {:?} to {:?} and {:?}", xs, lhs, rhs);

            (split, lhs.to_vec(), rhs.to_vec())
        }
        match *self {
            Data::Index(ref ptrs) => {
                let (split, lhs, rhs) = split_inner(ptrs);
                (split, Data::Index(lhs), Data::Index(rhs))
            }
            Data::Leaf(ref items) => {
                let (split, lhs, rhs) = split_inner(items);

                (split, Data::Leaf(lhs), Data::Leaf(rhs))
            }
        }
    }
}


#[derive(Clone, Debug)]
pub struct Node {
    pub id: PageID,
    pub data: Data,
    pub next: Option<PageID>,
    pub lo: Bound,
    pub hi: Bound,
}

impl Node {
    pub fn set_leaf(&mut self, key: Key, val: Value) {
        if let Data::Leaf(ref mut records) = self.data {
            let search = records.binary_search_by(|&(ref k, ref _v)| (**k).cmp(&*key));
            if let Ok(idx) = search {
                records.push((key, val));
                records.swap_remove(idx);
            } else {
                records.push((key, val));
                records.sort();
            }
        } else {
            panic!("tried to Set a value to an index");
        }
    }

    pub fn parent_split(&mut self, ps: ParentSplit) {
        // println!("splitting parent: {:?}\nwith {:?}", self, ps);
        if let Data::Index(ref mut ptrs) = self.data {
            ptrs.push((ps.at.inner().unwrap(), ps.to));
            ptrs.sort_by(|a, b| a.0.cmp(&b.0));
        } else {
            panic!("tried to attach a ParentSplit to a Leaf chain");
        }
    }

    pub fn del_leaf(&mut self, key: &Key) {
        if let Data::Leaf(ref mut records) = self.data {
            let search = records.binary_search_by(|&(ref k, ref _v)| (**k).cmp(key));
            if let Ok(idx) = search {
                records.remove(idx);
            } else {
                print!(".");
            }
        } else {
            panic!("tried to attach a Del to an Index chain");
        }
    }

    pub fn should_split(&self) -> bool {
        if self.data.len() > FANOUT {
            true
        } else {
            false
        }
    }

    pub fn split(&self, id: PageID) -> (Node, Node) {
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
        // println!("split of {:?}\n\tlhs: {:?}\n\trhs: {:?}", self, left, right);
        (left, right)
    }

    pub fn left_merge(&mut self, lm: LeftMerge) {
        let iter = StackIter::from_ptr(lm.head);

        let (rhs, _) = iter.consolidated();

        assert_eq!(self.hi.inner(),
                   rhs.lo.inner(),
                   "tried to merge non-contiguous nodes");

        self.next = rhs.next;
        self.hi = rhs.hi;
        self.data.merge(rhs.data);
    }

    pub fn parent_merge(&mut self, pm: ParentMerge) {
        let lhs_idx = self.data.index_of(pm.lhs);
        let rhs_idx = self.data.index_of(pm.rhs);
        assert!(rhs_idx - lhs_idx == 1,
                "merge lhs and rhs are not adjacent in parent index");
    }
}
