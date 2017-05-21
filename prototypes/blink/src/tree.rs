use std::fmt::{self, Debug};
use std::mem;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::thread;

use super::*;

const FANOUT: usize = 16;
const MAX_FRAG_LEN: usize = 7;

// TODO
// * need to push consolidation CAS to stack,
//   because we can't linearize CAS ops b/w radix & stack
// * need to preserve splits during consolidation
// * llvm sanitizer may be best way to debug some of this

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
        let (mut path, cur) = self.get_internal(&*key);
        if old != cur {
            return Err(cur);
        }
        let frag = Frag::Set(key, new);
        let frag_ptr = raw(frag);

        let stack = path.last_mut().unwrap();
        stack.cap(frag_ptr).map(|_| ()).map_err(|_| cur)
    }

    fn get_internal(&self, key: &[u8]) -> (Vec<StackView>, Option<Value>) {
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
            }
        }
    }

    pub fn set(&self, key: Key, value: Value) {
        // println!("starting set of {:?} -> {:?}", key, value);
        let frag = Frag::Set(key.clone(), value);
        let frag_ptr = raw(frag);
        loop {
            let (mut path, partial_seek) = self.path_for_k(&*key);
            let mut last = path.pop().unwrap();
            if let Ok(new) = last.cap(frag_ptr) {
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
        // println!("done set");
    }

    // TODO tunable: pessimistic delete, vs just appending without knowing if it's there
    pub fn del(&self, key: &[u8]) -> Option<Value> {
        // println!("starting del");
        // try to get, if none, do nothing
        let mut ret = None;
        loop {
            let (mut path, partial_seek) = self.path_for_k(&*key);
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
            let mut stack = path.last_mut().unwrap();
            if stack.cap(frag_ptr).is_ok() {
                // success
                break;
            } else {
                // failure, retry
                continue;
            }
        }

        // println!("done del");

        ret
    }

    fn consolidate(&self, mut view: StackView) -> Result<(), ()> {
        let base = view.consolidate();
        // println!("page is now {:?}", base.data);
        let node = node_from_frag_vec(vec![Frag::Base(base)]);

        let cas = view.cas(node);
        if cas.is_ok() {
            // TODO GC old stack
            Ok(())
        } else {
            println!("failed to consolidate");
            Err(())
        }
    }

    fn recursive_split(&self, path: &Vec<StackView>) {
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

        // root is special case, where we need to hoist a new root
        let mut frags = path.clone();
        let mut root_frag = frags.remove(0);

        // print!("frags: ");
        // for frag in &frags {
        // print!("{:?} ", frag.pid);
        // }
        // println!("");

        // frags.reverse();
        let this_thread = thread::current();
        let name = this_thread.name().unwrap();
        while let Some(mut stack_view) = frags.pop() {
            let pid = stack_view.pid;
            if stack_view.should_split() {
                // print!("frags remaining: ");
                // for frag in &frags {
                // print!("{:?} ", frag.pid);
                // }
                // println!("");
                // println!("before split of {}:\n{:?}", stack_view.pid, self);

                let new_pid = self.pages.allocate();

                println!("{}: splitting {} to {} at {:?}",
                         name,
                         pid,
                         new_pid,
                         stack_view.head);

                // println!("allocated new id {}", new_pid);

                // returns new entire stacks for sides, frag for parent
                let (lhs, rhs, parent_split) = stack_view.split(new_pid);
                let raw_parent_split = raw(Frag::ParentSplit(parent_split));

                // install new rhs
                self.pages.insert(new_pid, rhs).unwrap();

                // child split
                let cap = stack_view.cas(lhs);

                if cap.is_err() {
                    // child split failed, don't retry
                    // TODO nuke GC
                    println!("{}: child split of {} -", name, pid);
                    self.pages.free(new_pid);
                    continue;
                }

                println!("{}: child split for {} +", name, pid);

                // TODO add stack_view.stack to GC

                // parent split
                let mut parent_stack_view = frags.last_mut().unwrap_or(&mut root_frag);
                println!("{} installing parent split for split of {} to parent {}",
                         name,
                         pid,
                         parent_stack_view.pid);
                // println!("parent splitting node {:?}", parent_stack_view.pid);
                // install parent split
                let cap = parent_stack_view.cap(raw_parent_split);

                println!("{:?}", self);
                if cap.is_err() {
                    println!("{}: parent split of {} -", name, pid);
                    // TODO think how we should respond, maybe parent was merged/split
                    continue;
                }

                println!("{}: parent split of {} +", name, pid);
            } else if stack_view.stack_len() > MAX_FRAG_LEN {
                let consolidated = node_from_frag_vec(vec![Frag::Base(stack_view.consolidate())]);
                let cap = stack_view.cas(consolidated);

                if cap.is_err() {
                    println!("{}: consolidation of {} -", name, pid);
                } else {
                    println!("{}: consolidation of {} +", name, pid);
                }
            }
        }

        if root_frag.should_split() {
            println!("{}: hoisting root {}", name, root_frag.pid);

            // split the root into 2 pieces

            let new_pid = self.pages.allocate();
            let (lhs, rhs, parent_split) = root_frag.split(new_pid);
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
                         root_frag.pid,
                         actual);
                return;
            }

            // hoist new root, pointing to lhs & rhs
            let new_root_pid = self.pages.allocate();
            let root = Frag::Base(Node {
                id: new_root_pid.clone(),
                data: Data::Index(vec![(vec![], parent_split.from),
                                       (parent_split.at.inner().unwrap(), parent_split.to)]),
                next: None,
                lo: Bound::Inc(vec![]),
                hi: Bound::Inf,
            });
            self.pages.insert(new_root_pid, root).unwrap();
            // println!("split is {:?}", parent_split);
            // println!("trying to cas root at {:?} with real value {:?}", path.first().unwrap().pid, self.root.load(SeqCst));
            // println!("root_id is {}", root_id);
            let cas = self.root
                .compare_and_swap(path.first().unwrap().pid, new_root_pid, SeqCst);
            if cas == path.first().unwrap().pid {
                println!("{}: root hoist of {} successful", name, root_frag.pid);
                // TODO GC it
            } else {
                self.pages.free(new_root_pid);
                // FIXME loops here
                panic!("cas failed to install new root for root {}", root_frag.pid);
                println!("{:?}", self);
            }
        }
        // println!("after:\n{:?}\n", self);
    }

    /// returns the traversal path,
    fn path_for_k(&self, key: &[u8]) -> (Vec<StackView>, PartialSeek) {
        use SeekRes::*;
        let mut cursor = self.root.load(SeqCst);
        let root = cursor;
        let mut path = vec![];
        loop {
            // println!("down at node {} from root {}, depth {}", cursor, root, path.len());
            let (res, stack_view) = self.pages.seek(cursor, key.to_vec());
            path.push(stack_view);
            match res {
                ShortCircuitSome(ref value) => {
                    return (path, PartialSeek::ShortCircuit(Some(value.clone())));
                }
                Node(ref node_ptr) => {
                    let old_cursor = cursor.clone();
                    unsafe {
                        match (**node_ptr).data {
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
                                return (path, PartialSeek::Base(*node_ptr));
                            }
                        }
                    }
                }
                ShortCircuitNone => {
                    return (path, PartialSeek::ShortCircuit(None));
                }
                Split(ref to) => {
                    // we pop here to simplify recursive split logic
                    path.pop();
                    cursor = *to;
                }
                Merge => {
                    unimplemented!();
                }
            }
        }
    }
}

impl Debug for Tree {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let simple_view = |pid: PageID| -> StackView {
            let stack = self.pages.inner.get(pid).unwrap();
            StackView {
                pid: pid.clone(),
                stack: stack,
                head: unsafe { (*stack).head() },
                depth: 0,
                fix_parent_split: false,
            }
        };

        let mut pid = self.root.load(SeqCst);
        let mut left_most = pid.clone();
        let mut level = 0;

        f.write_str("Tree: \n\t");
        self.pages.fmt(f);
        f.write_str("\tlevel 0:\n");

        let mut count = 0;
        loop {
            let view = simple_view(pid);
            let node = view.consolidate();

            count += 1;
            f.write_str("\t\t");
            node.fmt(f);
            f.write_str("\n");

            if let Some(next_pid) = node.next {
                pid = next_pid;
            } else {
                // we've traversed our level, time to bump down
                let left_view = simple_view(left_most);
                let left_node = left_view.consolidate();

                match left_node.data {
                    Data::Index(ptrs) => {
                        if let Some(&(ref sep, ref next_pid)) = ptrs.first() {
                            pid = next_pid.clone();
                            left_most = next_pid.clone();
                            level += 1;
                            // f.write_str(&*format!("\t\t{:?} pages", count));
                            // f.write_str(&*format!("\n\t\t{:?}", ptrs));
                            // f.write_str(&*format!("\n\tlevel {}:\n", level));
                        } else {
                            panic!("trying to debug print empty index node");
                        }
                    }
                    Data::Leaf(items) => {
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

enum PartialSeek {
    ShortCircuit(Option<Value>),
    Base(*const Node),
}

#[derive(Clone, Debug)]
pub enum Data {
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
            let (lhs, rhs) = xs.split_at(xs.len() / 2 + 1);
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
    pub id: PageID,
    pub data: Data,
    pub next: Option<PageID>,
    pub lo: Bound,
    pub hi: Bound,
}

impl Node {
    pub fn set_leaf(&mut self, key: Key, val: Value) -> Result<(), ()> {
        if let Data::Leaf(ref mut records) = self.data {
            let search = records.binary_search_by(|&(ref k, ref _v)| (**k).cmp(&*key));
            if let Ok(idx) = search {
                records.push((key, val));
                records.swap_remove(idx);
            } else {
                records.push((key, val));
                records.sort();
            }

            Ok(())
        } else {
            Err(())
        }
    }

    pub fn parent_split(&mut self,
                        at: Bound,
                        to: PageID,
                        from: PageID,
                        hi: Bound)
                        -> Result<(), ()> {
        // println!("parent: {:?}", self);
        if let Data::Index(ref mut ptrs) = self.data {
            let mut idx_opt = None;
            for (i, &(ref k, ref pid)) in ptrs.clone().iter().enumerate() {
                if *pid == from {
                    idx_opt = Some((k.clone(), i.clone()));
                    break;
                    // } else if Bound::Inc(k.clone()) <= at {
                    // idx_opt = Some((k.clone(), i.clone()));
                    // } else {
                    // break;
                    //
                }
            }
            if idx_opt.is_none() {
                println!("parent split not found at {:?} from {:?} to {:?})",
                         at,
                         from,
                         to);
                return Err(());
                // FIXME
                panic!("split point not found in parent");
            }
            let (orig_k, idx) = idx_opt.unwrap();

            ptrs.remove(idx);
            ptrs.push((orig_k.clone(), from));
            ptrs.push((at.inner().unwrap(), to));
            ptrs.sort_by(|a, b| a.0.cmp(&b.0));

            Ok(())
        } else {
            Err(())
        }
    }

    pub fn del_leaf(&mut self, key: &Key) -> Result<(), ()> {
        if let Data::Leaf(ref mut records) = self.data {
            let search = records.binary_search_by(|&(ref k, ref _v)| (**k).cmp(key));
            if let Ok(idx) = search {
                records.remove(idx);
            } else {
                print!(".");
            }
            Ok(())
        } else {
            Err(())
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
        // println!("self before split: {:?}", self);
        // println!("split: {:?}", split);
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
        // println!("index split\n\tlhs: {:?}\n\trhs: {:?}", left, right);
        (left, right)
    }
}
