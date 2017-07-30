use std::fmt::{self, Debug};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use super::*;

mod frag;
mod node;
mod data;

pub use self::frag::*;
pub use self::data::*;
pub use self::node::*;

pub const FANOUT: usize = 2;

type Raw = *const stack::Node<*const tree::Frag>;

pub struct Tree {
    pages: PageCache<LockFreeLog, BLinkMaterializer>,
    root: AtomicUsize,
}

unsafe impl Send for Tree {}
unsafe impl Sync for Tree {}

impl Tree {
    pub fn new(path: Option<String>) -> Tree {
        let mut pages = PageCache::new(BLinkMaterializer { last_known_root: 0 }, path);

        let root_opt = pages.recover(0);

        let root_id = if let Some(root_id) = root_opt {
            root_id
        } else {
            let (root_id, root_cas_key) = pages.allocate();
            let (leaf_id, leaf_cas_key) = pages.allocate();

            let leaf = Frag::Base(Node {
                id: leaf_id,
                data: Data::Leaf(vec![]),
                next: None,
                lo: Bound::Inc(vec![]),
                hi: Bound::Inf,
            });

            let mut root_index_vec = vec![];
            root_index_vec.push((vec![], leaf_id));

            let root = Frag::Base(Node {
                id: root_id,
                data: Data::Index(root_index_vec),
                next: None,
                lo: Bound::Inc(vec![]),
                hi: Bound::Inf,
            });

            pages.append(root_id, root_cas_key, root).unwrap();
            pages.append(leaf_id, leaf_cas_key, leaf).unwrap();

            root_id
        };

        Tree {
            pages: pages,
            root: AtomicUsize::new(root_id),
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
        loop {
            let (mut path, cur) = self.get_internal(&*key);
            if old != cur {
                return Err(cur);
            }

            let &mut (ref node, ref cas_key) = path.last_mut().unwrap();
            return self.pages.append(node.id, *cas_key, frag.clone()).map(|_| ()).map_err(|_| cur);
        }
    }

    fn get_internal(&self, key: &[u8]) -> (Vec<(Node, Raw)>, Option<Value>) {
        let path = self.path_for_key(&*key);
        let (last_node, _last_cas_key) = path.last().cloned().unwrap();
        match last_node.data.clone() {
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
        loop {
            let mut path = self.path_for_key(&*key);
            let (mut last_node, last_cas_key) = path.pop().unwrap();
            // println!("last before: {:?}", last);
            if let Ok(new_cas_key) = self.pages.append(last_node.id, last_cas_key, frag.clone()) {
                last_node.apply(frag);
                // println!("last after: {:?}", last);
                let should_split = last_node.should_split();
                path.push((last_node.clone(), new_cas_key));
                // success
                if should_split {
                    // println!("need to split {:?}", last_node.id);
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
            let (leaf_node, leaf_cas_key) = path.pop().unwrap();
            match leaf_node.data {
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
            if self.pages.append(leaf_node.id, leaf_cas_key, frag).is_ok() {
                // success
                break;
            } else {
                // failure, retry
                continue;
            }
        }

        ret
    }

    pub fn scan(&self, key: &[u8]) -> TreeIter {
        let (path, _) = self.get_internal(key);
        let (last_node, _last_cas_key) = path.last().cloned().unwrap();
        TreeIter {
            id: last_node.id,
            inner: &self.pages,
            last_key: Bound::Non(key.to_vec()),
        }
    }

    pub fn iter(&self) -> TreeIter {
        let (path, _) = self.get_internal(b"");
        let (last_node, _last_cas_key) = path.last().cloned().unwrap();
        TreeIter {
            id: last_node.id,
            inner: &self.pages,
            last_key: Bound::Non(vec![]),
        }
    }

    fn recursive_split(&self, path: &Vec<(Node, Raw)>) {
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

        let mut all_page_views = path.clone();
        let mut root_and_key = all_page_views.remove(0);

        // println!("before:\n{:?}\n", self);

        while let Some((node, cas_key)) = all_page_views.pop() {
            // println!("splitting node {:?}", node);
            if node.should_split() {
                // try to child split
                if let Ok(parent_split) = self.child_split(&node, cas_key) {
                    // now try to parent split
                    // TODO(GC) double check for leaks here

                    let &mut (ref mut parent_node, ref mut parent_cas_key) =
                        all_page_views.last_mut()
                            .unwrap_or(&mut root_and_key);

                    let res = self.parent_split(parent_node.clone(),
                                                parent_cas_key.clone(),
                                                parent_split.clone());
                    if res.is_err() {
                        continue;
                    }
                    parent_node.apply(Frag::ParentSplit(parent_split));
                    *parent_cas_key = res.unwrap();
                } else {
                    continue;
                }
            }
        }

        let (root_node, root_cas_key) = root_and_key;

        if root_node.should_split() {
            // println!("{}: hoisting root {}", name, root_frag.node.id);
            if let Ok(parent_split) = self.child_split(&root_node, root_cas_key) {
                self.root_hoist(root_node.id,
                                parent_split.to,
                                parent_split.at.inner().unwrap());
            }
        }
        // println!("after:\n{:?}\n", self);
    }

    fn child_split(&self, node: &Node, node_cas_key: Raw) -> Result<ParentSplit, ()> {
        let (new_pid, new_cas_key) = self.pages.allocate();

        // split the node in half
        let rhs = node.split(new_pid);

        let child_split = Frag::ChildSplit(ChildSplit {
            at: rhs.lo.clone(),
            to: new_pid,
        });

        let parent_split = ParentSplit {
            at: rhs.lo.clone(),
            to: new_pid.clone(),
        };

        // install the new right side
        self.pages.append(new_pid, new_cas_key, Frag::Base(rhs)).unwrap();

        // try to install a child split on the left side
        if let Err(_) = self.pages.append(node.id, node_cas_key, child_split) {
            // if we failed, don't follow through with the parent split
            // let this_thread = thread::current();
            // let name = this_thread.name().unwrap();
            // println!("{}: {}|{} @ {:?} -", name, node.id, new_pid, parent_split.at);
            self.pages.free(new_pid);
            return Err(());
        }

        Ok(parent_split)
    }

    fn parent_split(&self,
                    parent_node: Node,
                    parent_cas_key: Raw,
                    parent_split: ParentSplit)
                    -> Result<*const stack::Node<*const Frag>, *const stack::Node<*const Frag>> {

        // try to install a parent split on the index above

        // TODO(GC) double check for leaks here

        // install parent split
        let res = self.pages.append(parent_node.id,
                                    parent_cas_key,
                                    Frag::ParentSplit(parent_split.clone()));

        if res.is_err() {
            // let this_thread = thread::current();
            // let name = this_thread.name().unwrap();
            // println!("{}: {} <- {:?}|{} -", name, parent_node.id, parent_split.at, parent_split.to);
        }

        res
    }

    fn root_hoist(&self, from: PageID, to: PageID, at: Key) {
        // hoist new root, pointing to lhs & rhs
        let (new_root_pid, new_root_cas_key) = self.pages.allocate();
        let mut new_root_vec = vec![];
        new_root_vec.push((vec![], from));
        new_root_vec.push((at, to));
        let new_root = Frag::Base(Node {
            id: new_root_pid.clone(),
            data: Data::Index(new_root_vec),
            next: None,
            lo: Bound::Inc(vec![]),
            hi: Bound::Inf,
        });
        self.pages.append(new_root_pid, new_root_cas_key, new_root).unwrap();
        // println!("split is {:?}", parent_split);
        // println!("trying to cas root at {:?} with real value {:?}", path.first().unwrap().pid, self.root.load(SeqCst));
        // println!("root_id is {}", root_id);
        let cas = self.root
            .compare_and_swap(from, new_root_pid, SeqCst);
        if cas == from {
            // let this_thread = thread::current();
            // let name = this_thread.name().unwrap();
            // println!("{}: root hoist of {} +", name, from);
        } else {
            self.pages.free(new_root_pid);
            // println!("root hoist of {} -", from);
        }
    }

    pub fn key_debug_str(&self, key: &[u8]) -> String {
        let path = self.path_for_key(key);
        let mut ret = String::new();
        for (node, _) in path.into_iter() {
            ret.push_str(&*format!("\n{:?}", node));
        }
        ret
    }

    /// returns the traversal path, completing any observed
    /// partially complete splits or merges along the way.
    fn path_for_key(&self, key: &[u8]) -> Vec<(Node, Raw)> {
        let key_bound = Bound::Inc(key.into());
        let mut cursor = self.root.load(SeqCst);
        let mut path = vec![];

        // unsplit_parent is used for tracking need
        // to complete partial splits.
        let mut unsplit_parent: Option<(Node, Raw)> = None;

        loop {
            let (node, cas_key) = self.pages.get(cursor).unwrap();

            // TODO this may need to change when handling (half) merges
            assert!(node.lo <= key_bound, "overshot key somehow");

            // half-complete split detect & completion
            if node.hi <= key_bound {
                // println!("{:?} is hi, looking for {:?}", page_view.node.hi, key);
                // we have encountered a child split, without
                // having hit the parent split above.
                cursor = node.next.unwrap();
                if unsplit_parent.is_none() {
                    unsplit_parent = path.last().cloned();
                }
                continue;
            } else if let Some((parent_node, parent_cas_key)) = unsplit_parent.take() {
                // we have found the proper page for
                // our split.
                // println!("before: {:?}", self);

                let ps = Frag::ParentSplit(ParentSplit {
                    at: node.lo.clone(),
                    to: node.id.clone(),
                });

                let _res = self.pages.append(parent_node.id, parent_cas_key, ps);
                // println!("trying to fix incomplete parent split: {:?}", res);
                // println!("after: {:?}", self);
            }

            path.push((node.clone(), cas_key));

            match node.data {
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
                        panic!("stuck in page traversal loop");
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
            let (node, _cas_key) = self.pages.get(pid).unwrap();

            f.write_str("\t\t").unwrap();
            node.fmt(f).unwrap();
            f.write_str("\n").unwrap();

            if let Some(next_pid) = node.next {
                pid = next_pid;
            } else {
                // we've traversed our level, time to bump down
                let (left_node, _left_cas_key) = self.pages.get(left_most).unwrap();

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

pub struct TreeIter<'a> {
    id: PageID,
    inner: &'a PageCache<LockFreeLog, BLinkMaterializer>,
    last_key: Bound,
}

impl<'a> Iterator for TreeIter<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (page, _cas_key) = self.inner.get(self.id).unwrap();
            for (ref k, ref v) in page.data.leaf().unwrap() {
                if Bound::Inc(k.clone()) > self.last_key {
                    self.last_key = Bound::Inc(k.to_vec());
                    return Some((k.clone(), v.clone()));
                }
            }
            if page.next.is_none() {
                return None;
            }
            self.id = page.next.unwrap();
        }
    }
}

impl<'a> IntoIterator for &'a Tree {
    type Item = (Vec<u8>, Vec<u8>);
    type IntoIter = TreeIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct BLinkMaterializer {
    last_known_root: PageID,
}

impl Materializer for BLinkMaterializer {
    type MaterializedPage = Node;
    type PartialPage = Frag;
    type Recovery = PageID;

    fn materialize(&self, frags: Vec<Frag>) -> Node {
        let consolidated = self.consolidate(frags);
        let ref base = consolidated[0];
        match base {
            &Frag::Base(ref b) => b.clone(),
            _ => panic!("non-Base consolidated frags"),
        }
    }

    fn consolidate(&self, mut frags: Vec<Frag>) -> Vec<Frag> {
        let base_frag = frags.remove(0);
        let mut base = base_frag.base().unwrap();

        for frag in frags.into_iter() {
            base.apply(frag);
        }

        vec![Frag::Base(base)]
    }

    fn recover(&mut self, node: Node) -> Option<PageID> {
        // if last_known_root is a child of this node, this node is the new root
        if node.data.is_parent_of(self.last_known_root) {
            Some(node.id)
        } else {
            None
        }
    }
}
