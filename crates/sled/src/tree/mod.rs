use std::fmt::{self, Debug};
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use epoch::{Guard, Shared, pin};

use super::*;

mod bound;
mod data;
mod frag;
mod node;
mod iter;
mod materializer;

pub use self::bound::Bound;
pub use self::frag::{ChildSplit, Frag, ParentSplit};
pub use self::data::Data;
pub use self::node::Node;
pub use self::iter::Iter;
pub use self::materializer::BLinkMaterializer;

impl<'a> IntoIterator for &'a Tree {
    type Item = DbResult<(Vec<u8>, Vec<u8>), ()>;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

/// A flash-sympathetic persistent lock-free B+ tree
#[derive(Clone)]
pub struct Tree {
    pages: Arc<PageCache<BLinkMaterializer, Frag, Vec<(PageID, PageID)>>>,
    config: Config,
    root: Arc<AtomicUsize>,
}

unsafe impl Send for Tree {}
unsafe impl Sync for Tree {}

impl Tree {
    /// Load existing or create a new `Tree`.
    pub fn start(config: Config) -> DbResult<Tree, ()> {
        #[cfg(feature = "check_snapshot_integrity")]
        config.verify_snapshot::<BLinkMaterializer, Frag, Vec<(PageID, PageID)>>();

        let pages = PageCache::start(config.clone())?;

        let roots_opt = pages.recovered_state().clone().and_then(
            |mut roots: Vec<(PageID, PageID)>| if roots.is_empty() {
                None
            } else {
                let mut last = std::usize::MAX;
                let mut last_idx = std::usize::MAX;
                while !roots.is_empty() {
                    // find the root that links to the last one
                    for (i, &(root, prev_root)) in roots.iter().enumerate() {
                        if prev_root == last {
                            last = root;
                            last_idx = i;
                            break;
                        }
                        assert_ne!(
                            i + 1,
                            roots.len(),
                            "encountered gap in root chain"
                        );
                    }
                    roots.remove(last_idx);
                }
                assert_ne!(last, std::usize::MAX);
                Some(last)
            },
        );

        let root_id = if let Some(root_id) = roots_opt {
            debug!("recovered root {} while starting tree", root_id);
            root_id
        } else {
            let guard = pin();
            let root_id = pages.allocate(&guard)?;
            assert_eq!(
                root_id,
                0,
                "we expect that this is the first page ever allocated"
            );
            debug!("allocated pid {} for root of new tree", root_id);

            let leaf_id = pages.allocate(&guard)?;
            trace!("allocated pid {} for leaf in new", leaf_id);

            let leaf = Frag::Base(
                Node {
                    id: leaf_id,
                    data: Data::Leaf(vec![]),
                    next: None,
                    lo: Bound::Inc(vec![]),
                    hi: Bound::Inf,
                },
                None,
            );

            let mut root_index_vec = vec![];
            root_index_vec.push((vec![], leaf_id));

            let root = Frag::Base(
                Node {
                    id: root_id,
                    data: Data::Index(root_index_vec),
                    next: None,
                    lo: Bound::Inc(vec![]),
                    hi: Bound::Inf,
                },
                Some(std::usize::MAX),
            );

            pages
                .replace(root_id, Shared::null(), root, &guard)
                .map_err(|e| e.danger_cast())?;
            pages
                .replace(leaf_id, Shared::null(), leaf, &guard)
                .map_err(|e| e.danger_cast())?;
            root_id
        };

        Ok(Tree {
            pages: Arc::new(pages),
            config: config,
            root: Arc::new(AtomicUsize::new(root_id)),
        })
    }

    /// Flushes any pending IO buffers to disk to ensure durability.
    pub fn flush(&self) -> std::io::Result<()> {
        self.pages.flush()
    }

    /// Retrieve a value from the `Tree` if it exists.
    pub fn get(&self, key: &[u8]) -> DbResult<Option<Value>, ()> {
        let guard = pin();
        let (_, ret) = self.get_internal(key, &guard)?;
        Ok(ret)
    }

    /// Compare and swap. Capable of unique creation, conditional modification,
    /// or deletion. If old is None, this will only set the value if it doesn't
    /// exist yet. If new is None, will delete the value if old is correct.
    /// If both old and new are Some, will modify the value if old is correct.
    /// If Tree is read-only, will do nothing.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::ConfigBuilder;
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let t = sled::Tree::start(config);
    ///
    /// // unique creation
    /// assert_eq!(t.cas(vec![1], None, Some(vec![1])), Ok(()));
    /// assert_eq!(t.cas(vec![1], None, Some(vec![1])), Err(Some(vec![1])));
    ///
    /// // conditional modification
    /// assert_eq!(t.cas(vec![1], Some(vec![1]), Some(vec![2])), Ok(()));
    /// assert_eq!(t.cas(vec![1], Some(vec![1]), Some(vec![2])), Err(Some(vec![2])));
    ///
    /// // conditional deletion
    /// assert_eq!(t.cas(vec![1], Some(vec![2]), None), Ok(()));
    /// assert_eq!(t.get(&*vec![1]), None);
    ///
    /// // read-only tree
    /// let ro_config = ConfigBuilder::new().temporary(true).read_only(true).build();
    /// let t = sled::Tree::start(ro_config);
    /// assert_eq!(t.cas(vec![10], Some(vec![2]), None), Err(None));
    /// ```
    pub fn cas(
        &self,
        key: Key,
        old: Option<Value>,
        new: Option<Value>,
    ) -> DbResult<(), Option<Value>> {
        if self.config.get_read_only() {
            return Err(Error::CasFailed(None));
        }
        // we need to retry caps until old != cur, since just because
        // cap fails it doesn't mean our value was changed.
        let frag = new.map(|n| Frag::Set(key.clone(), n)).unwrap_or_else(|| {
            Frag::Del(key.clone())
        });
        let guard = pin();
        loop {
            let (mut path, cur) =
                self.get_internal(&*key, &guard).map_err(
                    |e| e.danger_cast(),
                )?;
            if old != cur {
                return Err(Error::CasFailed(cur));
            }

            let &mut (ref node, ref cas_key) = path.last_mut().unwrap();
            if self.pages
                .link(node.id, cas_key.clone(), frag.clone(), &guard)
                .is_ok()
            {
                return Ok(());
            }
            M.tree_looped();
        }
    }

    /// Set a key to a new value.
    pub fn set(&self, key: Key, value: Value) -> DbResult<(), ()> {
        if self.config.get_read_only() {
            return Err(Error::Unsupported(
                "the database is in read-only mode".to_owned(),
            ));
        }
        let frag = Frag::Set(key.clone(), value);
        let guard = pin();
        loop {
            let mut path = self.path_for_key(&*key, &guard)?;
            let (mut last_node, last_cas_key) = path.pop().unwrap();
            if let Ok(new_cas_key) = self.pages.link(
                last_node.id,
                last_cas_key,
                frag.clone(),
                &guard,
            )
            {
                last_node.apply(&frag);
                let should_split = last_node.should_split(self.fanout());
                path.push((last_node.clone(), new_cas_key));
                // success
                if should_split {
                    self.recursive_split(&path, &guard)?;
                }
                return Ok(());
            }
            M.tree_looped();
        }
    }

    /// Delete a value, returning the last result if it existed.
    ///
    /// # Examples
    ///
    /// ```
    /// let config = sled::ConfigBuilder::new().temporary(true).build();
    /// let t = sled::Tree::start(config);
    /// t.set(vec![1], vec![1]);
    /// assert_eq!(t.del(&*vec![1]), Some(vec![1]));
    /// assert_eq!(t.del(&*vec![1]), None);
    /// ```
    pub fn del(&self, key: &[u8]) -> DbResult<Option<Value>, ()> {
        if self.config.get_read_only() {
            return Ok(None);
        }
        let guard = pin();
        let mut ret: Option<Value>;
        loop {
            let mut path = self.path_for_key(&*key, &guard)?;
            let (leaf_node, leaf_cas_key) = path.pop().unwrap();
            match leaf_node.data {
                Data::Leaf(ref items) => {
                    let search = items.binary_search_by(
                        |&(ref k, ref _v)| (**k).cmp(key),
                    );
                    if let Ok(idx) = search {
                        ret = Some(items[idx].1.clone());
                    } else {
                        ret = None;
                        break;
                    }
                }
                _ => panic!("last node in path is not leaf"),
            }

            let frag = Frag::Del(key.to_vec());
            if self.pages
                .link(leaf_node.id, leaf_cas_key, frag, &guard)
                .is_ok()
            {
                // success
                break;
            } else {
                // failure, retry
            }
            M.tree_looped();
        }
        Ok(ret)
    }

    /// Iterate over tuples of keys and values, starting at the provided key.
    ///
    /// # Examples
    ///
    /// ```
    /// let config = sled::ConfigBuilder::new().temporary(true).build();
    /// let t = sled::Tree::start(config);
    /// t.set(vec![1], vec![10]);
    /// t.set(vec![2], vec![20]);
    /// t.set(vec![3], vec![30]);
    /// let mut iter = t.scan(&*vec![2]);
    /// assert_eq!(iter.next(), Some((vec![2], vec![20])));
    /// assert_eq!(iter.next(), Some((vec![3], vec![30])));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn scan(&self, key: &[u8]) -> Iter {
        let guard = pin();
        let mut broken = None;
        let id = match self.get_internal(key, &guard) {
            Ok((path, _)) => {
                if path.is_empty() {
                    broken = Some(Error::ReportableBug(
                        "failed to get path for key".to_owned(),
                    ));
                    0
                } else {
                    let &(ref last_node, ref _last_cas_key) = path.last()
                        .unwrap();
                    last_node.id
                }
            }
            Err(e) => {
                broken = Some(e.danger_cast());
                0
            }
        };
        Iter {
            id: id,
            inner: &self.pages,
            last_key: Bound::Non(key.to_vec()),
            broken: broken,
            done: false,
        }
    }

    /// Iterate over the tuples of keys and values in this tree.
    ///
    /// # Examples
    ///
    /// ```
    /// let config = sled::ConfigBuilder::new().temporary(true).build();
    /// let t = sled::Tree::start(config);
    /// t.set(vec![1], vec![10]);
    /// t.set(vec![2], vec![20]);
    /// t.set(vec![3], vec![30]);
    /// let mut iter = t.iter();
    /// assert_eq!(iter.next(), Some((vec![1], vec![10])));
    /// assert_eq!(iter.next(), Some((vec![2], vec![20])));
    /// assert_eq!(iter.next(), Some((vec![3], vec![30])));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn iter(&self) -> Iter {
        self.scan(b"")
    }

    fn recursive_split<'g>(
        &self,
        path: &[(Node, TreePtr<'g>)],
        guard: &'g Guard,
    ) -> DbResult<(), ()> {
        // to split, we pop the path, see if it's in need of split, recurse up
        // two-phase: (in prep for lock-free, not necessary for single threaded)
        //  1. half-split: install split on child, P
        //      a. allocate new right sibling page, Q
        //      b. locate split point
        //      c. create new consolidated pages for both sides
        //      d. add new node to pagetable
        //      e. merge split delta to original page P with physical pointer to Q
        //      f. if failed, free the new page
        //  2. parent update: install new index term on parent
        //      a. merge "index term delta record" to parent, containing:
        //          i. new bounds for P & Q
        //          ii. logical pointer to Q
        //
        //      (it's possible parent was merged in the mean-time, so if that's the
        //      case, we need to go up the path to the grandparent then down again
        //      or higher until it works)
        //  3. any traversing nodes that witness #1 but not #2 try to complete it
        //
        //  root is special case, where we need to hoist a new root

        let mut all_page_views = path.to_vec();
        let mut root_and_key = all_page_views.remove(0);

        while let Some((node, cas_key)) = all_page_views.pop() {
            if node.should_split(self.fanout()) {
                // try to child split
                if let Ok(parent_split) = self.child_split(
                    &node,
                    cas_key,
                    guard,
                )
                {
                    // now try to parent split
                    let &mut (ref mut parent_node, ref mut parent_cas_key) =
                        all_page_views.last_mut().unwrap_or(&mut root_and_key);

                    let res = self.parent_split(
                        parent_node.clone(),
                        parent_cas_key.clone(),
                        parent_split.clone(),
                        guard,
                    );

                    if let Ok(res) = res {
                        parent_node.apply(&Frag::ParentSplit(parent_split));
                        *parent_cas_key = res;
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            }
        }

        let (root_node, root_cas_key) = root_and_key;

        if root_node.should_split(self.fanout()) {
            if let Ok(parent_split) = self.child_split(
                &root_node,
                root_cas_key,
                guard,
            )
            {
                return self.root_hoist(
                    root_node.id,
                    parent_split.to,
                    parent_split.at.inner().unwrap(),
                    guard,
                ).map(|_| ())
                    .map_err(|e| e.danger_cast());
            }
        }
        Ok(())
    }

    fn child_split<'g>(
        &self,
        node: &Node,
        node_cas_key: TreePtr<'g>,
        guard: &'g Guard,
    ) -> DbResult<ParentSplit, ()> {
        let new_pid = self.pages.allocate(guard)?;
        trace!("allocated pid {} in child_split", new_pid);

        // split the node in half
        let rhs = node.split(new_pid);

        let child_split = Frag::ChildSplit(ChildSplit {
            at: rhs.lo.clone(),
            to: new_pid,
        });

        let parent_split = ParentSplit {
            at: rhs.lo.clone(),
            to: new_pid,
        };

        // install the new right side
        self.pages
            .replace(new_pid, Shared::null(), Frag::Base(rhs, None), guard)
            .expect("failed to initialize child split");

        // try to install a child split on the left side
        if self.pages
            .link(node.id, node_cas_key, child_split, guard)
            .is_err()
        {
            // if we failed, don't follow through with the parent split
            self.pages.free(new_pid, guard).map_err(|e| e.danger_cast())?;
            return Err(Error::CasFailed(()));
        }

        Ok(parent_split)
    }

    fn parent_split<'g>(
        &self,
        parent_node: Node,
        parent_cas_key: TreePtr<'g>,
        parent_split: ParentSplit,
        guard: &'g Guard,
    ) -> DbResult<TreePtr<'g>, Option<TreePtr<'g>>> {
        // install parent split
        let res = self.pages.link(
            parent_node.id,
            parent_cas_key,
            Frag::ParentSplit(parent_split.clone()),
            guard,
        );

        res
    }

    fn root_hoist<'g>(
        &self,
        from: PageID,
        to: PageID,
        at: Key,
        guard: &'g Guard,
    ) -> DbResult<(), ()> {
        // hoist new root, pointing to lhs & rhs
        let new_root_pid = self.pages.allocate(guard)?;
        debug!("allocated pid {} in root_hoist", new_root_pid);

        let mut new_root_vec = vec![];
        new_root_vec.push((vec![], from));
        new_root_vec.push((at, to));
        let new_root = Frag::Base(
            Node {
                id: new_root_pid,
                data: Data::Index(new_root_vec),
                next: None,
                lo: Bound::Inc(vec![]),
                hi: Bound::Inf,
            },
            Some(from),
        );
        pagecache::debug_delay();
        let cas = self.root.compare_and_swap(from, new_root_pid, SeqCst);
        if cas == from {
            // TODO think about the racyness of this
            debug!("root hoist from {} to {} successful", from, new_root_pid);
            self.pages
                .replace(new_root_pid, Shared::null(), new_root, guard)
                .map(|_| ())
                .map_err(|e| e.danger_cast())
        } else {
            debug!("root hoist from {} to {} failed", from, new_root_pid);
            self.pages.free(new_root_pid, guard).map_err(
                |e| e.danger_cast(),
            )
        }
    }

    fn get_internal<'g>(
        &self,
        key: &[u8],
        guard: &'g Guard,
    ) -> DbResult<(Vec<(Node, TreePtr<'g>)>, Option<Value>), ()> {
        let path = self.path_for_key(&*key, guard)?;

        let ret = path.last().and_then(|&(ref last_node, ref _last_cas_key)| {
            let data = &last_node.data;
            let items = data.leaf_ref().unwrap();
            let search =
                items.binary_search_by(|&(ref k, ref _v)| (**k).cmp(key));
            if let Ok(idx) = search {
                // cap a del frag below
                Some(items[idx].1.clone())
            } else {
                // key does not exist
                None
            }
        });

        Ok((path, ret))
    }

    fn fanout(&self) -> usize {
        self.config.get_blink_fanout()
    }

    #[doc(hidden)]
    pub fn key_debug_str(&self, key: &[u8]) -> String {
        let guard = pin();
        let path = self.path_for_key(key, &guard).unwrap();
        let mut ret = String::new();
        for &(ref node, _) in &path {
            ret.push_str(&*format!("\n{:?}", node));
        }
        ret
    }

    /// returns the traversal path, completing any observed
    /// partially complete splits or merges along the way.
    fn path_for_key<'g>(
        &self,
        key: &[u8],
        guard: &'g Guard,
    ) -> DbResult<Vec<(Node, TreePtr<'g>)>, ()> {
        let key_bound = Bound::Inc(key.into());
        let mut cursor = self.root.load(SeqCst);
        let mut path: Vec<(Node, TreePtr<'g>)> = vec![];

        // unsplit_parent is used for tracking need
        // to complete partial splits.
        let mut unsplit_parent: Option<usize> = None;

        let mut not_found_loops = 0;
        loop {
            let get_cursor =
                self.pages.get(cursor, guard).map_err(|e| e.danger_cast())?;
            if get_cursor.is_free() || get_cursor.is_allocated() {
                // restart search from the tree's root
                not_found_loops += 1;
                debug_assert_ne!(
                    not_found_loops,
                    10_000,
                    "cannot find pid {} in path_for_key",
                    cursor
                );
                cursor = self.root.load(SeqCst);
                continue;
            }
            if !get_cursor.is_materialized() {
                error!("unwrapping {:?}", get_cursor);
            }

            let (frag, cas_key) = get_cursor.unwrap();
            let (node, _is_root) = frag.into_base().unwrap();

            // TODO this may need to change when handling (half) merges
            assert!(node.lo <= key_bound, "overshot key somehow");

            // half-complete split detect & completion
            if node.hi <= key_bound {
                // we have encountered a child split, without
                // having hit the parent split above.
                cursor = node.next.unwrap();
                if unsplit_parent.is_none() && !path.is_empty() {
                    unsplit_parent = Some(path.len() - 1);
                }
                continue;
            } else if let Some(idx) = unsplit_parent.take() {
                // we have found the proper page for
                // our split.
                let &(ref parent_node, ref parent_cas_key): &(Node, TreePtr<'g>) = &path[idx];

                let ps = Frag::ParentSplit(ParentSplit {
                    at: node.lo.clone(),
                    to: node.id,
                });

                let _res = self.pages.link(
                    parent_node.id,
                    parent_cas_key.clone(),
                    ps,
                    guard,
                );
            }

            path.push((node, cas_key));

            match path.last().unwrap().0.data {
                Data::Index(ref ptrs) => {
                    let old_cursor = cursor;
                    for &(ref sep_k, ref ptr) in ptrs {
                        if &**sep_k <= key {
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
                    break;
                }
            }
        }

        Ok(path)
    }
}

impl Debug for Tree {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let mut pid = self.root.load(SeqCst);
        let mut left_most = pid;
        let mut level = 0;

        f.write_str("Tree: \n\t").unwrap();
        self.pages.fmt(f).unwrap();
        f.write_str("\tlevel 0:\n").unwrap();

        let guard = pin();
        loop {
            let (frag, _cas_key) =
                self.pages.get(pid, &guard).unwrap().unwrap();
            let (node, _is_root) = frag.base().unwrap();

            f.write_str("\t\t").unwrap();
            node.fmt(f).unwrap();
            f.write_str("\n").unwrap();

            if let Some(next_pid) = node.next {
                pid = next_pid;
            } else {
                // we've traversed our level, time to bump down
                let (left_frag, _left_cas_key) =
                    self.pages.get(left_most, &guard).unwrap().unwrap();
                let (left_node, _is_root) = left_frag.base().unwrap();

                match left_node.data {
                    Data::Index(ptrs) => {
                        if let Some(&(ref _sep, ref next_pid)) = ptrs.first() {
                            pid = *next_pid;
                            left_most = *next_pid;
                            level += 1;
                            f.write_str(&*format!("\n\tlevel {}:\n", level))
                                .unwrap();
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
