use super::*;

impl<'a> IntoIterator for &'a Tree {
    type Item = (Vec<u8>, Vec<u8>);
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

/// A flash-sympathetic persistent lock-free B+ tree
pub struct Tree {
    pages: PageCache<BLinkMaterializer, Frag, PageID>,
    config: Config,
    root: AtomicUsize,
}

unsafe impl Send for Tree {}
unsafe impl Sync for Tree {}

impl Tree {
    /// Load existing or create a new `Tree`.
    pub fn new(config: Config) -> Tree {
        let mut pages = PageCache::new(
            BLinkMaterializer {
                roots: Mutex::new(vec![]),
            },
            config.clone(),
        );

        let root_opt = pages.recover();

        let root_id = if let Some(root_id) = root_opt {
            debug!("recovered root {} while starting tree", root_id);
            root_id
        } else {
            let (root_id, root_cas_key) = pages.allocate();
            debug!("allocated pid {} for root of new tree", root_id);

            let (leaf_id, leaf_cas_key) = pages.allocate();
            trace!("allocated pid {} for leaf in new", leaf_id);

            let leaf = Frag::Base(
                Node {
                    id: leaf_id,
                    data: Data::Leaf(vec![]),
                    next: None,
                    lo: Bound::Inc(vec![]),
                    hi: Bound::Inf,
                },
                false,
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
                true,
            );

            pages.replace(root_id, root_cas_key, root).unwrap();
            pages.replace(leaf_id, leaf_cas_key, leaf).unwrap();
            root_id
        };

        Tree {
            pages: pages,
            config: config,
            root: AtomicUsize::new(root_id),
        }
    }

    /// Retrieve a value from the `Tree` if it exists.
    pub fn get(&self, key: &[u8]) -> Option<Value> {
        let start = clock();
        let (_, ret) = self.get_internal(key);
        M.tree_get.measure(clock() - start);
        ret
    }

    /// Compare and swap. Capable of unique creation, conditional modification,
    /// or deletion. If old is None, this will only set the value if it doesn't
    /// exist yet. If new is None, will delete the value if old is correct.
    /// If both old and new are Some, will modify the value if old is correct.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::Config;
    /// let t = Config::default().tree();
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
    /// ```
    pub fn cas(
        &self,
        key: Key,
        old: Option<Value>,
        new: Option<Value>,
    ) -> Result<(), Option<Value>> {
        let start = clock();
        // we need to retry caps until old != cur, since just because
        // cap fails it doesn't mean our value was changed.
        let frag = new.map(|n| Frag::Set(key.clone(), n)).unwrap_or_else(|| {
            Frag::Del(key.clone())
        });
        loop {
            let (mut path, cur) = self.get_internal(&*key);
            if old != cur {
                M.tree_cas.measure(clock() - start);
                return Err(cur);
            }

            let &mut (ref node, ref cas_key) = path.last_mut().unwrap();
            if self.pages
                .link(node.id, cas_key.clone(), frag.clone())
                .is_ok()
            {
                M.tree_cas.measure(clock() - start);
                return Ok(());
            }
            M.tree_looped();
        }
    }

    /// Set a key to a new value.
    pub fn set(&self, key: Key, value: Value) {
        let start = clock();
        // println!("starting set of {:?} -> {:?}", key, value);
        let frag = Frag::Set(key.clone(), value);
        loop {
            let mut path = self.path_for_key(&*key);
            let (mut last_node, last_cas_key) = path.pop().unwrap();
            // println!("last before: {:?}", last);
            if let Ok(new_cas_key) = self.pages.link(
                last_node.id,
                last_cas_key,
                frag.clone(),
            )
            {
                last_node.apply(&frag);
                // println!("last after: {:?}", last);
                let should_split = last_node.should_split(self.fanout());
                path.push((last_node.clone(), new_cas_key));
                // success
                if should_split {
                    // println!("need to split {:?}", last_node.id);
                    self.recursive_split(&path);
                }
                M.tree_set.measure(clock() - start);
                return;
            }
            M.tree_looped();
        }
        // println!("done set of {:?}", key);
    }

    /// Delete a value, returning the last result if it existed.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::Config;
    /// let t = Config::default().tree();
    /// t.set(vec![1], vec![1]);
    /// assert_eq!(t.del(&*vec![1]), Some(vec![1]));
    /// assert_eq!(t.del(&*vec![1]), None);
    /// ```
    pub fn del(&self, key: &[u8]) -> Option<Value> {
        let start = clock();
        let mut ret: Option<Value>;
        loop {
            let mut path = self.path_for_key(&*key);
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
            if self.pages.link(leaf_node.id, leaf_cas_key, frag).is_ok() {
                // success
                break;
            } else {
                // failure, retry
            }
            M.tree_looped();
        }
        M.tree_del.measure(clock() - start);
        ret
    }

    /// Iterate over tuples of keys and values, starting at the provided key.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::Config;
    /// let t = Config::default().tree();
    /// t.set(vec![1], vec![10]);
    /// t.set(vec![2], vec![20]);
    /// t.set(vec![3], vec![30]);
    /// let mut iter = t.scan(&*vec![2]);
    /// assert_eq!(iter.next(), Some((vec![2], vec![20])));
    /// assert_eq!(iter.next(), Some((vec![3], vec![30])));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn scan(&self, key: &[u8]) -> Iter {
        let (path, _) = self.get_internal(key);
        let &(ref last_node, ref _last_cas_key) = path.last().unwrap();
        Iter {
            id: last_node.id,
            inner: &self.pages,
            last_key: Bound::Non(key.to_vec()),
        }
    }

    /// Iterate over the tuples of keys and values in this tree.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::Config;
    /// let t = Config::default().tree();
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
        let (path, _) = self.get_internal(b"");
        let &(ref last_node, ref _last_cas_key) = path.last().unwrap();
        Iter {
            id: last_node.id,
            inner: &self.pages,
            last_key: Bound::Non(vec![]),
        }
    }

    fn recursive_split(&self, path: &[(Node, CasKey<Frag>)]) {
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

        // println!("before:\n{:?}\n", self);

        while let Some((node, cas_key)) = all_page_views.pop() {
            // println!("splitting node {:?}", node);
            if node.should_split(self.fanout()) {
                // try to child split
                if let Ok(parent_split) = self.child_split(&node, cas_key) {
                    // now try to parent split
                    let &mut (ref mut parent_node, ref mut parent_cas_key) =
                        all_page_views.last_mut().unwrap_or(&mut root_and_key);

                    let res = self.parent_split(
                        parent_node.clone(),
                        parent_cas_key.clone(),
                        parent_split.clone(),
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
            // println!("{}: hoisting root {}", name, root_frag.node.id);
            if let Ok(parent_split) = self.child_split(
                &root_node,
                root_cas_key,
            )
            {
                self.root_hoist(
                    root_node.id,
                    parent_split.to,
                    parent_split.at.inner().unwrap(),
                );
            }
        }
        // println!("after:\n{:?}\n", self);
    }

    fn child_split(
        &self,
        node: &Node,
        node_cas_key: CasKey<Frag>,
    ) -> Result<ParentSplit, ()> {
        let (new_pid, new_cas_key) = self.pages.allocate();
        debug!("allocated pid {} in child_split", new_pid);

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
            .replace(new_pid, new_cas_key, Frag::Base(rhs, false))
            .expect("failed to initialize child split");

        // try to install a child split on the left side
        if self.pages.link(node.id, node_cas_key, child_split).is_err() {
            // if we failed, don't follow through with the parent split
            // println!("{}: {}|{} @ {:?} -", tn(), node.id, new_pid, parent_split.at);
            self.pages.free(new_pid);
            return Err(());
        }

        Ok(parent_split)
    }

    fn parent_split(
        &self,
        parent_node: Node,
        parent_cas_key: CasKey<Frag>,
        parent_split: ParentSplit,
    ) -> Result<CasKey<Frag>, Option<CasKey<Frag>>> {
        // install parent split
        let res = self.pages.link(
            parent_node.id,
            parent_cas_key,
            Frag::ParentSplit(parent_split.clone()),
        );

        if res.is_err() {
            // println!("{}: {} <- {:?}|{} -", tn(), parent_node.id, parent_split.at, parent_split.to);
        }

        res
    }

    fn root_hoist(&self, from: PageID, to: PageID, at: Key) {
        // hoist new root, pointing to lhs & rhs
        let (new_root_pid, new_root_cas_key) = self.pages.allocate();
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
            true,
        );
        // println!("split is {:?}", parent_split);
        // println!("trying to cas root at {:?} with real value {:?}", path.first().unwrap().pid, self.root.load(SeqCst));
        // println!("root_id is {}", root_id);
        debug_delay();
        let cas = self.root.compare_and_swap(from, new_root_pid, SeqCst);
        if cas == from {
            // TODO think about the racyness of this
            self.pages
                .replace(new_root_pid, new_root_cas_key, new_root)
                .unwrap();
            debug!(
                "{}: root hoist from {} to {} successful",
                tn(),
                from,
                new_root_pid
            );
        } else {
            debug!("root hoist from {} to {} failed", from, new_root_pid);
            self.pages.free(new_root_pid);
        }
    }

    fn get_internal(
        &self,
        key: &[u8],
    ) -> (Vec<(Node, CasKey<Frag>)>, Option<Value>) {
        let path = self.path_for_key(&*key);

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

        (path, ret)
    }

    fn fanout(&self) -> usize {
        self.config.get_blink_fanout()
    }

    #[doc(hidden)]
    pub fn key_debug_str(&self, key: &[u8]) -> String {
        let path = self.path_for_key(key);
        let mut ret = String::new();
        for &(ref node, _) in &path {
            ret.push_str(&*format!("\n{:?}", node));
        }
        ret
    }

    /// returns the traversal path, completing any observed
    /// partially complete splits or merges along the way.
    fn path_for_key(&self, key: &[u8]) -> Vec<(Node, CasKey<Frag>)> {
        let key_bound = Bound::Inc(key.into());
        let mut cursor = self.root.load(SeqCst);
        let mut path: Vec<(Node, CasKey<Frag>)> = vec![];

        // unsplit_parent is used for tracking need
        // to complete partial splits.
        let mut unsplit_parent: Option<usize> = None;

        let mut not_found_loops = 0;
        loop {
            let get_cursor = self.pages.get(cursor);
            if get_cursor.is_none() {
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
            let (frag, cas_key) = get_cursor.unwrap();
            let (node, _is_root) = frag.into_base().unwrap();

            // TODO this may need to change when handling (half) merges
            assert!(node.lo <= key_bound, "overshot key somehow");

            // half-complete split detect & completion
            if node.hi <= key_bound {
                // println!("{:?} is hi, looking for {:?}", page_view.node.hi, key);
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
                // println!("before: {:?}", self);
                let &(ref parent_node, ref parent_cas_key): &(Node, CasKey<Frag>) = &path[idx];

                let ps = Frag::ParentSplit(ParentSplit {
                    at: node.lo.clone(),
                    to: node.id,
                });

                let _res =
                    self.pages.link(parent_node.id, parent_cas_key.clone(), ps);
                // println!("trying to fix incomplete parent split: {:?}", res);
                // println!("after: {:?}", self);
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

        path
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

        loop {
            let (frag, _cas_key) = self.pages.get(pid).unwrap();
            let (node, _is_root) = frag.base().unwrap();

            f.write_str("\t\t").unwrap();
            node.fmt(f).unwrap();
            f.write_str("\n").unwrap();

            if let Some(next_pid) = node.next {
                pid = next_pid;
            } else {
                // we've traversed our level, time to bump down
                let (left_frag, _left_cas_key) =
                    self.pages.get(left_most).unwrap();
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
