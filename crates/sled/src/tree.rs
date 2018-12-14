use std::{
    borrow::Cow,
    cmp::Ordering::{Greater, Less},
    fmt::{self, Debug},
    ops::{self, RangeBounds},
    sync::{
        atomic::{
            AtomicUsize,
            Ordering::{Acquire, Relaxed, Release, SeqCst},
        },
        Arc, Mutex,
    },
};

use pagecache::PagePtr;

use super::*;

const COUNTER_PID: PageId = 0;
const LEFT_LEAF_PID: PageId = 1;
const ORIGINAL_ROOT_PID: PageId = 2;

type Path<'g> = Vec<(&'g Frag, TreePtr<'g>)>;

impl<'a> IntoIterator for &'a Tree {
    type Item = Result<(Vec<u8>, PinnedValue), ()>;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

/// A flash-sympathetic persistent lock-free B+ tree
///
/// # Examples
///
/// ```
/// let t = sled::Tree::start_default("path_to_my_database").unwrap();
///
/// t.set(b"yo!", b"v1".to_vec());
/// assert!(t.get(b"yo!").unwrap().unwrap() == &*b"v1".to_vec());
///
/// t.cas(
///     b"yo!",                // key
///     Some(b"v1"),           // old value, None for not present
///     Some(b"v2".to_vec()),  // new value, None for delete
/// ).unwrap();
///
/// let mut iter = t.scan(b"a non-present key before yo!");
/// // assert_eq!(iter.next(), Some(Ok((b"yo!".to_vec(), b"v2".to_vec()))));
/// // assert_eq!(iter.next(), None);
///
/// t.del(b"yo!");
/// assert_eq!(t.get(b"yo!"), Ok(None));
/// ```
#[derive(Clone)]
pub struct Tree {
    pub(crate) pages:
        Arc<PageCache<BLinkMaterializer, Frag, Recovery>>,
    config: Config,
    root: Arc<AtomicUsize>,
    idgen: Arc<AtomicUsize>,
    idgen_persists: Arc<AtomicUsize>,
    idgen_persist_mu: Arc<Mutex<()>>,
    subscriptions: Arc<Subscriptions>,
}

unsafe impl Send for Tree {}
unsafe impl Sync for Tree {}

#[cfg(feature = "event_log")]
impl Drop for Tree {
    fn drop(&mut self) {
        self.config
            .event_log
            .tree_root_before_restart(self.root.load(SeqCst));
    }
}

impl Tree {
    /// Load existing or create a new `Tree` with a default configuration.
    pub fn start_default<P: AsRef<std::path::Path>>(
        path: P,
    ) -> Result<Tree, ()> {
        let config = ConfigBuilder::new().path(path).build();
        Self::start(config)
    }

    /// Load existing or create a new `Tree`.
    pub fn start(config: Config) -> Result<Tree, ()> {
        let _measure = Measure::new(&M.tree_start);

        #[cfg(any(test, feature = "check_snapshot_integrity"))]
        match config
            .verify_snapshot::<BLinkMaterializer, Frag, Recovery>()
        {
            Ok(_) => {}
            #[cfg(feature = "failpoints")]
            Err(Error::FailPoint) => {}
            other => panic!("failed to verify snapshot: {:?}", other),
        }

        let pages = PageCache::start(config.clone())?;

        let mut recovery: Recovery =
            pages.recovered_state().unwrap_or_default();
        let idgen_recovery =
            recovery.counter + (2 * config.idgen_persist_interval);
        let idgen_persists = recovery.counter
            / config.idgen_persist_interval
            * config.idgen_persist_interval;

        let roots_opt = if recovery.root_transitions.is_empty() {
            None
        } else {
            let mut last = std::usize::MAX;
            let mut last_idx = std::usize::MAX;
            while !recovery.root_transitions.is_empty() {
                // find the root that links to the last one
                for (i, &(root, prev_root)) in
                    recovery.root_transitions.iter().enumerate()
                {
                    if prev_root == last {
                        last = root;
                        last_idx = i;
                        break;
                    }
                    assert_ne!(
                        i + 1,
                        recovery.root_transitions.len(),
                        "encountered gap in root transitions: {:?}",
                        recovery.root_transitions
                    );
                }
                recovery.root_transitions.remove(last_idx);
            }
            assert_ne!(last, std::usize::MAX);
            Some(last)
        };

        let root_id = if let Some(root_id) = roots_opt {
            debug!("recovered root {} while starting tree", root_id);
            root_id
        } else {
            let guard = pin();

            // set up idgen
            let counter_id = pages.allocate(&guard)?;
            let counter = Frag::Counter(0);
            pages
                .replace(
                    counter_id,
                    PagePtr::allocated(),
                    counter,
                    &guard,
                )
                .map_err(|e| e.danger_cast())?;

            // set up empty leaf
            let leaf_id = pages.allocate(&guard)?;
            trace!("allocated pid {} for leaf in new", leaf_id);

            assert_eq!(
                leaf_id,
                LEFT_LEAF_PID,
                "we expect the first leaf to have pid {}, but it had pid {} instead",
                LEFT_LEAF_PID,
                leaf_id,
            );

            let leaf = Frag::Base(
                Node {
                    id: leaf_id,
                    data: Data::Leaf(vec![]),
                    next: None,
                    lo: Bound::Inclusive(vec![]),
                    hi: Bound::Inf,
                },
                None,
            );

            pages
                .replace(leaf_id, PagePtr::allocated(), leaf, &guard)
                .map_err(|e| e.danger_cast())?;

            // set up root index
            let root_id = pages.allocate(&guard)?;
            assert_eq!(
                root_id,
                ORIGINAL_ROOT_PID,
                "we expect that this is the third page ever allocated"
            );
            debug!("allocated pid {} for root of new tree", root_id);

            // vec![0] represents a prefix-encoded empty prefix
            let root_index_vec = vec![(vec![0], leaf_id)];

            let root = Frag::Base(
                Node {
                    id: root_id,
                    data: Data::Index(root_index_vec),
                    next: None,
                    lo: Bound::Inclusive(vec![]),
                    hi: Bound::Inf,
                },
                Some(std::usize::MAX),
            );

            pages
                .replace(root_id, PagePtr::allocated(), root, &guard)
                .map_err(|e| e.danger_cast())?;

            guard.flush();

            root_id
        };

        #[cfg(feature = "event_log")]
        config.event_log.tree_root_after_restart(root_id);

        Ok(Tree {
            pages: Arc::new(pages),
            config,
            root: Arc::new(AtomicUsize::new(root_id)),
            idgen: Arc::new(AtomicUsize::new(idgen_recovery)),
            idgen_persists: Arc::new(AtomicUsize::new(
                idgen_persists,
            )),
            idgen_persist_mu: Arc::new(Mutex::new(())),
            subscriptions: Arc::new(Subscriptions::default()),
        })
    }

    /// Subscribe to `Event`s that happen to keys that have
    /// the specified prefix. Events for particular keys are
    /// guaranteed to be witnessed in the same order by all
    /// threads, but threads may witness different interleavings
    /// of `Event`s across different keys. If subscribers don't
    /// keep up with new writes, they will cause new writes
    /// to block. There is a buffer of 1024 items per
    /// `Subscriber`. This can be used to build reactive
    /// and replicated systems.
    ///
    /// # Examples
    /// ```
    /// use sled::{Event, ConfigBuilder};
    /// let config = ConfigBuilder::new().temporary(true).build();
    ///
    /// let tree = sled::Tree::start(config).unwrap();
    ///
    /// // watch all events by subscribing to the empty prefix
    /// let mut events = tree.watch_prefix(vec![]);
    ///
    /// let tree_2 = tree.clone();
    /// let thread = std::thread::spawn(move || {
    ///     tree.set(vec![0], vec![1]).unwrap();
    /// });
    ///
    /// // events is a blocking `Iterator` over `Event`s
    /// for event in events.take(1) {
    ///     match event {
    ///         Event::Set(key, value) => assert_eq!(key, vec![0]),
    ///         Event::Merge(key, partial_value) => {}
    ///         Event::Del(key) => {}
    ///     }
    /// }
    ///
    /// thread.join().unwrap();
    /// ```
    pub fn watch_prefix(&self, prefix: Vec<u8>) -> Subscriber {
        self.subscriptions.register(prefix)
    }

    /// Generate a monotonic ID. Not guaranteed to be
    /// contiguous. Written to disk every `idgen_persist_interval`
    /// operations, followed by a blocking flush. During recovery, we
    /// take the last recovered generated ID and add 2x
    /// the `idgen_persist_interval` to it. While persisting, if the
    /// previous persisted counter wasn't synced to disk yet, we will do
    /// a blocking flush to fsync the latest counter, ensuring
    /// that we will never give out the same counter twice.
    pub fn generate_id(&self) -> Result<usize, ()> {
        let ret = self.idgen.fetch_add(1, Relaxed);

        let interval = self.config.idgen_persist_interval;
        let necessary_persists = ret / interval * interval;
        let mut persisted = self.idgen_persists.load(Acquire);

        while persisted < necessary_persists {
            let _mu = self.idgen_persist_mu.lock().unwrap();
            persisted = self.idgen_persists.load(Acquire);
            if persisted < necessary_persists {
                // it's our responsibility to persist up to our ID
                let guard = pin();
                let (current, key) = self
                    .pages
                    .get(COUNTER_PID, &guard)
                    .map_err(|e| e.danger_cast())?
                    .unwrap();

                if let Frag::Counter(current) = current {
                    assert_eq!(*current, persisted);
                } else {
                    panic!(
                        "counter pid contained non-Counter: {:?}",
                        current
                    );
                }

                let counter_frag = Frag::Counter(necessary_persists);

                let old = self
                    .idgen_persists
                    .swap(necessary_persists, Release);
                assert_eq!(old, persisted);

                self.pages
                    .replace(
                        COUNTER_PID,
                        key.clone(),
                        counter_frag,
                        &guard,
                    )
                    .map_err(|e| e.danger_cast())?;

                // during recovery we add 2x the interval. we only
                // need to block if the last one wasn't stable yet.
                if key.last_lsn() > self.pages.stable_lsn() {
                    self.pages.make_stable(key.last_lsn())?;
                }

                guard.flush();
            }
        }

        Ok(ret)
    }

    /// Clears the `Tree`, removing all values.
    ///
    /// Note that this is not atomic.
    pub fn clear(&self) -> Result<(), ()> {
        for k in self.keys(b"") {
            let key = k?;
            self.del(key)?;
        }
        Ok(())
    }

    /// Flushes all dirty IO buffers and calls fsync.
    /// If this succeeds, it is guaranteed that
    /// all previous writes will be recovered if
    /// the system crashes.
    pub fn flush(&self) -> Result<(), ()> {
        self.pages.flush()
    }

    /// Returns `true` if the `Tree` contains a value for
    /// the specified key.
    pub fn contains_key<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> Result<bool, ()> {
        self.get(key).map(|r| r.is_some())
    }

    /// Retrieve a value from the `Tree` if it exists.
    pub fn get<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> Result<Option<PinnedValue>, ()> {
        let _measure = Measure::new(&M.tree_get);

        let guard = pin();

        // the double guard is a hack that maintains
        // correctness of the ret value
        let double_guard = guard.clone();
        let (_, ret) = self.get_internal(key.as_ref(), &guard)?;

        guard.flush();

        Ok(ret.map(|r| PinnedValue::new(r, double_guard)))
    }

    /// Retrieve the key and value before the provided key,
    /// if one exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::{ConfigBuilder, Error};
    /// let config = ConfigBuilder::new().temporary(true).build();
    ///
    /// let tree = sled::Tree::start(config).unwrap();
    ///
    /// for i in 0..10 {
    ///     tree.set(vec![i], vec![i]).expect("should write successfully");
    /// }
    ///
    /// assert!(tree.get_lt(vec![]).unwrap().is_none());
    /// assert!(tree.get_lt(vec![0]).unwrap().is_none());
    /// assert!(tree.get_lt(vec![1]).unwrap().unwrap().1 == vec![0]);
    /// assert!(tree.get_lt(vec![9]).unwrap().unwrap().1 == vec![8]);
    /// assert!(tree.get_lt(vec![10]).unwrap().unwrap().1 == vec![9]);
    /// assert!(tree.get_lt(vec![255]).unwrap().unwrap().1 == vec![9]);
    /// ```
    pub fn get_lt<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> Result<Option<(Key, PinnedValue)>, ()> {
        let _measure = Measure::new(&M.tree_get);

        // the double guard is a hack that maintains
        // correctness of the ret value
        let guard = pin();
        let double_guard = guard.clone();

        let path = self.path_for_key(key.as_ref(), &guard)?;
        let (last_frag, _tree_ptr) = path
            .last()
            .expect("path should always contain a last element");

        let last_node = last_frag.unwrap_base();
        let data = &last_node.data;
        let items =
            data.leaf_ref().expect("last_node should be a leaf");
        let search = leaf_search(Less, items, |&(ref k, ref _v)| {
            prefix_cmp_encoded(k, key.as_ref(), last_node.lo.inner())
        });

        let ret = if search.is_none() {
            let mut iter = self.range((
                std::ops::Bound::Unbounded,
                std::ops::Bound::Excluded(key),
            ));

            match iter.next_back() {
                Some(Err(e)) => return Err(e),
                Some(Ok(pair)) => Some(pair),
                None => None,
            }
        } else {
            let idx = search.unwrap();
            let (encoded_key, v) = &items[idx];
            Some((
                prefix_decode(last_node.lo.inner(), &*encoded_key),
                PinnedValue::new(&*v, double_guard),
            ))
        };

        guard.flush();

        Ok(ret)
    }

    /// Retrieve the next key and value from the `Tree` after the
    /// provided key.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::{ConfigBuilder, Error};
    /// let config = ConfigBuilder::new().temporary(true).build();
    ///
    /// let tree = sled::Tree::start(config).unwrap();
    ///
    /// for i in 0..10 {
    ///     tree.set(vec![i], vec![i]).expect("should write successfully");
    /// }
    ///
    /// assert!(tree.get_gt(vec![]).unwrap().unwrap().1 == vec![0]);
    /// assert!(tree.get_gt(vec![0]).unwrap().unwrap().1 == vec![1]);
    /// assert!(tree.get_gt(vec![1]).unwrap().unwrap().1 == vec![2]);
    /// assert!(tree.get_gt(vec![8]).unwrap().unwrap().1 == vec![9]);
    /// assert!(tree.get_gt(vec![9]).unwrap().is_none());
    /// ```
    pub fn get_gt<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> Result<Option<(Key, PinnedValue)>, ()> {
        let _measure = Measure::new(&M.tree_get);

        // the double guard is a hack that maintains
        // correctness of the ret value
        let guard = pin();
        let double_guard = guard.clone();

        let path = self.path_for_key(key.as_ref(), &guard)?;
        let (last_frag, _tree_ptr) = path
            .last()
            .expect("path should always contain a last element");

        let last_node = last_frag.unwrap_base();
        let data = &last_node.data;
        let items =
            data.leaf_ref().expect("last_node should be a leaf");
        let search =
            leaf_search(Greater, items, |&(ref k, ref _v)| {
                prefix_cmp_encoded(
                    k,
                    key.as_ref(),
                    last_node.lo.inner(),
                )
            });

        let ret = if search.is_none() {
            let mut iter = self.range((
                std::ops::Bound::Excluded(key),
                std::ops::Bound::Unbounded,
            ));

            match iter.next() {
                Some(Err(e)) => return Err(e),
                Some(Ok(pair)) => Some(pair),
                None => None,
            }
        } else {
            let idx = search.unwrap();
            let (encoded_key, v) = &items[idx];
            Some((
                prefix_decode(last_node.lo.inner(), &*encoded_key),
                PinnedValue::new(&*v, double_guard),
            ))
        };

        guard.flush();

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
    /// use sled::{ConfigBuilder, Error};
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let t = sled::Tree::start(config).unwrap();
    ///
    /// // unique creation
    /// assert_eq!(t.cas(&[1], None, Some(vec![1])), Ok(()));
    /// // assert_eq!(t.cas(&[1], None, Some(vec![1])), Err(Error::CasFailed(Some(vec![1]))));
    ///
    /// // conditional modification
    /// assert_eq!(t.cas(&[1], Some(&*vec![1]), Some(vec![2])), Ok(()));
    /// // assert_eq!(t.cas(&[1], Some(vec![1]), Some(vec![2])), Err(Error::CasFailed(Some(vec![2]))));
    ///
    /// // conditional deletion
    /// assert_eq!(t.cas(&[1], Some(&[2]), None), Ok(()));
    /// assert_eq!(t.get(&[1]), Ok(None));
    /// ```
    pub fn cas<K: AsRef<[u8]>>(
        &self,
        key: K,
        old: Option<&[u8]>,
        new: Option<Value>,
    ) -> Result<(), Option<PinnedValue>> {
        let _measure = Measure::new(&M.tree_cas);

        if self.config.read_only {
            return Err(Error::CasFailed(None));
        }

        let guard = pin();

        // we need to retry caps until old != cur, since just because
        // cap fails it doesn't mean our value was changed.
        loop {
            let pin_guard = guard.clone();
            let (mut path, cur) = self
                .get_internal(key.as_ref(), &guard)
                .map_err(|e| e.danger_cast())?;

            if old.as_ref().map(|o| o.as_ref()) != cur.map(|v| &*v) {
                return Err(Error::CasFailed(
                    cur.map(|c| PinnedValue::new(c, pin_guard)),
                ));
            }

            let mut subscriber_reservation =
                self.subscriptions.reserve(&key);

            let (leaf_frag, leaf_ptr) = path.pop().expect(
                "get_internal somehow returned a path of length zero",
            );

            let (node_id, encoded_key) = {
                let node: &Node = leaf_frag.unwrap_base();
                (
                    node.id,
                    prefix_encode(node.lo.inner(), key.as_ref()),
                )
            };
            let frag = if let Some(ref n) = new {
                Frag::Set(encoded_key, n.clone())
            } else {
                Frag::Del(encoded_key)
            };
            let link = self.pages.link(
                node_id,
                leaf_ptr,
                frag.clone(),
                &guard,
            );
            match link {
                Ok(_) => {
                    if let Some(res) = subscriber_reservation.take() {
                        let event = if new.is_some() {
                            subscription::Event::Set(
                                key.as_ref().to_vec(),
                                new.unwrap().clone(),
                            )
                        } else {
                            subscription::Event::Del(
                                key.as_ref().to_vec(),
                            )
                        };

                        res.complete(event);
                    }

                    guard.flush();
                    return Ok(());
                }
                Err(Error::CasFailed(_)) => {}
                Err(other) => {
                    guard.flush();
                    return Err(other.danger_cast());
                }
            }
            M.tree_looped();
        }
    }

    /// Set a key to a new value, returning the old value if it
    /// was set.
    pub fn set<K: AsRef<[u8]>>(
        &self,
        key: K,
        value: Value,
    ) -> Result<Option<PinnedValue>, ()> {
        let _measure = Measure::new(&M.tree_set);

        if self.config.read_only {
            return Err(Error::Unsupported(
                "the database is in read-only mode".to_owned(),
            ));
        }

        let guard = pin();

        loop {
            let double_guard = guard.clone();
            let (mut path, existing_key) =
                self.get_internal(key.as_ref(), &guard)?;
            let (leaf_frag, leaf_ptr) = path.pop().expect(
                "path_for_key should always return a path \
                 of length >= 2 (root + leaf)",
            );
            let node: &Node = leaf_frag.unwrap_base();
            let encoded_key =
                prefix_encode(node.lo.inner(), key.as_ref());

            let mut subscriber_reservation =
                self.subscriptions.reserve(&key);

            let frag = Frag::Set(encoded_key, value.clone());
            let link = self.pages.link(
                node.id,
                leaf_ptr.clone(),
                frag.clone(),
                &guard,
            );
            match link {
                Ok(new_cas_key) => {
                    // success
                    if let Some(res) = subscriber_reservation.take() {
                        let event = subscription::Event::Set(
                            key.as_ref().to_vec(),
                            value.clone(),
                        );

                        res.complete(event);
                    }

                    if node.should_split(
                        self.config.blink_node_split_size as u64,
                    ) {
                        let mut path2 = path
                            .iter()
                            .map(|&(f, ref p)| {
                                (Cow::Borrowed(f), p.clone())
                            })
                            .collect::<Vec<(Cow<'_, Frag>, _)>>();
                        let mut node2 = node.clone();
                        node2
                            .apply(&frag, self.config.merge_operator);
                        let frag2 =
                            Cow::Owned(Frag::Base(node2, None));
                        path2.push((frag2, new_cas_key));
                        self.recursive_split(path2, &guard)?;
                    }

                    guard.flush();

                    return Ok(existing_key.map(move |r| {
                        PinnedValue::new(r, double_guard)
                    }));
                }
                Err(Error::CasFailed(_)) => {}
                Err(other) => {
                    guard.flush();

                    return Err(other.danger_cast());
                }
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
    /// let t = sled::Tree::start(config).unwrap();
    /// t.set(&[1], vec![1]);
    /// assert_eq!(t.del(&*vec![1]).unwrap().unwrap(), vec![1]);
    /// assert_eq!(t.del(&*vec![1]), Ok(None));
    /// ```
    pub fn del<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> Result<Option<PinnedValue>, ()> {
        let _measure = Measure::new(&M.tree_del);

        if self.config.read_only {
            return Ok(None);
        }

        let guard = pin();
        let double_guard = guard.clone();

        loop {
            let (mut path, existing_key) =
                self.get_internal(key.as_ref(), &guard)?;

            let mut subscriber_reservation =
                self.subscriptions.reserve(&key);

            let (leaf_frag, leaf_ptr) = path.pop().expect(
                "path_for_key should always return a path \
                 of length >= 2 (root + leaf)",
            );
            let node: &Node = leaf_frag.unwrap_base();
            let encoded_key =
                prefix_encode(node.lo.inner(), key.as_ref());

            let frag = Frag::Del(encoded_key);
            let link = self.pages.link(
                node.id,
                leaf_ptr.clone(),
                frag,
                &guard,
            );

            match link {
                Ok(_) => {
                    // success
                    if let Some(res) = subscriber_reservation.take() {
                        let event = subscription::Event::Del(
                            key.as_ref().to_vec(),
                        );

                        res.complete(event);
                    }

                    guard.flush();
                    return Ok(existing_key.map(move |r| {
                        PinnedValue::new(r, double_guard)
                    }));
                }
                Err(Error::CasFailed(_)) => {
                    M.tree_looped();
                    continue;
                }
                Err(other) => return Err(other.danger_cast()),
            }
        }
    }

    /// Merge state directly into a given key's value using the
    /// configured merge operator. This allows state to be written
    /// into a value directly, without any read-modify-write steps.
    /// Merge operators can be used to implement arbitrary data
    /// structures.
    ///
    /// # Panics
    ///
    /// Calling `merge` will panic if no merge operator has been
    /// configured.
    ///
    /// # Examples
    ///
    /// ```
    /// fn concatenate_merge(
    ///   _key: &[u8],               // the key being merged
    ///   old_value: Option<&[u8]>,  // the previous value, if one existed
    ///   merged_bytes: &[u8]        // the new bytes being merged in
    /// ) -> Option<Vec<u8>> {       // set the new value, return None to delete
    ///   let mut ret = old_value
    ///     .map(|ov| ov.to_vec())
    ///     .unwrap_or_else(|| vec![]);
    ///
    ///   ret.extend_from_slice(merged_bytes);
    ///
    ///   Some(ret)
    /// }
    ///
    /// let config = sled::ConfigBuilder::new()
    ///   .temporary(true)
    ///   .merge_operator(concatenate_merge)
    ///   .build();
    ///
    /// let tree = sled::Tree::start(config).unwrap();
    ///
    /// let k = b"k1";
    ///
    /// tree.set(k, vec![0]);
    /// tree.merge(k, vec![1]);
    /// tree.merge(k, vec![2]);
    /// // assert_eq!(tree.get(k).unwrap().unwrap(), vec![0, 1, 2]);
    ///
    /// // sets replace previously merged data,
    /// // bypassing the merge function.
    /// tree.set(k, vec![3]);
    /// // assert_eq!(tree.get(k), Ok(Some(vec![3])));
    ///
    /// // merges on non-present values will add them
    /// tree.del(k);
    /// tree.merge(k, vec![4]);
    /// // assert_eq!(tree.get(k).unwrap().unwrap(), vec![4]);
    /// ```
    pub fn merge<K: AsRef<[u8]>>(
        &self,
        key: K,
        value: Value,
    ) -> Result<(), ()> {
        let _measure = Measure::new(&M.tree_merge);

        if self.config.read_only {
            return Err(Error::Unsupported(
                "the database is in read-only mode".to_owned(),
            ));
        }

        let guard = pin();

        loop {
            let mut path = self.path_for_key(key.as_ref(), &guard)?;
            let (leaf_frag, leaf_ptr) = path.pop().expect(
                "path_for_key should always return a path \
                 of length >= 2 (root + leaf)",
            );
            let node: &Node = leaf_frag.unwrap_base();

            let encoded_key =
                prefix_encode(node.lo.inner(), key.as_ref());
            let frag = Frag::Merge(encoded_key, value.clone());

            let link = self.pages.link(
                node.id,
                leaf_ptr.clone(),
                frag.clone(),
                &guard,
            );
            match link {
                Ok(new_cas_key) => {
                    // success
                    if node.should_split(
                        self.config.blink_node_split_size as u64,
                    ) {
                        let mut path2 = path
                            .iter()
                            .map(|&(f, ref p)| {
                                (Cow::Borrowed(f), p.clone())
                            })
                            .collect::<Vec<(Cow<'_, Frag>, _)>>();
                        let mut node2 = node.clone();
                        node2
                            .apply(&frag, self.config.merge_operator);
                        let frag2 =
                            Cow::Owned(Frag::Base(node2, None));
                        path2.push((frag2, new_cas_key));
                        self.recursive_split(path2, &guard)?;
                    }
                    guard.flush();
                    return Ok(());
                }
                Err(Error::CasFailed(_)) => {}
                Err(other) => {
                    guard.flush();
                    return Err(other.danger_cast());
                }
            }
            M.tree_looped();
        }
    }

    /// Create a double-ended iterator over the tuples of keys and
    /// values in this tree.
    ///
    /// # Examples
    ///
    /// ```
    /// let config = sled::ConfigBuilder::new().temporary(true).build();
    /// let t = sled::Tree::start(config).unwrap();
    /// t.set(&[1], vec![10]);
    /// t.set(&[2], vec![20]);
    /// t.set(&[3], vec![30]);
    /// let mut iter = t.iter();
    /// // assert_eq!(iter.next(), Some(Ok((vec![1], vec![10]))));
    /// // assert_eq!(iter.next(), Some(Ok((vec![2], vec![20]))));
    /// // assert_eq!(iter.next(), Some(Ok((vec![3], vec![30]))));
    /// // assert_eq!(iter.next(), None);
    /// ```
    pub fn iter(&self) -> Iter<'_> {
        self.range::<Vec<u8>, _>(..)
    }

    /// Create a double-ended iterator over tuples of keys and values,
    /// starting at the provided key.
    ///
    /// # Examples
    ///
    /// ```
    /// let config = sled::ConfigBuilder::new()
    ///     .temporary(true)
    ///     .build();
    /// let t = sled::Tree::start(config).unwrap();
    ///
    /// t.set(b"0", vec![0]).unwrap();
    /// t.set(b"1", vec![10]).unwrap();
    /// t.set(b"2", vec![20]).unwrap();
    /// t.set(b"3", vec![30]).unwrap();
    /// t.set(b"4", vec![40]).unwrap();
    /// t.set(b"5", vec![50]).unwrap();
    ///
    /// let mut r = t.scan(b"2");
    /// assert_eq!(r.next().unwrap().unwrap().0, b"2");
    /// assert_eq!(r.next().unwrap().unwrap().0, b"3");
    /// assert_eq!(r.next().unwrap().unwrap().0, b"4");
    /// assert_eq!(r.next().unwrap().unwrap().0, b"5");
    /// assert_eq!(r.next(), None);

    /// let mut r = t.scan(b"2").rev();
    /// assert_eq!(r.next().unwrap().unwrap().0, b"2");
    /// assert_eq!(r.next().unwrap().unwrap().0, b"1");
    /// assert_eq!(r.next().unwrap().unwrap().0, b"0");
    /// assert_eq!(r.next(), None);
    /// ```
    pub fn scan<K>(&self, key: K) -> Iter<'_>
    where
        K: AsRef<[u8]>,
    {
        let mut iter = self.range(key..);
        iter.is_scan = true;
        iter
    }

    /// Create a double-ended iterator over tuples of keys and values,
    /// where the keys fall within the specified range.
    ///
    /// # Examples
    ///
    /// ```
    /// let config = sled::ConfigBuilder::new()
    ///     .temporary(true)
    ///     .build();
    /// let t = sled::Tree::start(config).unwrap();
    ///
    /// t.set(b"0", vec![0]).unwrap();
    /// t.set(b"1", vec![10]).unwrap();
    /// t.set(b"2", vec![20]).unwrap();
    /// t.set(b"3", vec![30]).unwrap();
    /// t.set(b"4", vec![40]).unwrap();
    /// t.set(b"5", vec![50]).unwrap();
    ///
    /// let start: &[u8] = b"2";
    /// let end: &[u8] = b"4";
    /// let mut r = t.range(start..end);
    /// assert_eq!(r.next().unwrap().unwrap().0, b"2");
    /// assert_eq!(r.next().unwrap().unwrap().0, b"3");
    /// assert_eq!(r.next(), None);
    ///
    /// let start = b"2".to_vec();
    /// let end = b"4".to_vec();
    /// let mut r = t.range(start..end).rev();
    /// assert_eq!(r.next().unwrap().unwrap().0, b"3");
    /// assert_eq!(r.next().unwrap().unwrap().0, b"2");
    /// assert_eq!(r.next(), None);
    /// ```
    pub fn range<K, R>(&self, range: R) -> Iter<'_>
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
    {
        let _measure = Measure::new(&M.tree_scan);

        let guard = pin();

        let lo = match range.start_bound() {
            ops::Bound::Included(ref end) => {
                ops::Bound::Included(end.as_ref().to_vec())
            }
            ops::Bound::Excluded(ref end) => {
                ops::Bound::Excluded(end.as_ref().to_vec())
            }
            ops::Bound::Unbounded => ops::Bound::Unbounded,
        };
        let hi = match range.end_bound() {
            ops::Bound::Included(ref end) => {
                ops::Bound::Included(end.as_ref().to_vec())
            }
            ops::Bound::Excluded(ref end) => {
                ops::Bound::Excluded(end.as_ref().to_vec())
            }
            ops::Bound::Unbounded => ops::Bound::Unbounded,
        };

        Iter {
            tree: &self,
            hi,
            lo,
            last_id: None,
            last_key: None,
            broken: None,
            done: false,
            is_scan: false,
            guard,
        }
    }

    /// Create a double-ended iterator over keys, starting at the provided key.
    ///
    /// # Examples
    ///
    /// ```
    /// let config = sled::ConfigBuilder::new().temporary(true).build();
    /// let t = sled::Tree::start(config).unwrap();
    /// t.set(&[1], vec![10]);
    /// t.set(&[2], vec![20]);
    /// t.set(&[3], vec![30]);
    /// let mut iter = t.scan(&*vec![2]);
    /// // assert_eq!(iter.next(), Some(Ok(vec![2])));
    /// // assert_eq!(iter.next(), Some(Ok(vec![3])));
    /// // assert_eq!(iter.next(), None);
    /// ```
    pub fn keys<K>(&self, key: K) -> Keys<'_>
    where
        K: AsRef<[u8]>,
    {
        self.scan(key).keys()
    }

    /// Create a double-ended iterator over values, starting at the provided key.
    ///
    /// # Examples
    ///
    /// ```
    /// let config = sled::ConfigBuilder::new().temporary(true).build();
    /// let t = sled::Tree::start(config).unwrap();
    /// t.set(&[1], vec![10]);
    /// t.set(&[2], vec![20]);
    /// t.set(&[3], vec![30]);
    /// let mut iter = t.scan(&*vec![2]);
    /// // assert_eq!(iter.next(), Some(Ok(vec![20])));
    /// // assert_eq!(iter.next(), Some(Ok(vec![30])));
    /// // assert_eq!(iter.next(), None);
    /// ```
    pub fn values<K: AsRef<[u8]>>(&self, key: K) -> Values<'_> {
        self.scan(key).values()
    }

    /// Returns the number of elements in this tree.
    ///
    /// Beware: performs a full O(n) scan under the hood.
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Returns `true` if the `Tree` contains no elements.
    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }

    fn recursive_split<'g>(
        &self,
        path: Vec<(Cow<'g, Frag>, TreePtr<'g>)>,
        guard: &'g Guard,
    ) -> Result<(), ()> {
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

        for window in path.windows(2).rev() {
            let (parent_frag, parent_ptr) = window[0].clone();
            let (node_frag, node_ptr) = window[1].clone();
            let node: &Node = node_frag.unwrap_base();
            if node.should_split(
                self.config.blink_node_split_size as u64,
            ) {
                // try to child split
                if let Ok(parent_split) =
                    self.child_split(node, node_ptr.clone(), guard)
                {
                    // now try to parent split
                    let parent_node = parent_frag.unwrap_base();

                    let res = self.parent_split(
                        parent_node.id,
                        parent_ptr.clone(),
                        parent_split.clone(),
                        guard,
                    );

                    match res {
                        Ok(_res) => {}
                        Err(Error::CasFailed(_)) => continue,
                        other => {
                            return other
                                .map(|_| ())
                                .map_err(|e| e.danger_cast())
                        }
                    }
                }
            }
        }

        let (ref root_frag, ref root_ptr) = path[0];
        let root_node: &Node = root_frag.unwrap_base();

        if root_node
            .should_split(self.config.blink_node_split_size as u64)
        {
            if let Ok(parent_split) =
                self.child_split(&root_node, root_ptr.clone(), guard)
            {
                return self
                    .root_hoist(
                        root_node.id,
                        parent_split.to,
                        parent_split.at.inner().to_vec(),
                        guard,
                    )
                    .map(|_| ())
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
    ) -> Result<ParentSplit, ()> {
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
        let new_ptr = self
            .pages
            .replace(
                new_pid,
                PagePtr::allocated(),
                Frag::Base(rhs, None),
                guard,
            )
            .map_err(|e| e.danger_cast())?;

        // try to install a child split on the left side
        let link = self.pages.link(
            node.id,
            node_cas_key,
            child_split,
            guard,
        );

        match link {
            Ok(_) => {}
            Err(Error::CasFailed(_)) => {
                // if we failed, don't follow through with the parent split
                let mut ptr = new_ptr.clone();
                loop {
                    match self.pages.free(new_pid, ptr, guard) {
                        Err(Error::CasFailed(Some(actual_ptr))) => {
                            ptr = actual_ptr.clone()
                        }
                        Err(Error::CasFailed(None)) => panic!("somehow allocated child was already freed"),
                        Err(other) => return Err(other.danger_cast()),
                        Ok(_) => break,
                    }
                }
                return Err(Error::CasFailed(()));
            }
            Err(other) => return Err(other.danger_cast()),
        }

        Ok(parent_split)
    }

    fn parent_split<'g>(
        &self,
        parent_node_id: usize,
        parent_cas_key: TreePtr<'g>,
        parent_split: ParentSplit,
        guard: &'g Guard,
    ) -> Result<TreePtr<'g>, Option<TreePtr<'g>>> {
        // install parent split
        self.pages.link(
            parent_node_id,
            parent_cas_key,
            Frag::ParentSplit(parent_split.clone()),
            guard,
        )
    }

    fn root_hoist<'g>(
        &self,
        from: PageId,
        to: PageId,
        at: Key,
        guard: &'g Guard,
    ) -> Result<(), ()> {
        // hoist new root, pointing to lhs & rhs
        let new_root_pid = self.pages.allocate(guard)?;
        debug!("allocated pid {} in root_hoist", new_root_pid);

        let root_lo = b"";
        let mut new_root_vec = vec![];
        new_root_vec.push((vec![0], from));

        let encoded_at = prefix_encode(root_lo, &*at);
        new_root_vec.push((encoded_at, to));
        let new_root = Frag::Base(
            Node {
                id: new_root_pid,
                data: Data::Index(new_root_vec),
                next: None,
                lo: Bound::Inclusive(vec![]),
                hi: Bound::Inf,
            },
            Some(from),
        );
        debug_delay();
        let new_root_ptr = self
            .pages
            .replace(
                new_root_pid,
                PagePtr::allocated(),
                new_root,
                guard,
            )
            .expect(
                "we should be able to replace a newly \
                 allocated page without issue",
            );

        debug_delay();
        let cas =
            self.root.compare_and_swap(from, new_root_pid, SeqCst);
        if cas == from {
            debug!(
                "root hoist from {} to {} successful",
                from, new_root_pid
            );
            Ok(())
        } else {
            debug!(
                "root hoist from {} to {} failed",
                from, new_root_pid
            );
            self.pages
                .free(new_root_pid, new_root_ptr, guard)
                .map_err(|e| e.danger_cast())
        }
    }

    fn get_internal<'g, K: AsRef<[u8]>>(
        &self,
        key: K,
        guard: &'g Guard,
    ) -> Result<(Path<'g>, Option<&'g [u8]>), ()> {
        let path = self.path_for_key(key.as_ref(), guard)?;

        let ret = path.last().and_then(|(last_frag, _tree_ptr)| {
            let last_node = last_frag.unwrap_base();
            let data = &last_node.data;
            let items =
                data.leaf_ref().expect("last_node should be a leaf");
            let search = items
                .binary_search_by(|&(ref k, ref _v)| {
                    prefix_cmp_encoded(
                        k,
                        key.as_ref(),
                        last_node.lo.inner(),
                    )
                })
                .ok();

            search.map(|idx| &*items[idx].1)
        });

        Ok((path, ret))
    }

    #[doc(hidden)]
    pub fn key_debug_str<K: AsRef<[u8]>>(&self, key: K) -> String {
        let guard = pin();

        let path = self.path_for_key(key.as_ref(), &guard).expect(
            "path_for_key should always return at least 2 nodes, \
             even if the key being searched for is not present",
        );
        let mut ret = String::new();
        for (node, _ptr) in &path {
            ret.push_str(&*format!("\n{:?}", node));
        }

        guard.flush();

        ret
    }

    /// returns the traversal path, completing any observed
    /// partially complete splits or merges along the way.
    pub(crate) fn path_for_key<'g, K: AsRef<[u8]>>(
        &self,
        key: K,
        guard: &'g Guard,
    ) -> Result<Vec<(&'g Frag, TreePtr<'g>)>, ()> {
        let _measure = Measure::new(&M.tree_traverse);

        let mut cursor = self.root.load(SeqCst);
        let mut path: Vec<(&'g Frag, TreePtr<'g>)> = vec![];

        // unsplit_parent is used for tracking need
        // to complete partial splits.
        let mut unsplit_parent: Option<usize> = None;

        let mut not_found_loops = 0;
        loop {
            let get_cursor = self
                .pages
                .get(cursor, guard)
                .map_err(|e| e.danger_cast())?;

            if get_cursor.is_free() || get_cursor.is_allocated() {
                // restart search from the tree's root
                not_found_loops += 1;
                debug_assert_ne!(
                    not_found_loops, 10_000,
                    "cannot find pid {} in path_for_key",
                    cursor
                );
                cursor = self.root.load(SeqCst);
                continue;
            }

            let (frag, cas_key) = match get_cursor {
                PageGet::Materialized(node, cas_key) => {
                    (node, cas_key)
                }
                broken => {
                    return Err(Error::ReportableBug(format!(
                    "got non-base node while traversing tree: {:?}",
                    broken
                )))
                }
            };

            let node = frag.unwrap_base();

            // TODO this may need to change when handling (half) merges
            assert!(
                node.lo.inner() <= key.as_ref(),
                "overshot key somehow"
            );

            // half-complete split detect & completion
            if node.hi <= Bound::Inclusive(key.as_ref().to_vec()) {
                // we have encountered a child split, without
                // having hit the parent split above.
                cursor = node.next.expect(
                    "if our hi bound is not Inf (inity), \
                     we should have a right sibling",
                );
                if unsplit_parent.is_none() && !path.is_empty() {
                    unsplit_parent = Some(path.len() - 1);
                }
                continue;
            } else if let Some(idx) = unsplit_parent.take() {
                // we have found the proper page for
                // our split.
                let (parent_frag, parent_ptr) = &path[idx];
                let parent_node = parent_frag.unwrap_base();

                let ps = Frag::ParentSplit(ParentSplit {
                    at: node.lo.clone(),
                    to: node.id,
                });

                let link = self.pages.link(
                    parent_node.id,
                    parent_ptr.clone(),
                    ps,
                    guard,
                );
                match link {
                    Ok(_new_key) => {
                        // TODO set parent's cas_key (not this cas_key) to
                        // new_key in the path, along with updating the
                        // parent's node in the path vec. if we don't do
                        // both, we lose the newly appended parent split.
                    }
                    Err(Error::CasFailed(_)) => {}
                    Err(other) => return Err(other.danger_cast()),
                }
            }

            path.push((frag, cas_key.clone()));

            match path
                .last()
                .expect("we just pushed to path, so it's not empty")
                .0
                .unwrap_base()
                .data
            {
                Data::Index(ref ptrs) => {
                    let old_cursor = cursor;

                    let search = binary_search_lub(
                        ptrs,
                        |&(ref k, ref _v)| {
                            prefix_cmp_encoded(
                                k,
                                key.as_ref(),
                                node.lo.inner(),
                            )
                        },
                    );

                    // This might be none if ord is Less and we're
                    // searching for the empty key
                    let index =
                        search.expect("failed to traverse index");

                    cursor = ptrs[index].1;

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
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> std::result::Result<(), fmt::Error> {
        let mut pid = self.root.load(SeqCst);
        let mut left_most = pid;
        let mut level = 0;

        f.write_str("Tree: \n\t")?;
        self.pages.fmt(f)?;
        f.write_str("\tlevel 0:\n")?;

        let guard = pin();

        loop {
            let get_res = self.pages.get(pid, &guard);
            let node = match get_res {
                Ok(PageGet::Materialized(ref frag, ref _ptr)) => {
                    frag.unwrap_base()
                }
                broken => panic!(
                    "pagecache returned non-base node: {:?}",
                    broken
                ),
            };

            f.write_str("\t\t")?;
            node.fmt(f)?;
            f.write_str("\n")?;

            if let Some(next_pid) = node.next {
                pid = next_pid;
            } else {
                // we've traversed our level, time to bump down
                let left_get_res = self.pages.get(left_most, &guard);
                let left_node = match left_get_res {
                    Ok(PageGet::Materialized(mf, ..)) => {
                        mf.unwrap_base()
                    }
                    broken => panic!(
                        "pagecache returned non-base node: {:?}",
                        broken
                    ),
                };

                match &left_node.data {
                    Data::Index(ptrs) => {
                        if let Some(&(ref _sep, ref next_pid)) =
                            ptrs.first()
                        {
                            pid = *next_pid;
                            left_most = *next_pid;
                            level += 1;
                            f.write_str(&*format!(
                                "\n\tlevel {}:\n",
                                level
                            ))?;
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

        guard.flush();

        Ok(())
    }
}
