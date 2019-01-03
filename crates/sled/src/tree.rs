use std::{
    borrow::Cow,
    cmp::Ordering::{Greater, Less},
    fmt::{self, Debug},
    ops::{self, RangeBounds},
    sync::Arc,
};

use pagecache::PagePtr;

use super::*;

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
/// let t = sled::Db::start_default("path_to_my_database").unwrap();
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
    pub(crate) tree_id: Vec<u8>,
    pub(crate) config: Config,
    pub(crate) subscriptions: Arc<Subscriptions>,
    pub(crate) pages:
        Arc<PageCache<BLinkMaterializer, Frag, Recovery>>,
}

unsafe impl Send for Tree {}
unsafe impl Sync for Tree {}

impl Tree {
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
    /// let tree = sled::Db::start(config).unwrap();
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
    /// let tree = sled::Db::start(config).unwrap();
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
            prefix_cmp_encoded(k, key.as_ref(), &last_node.lo)
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
                prefix_decode(&last_node.lo, &*encoded_key),
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
    /// let tree = sled::Db::start(config).unwrap();
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
                prefix_cmp_encoded(k, key.as_ref(), &last_node.lo)
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
                prefix_decode(&last_node.lo, &*encoded_key),
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
    /// let t = sled::Db::start(config).unwrap();
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

            if old != cur.map(|v| &*v) {
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
                (node.id, prefix_encode(&node.lo, key.as_ref()))
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
            let encoded_key = prefix_encode(&node.lo, key.as_ref());

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
                        let frag2 = Cow::Owned(Frag::Base(node2));
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
    /// let t = sled::Db::start(config).unwrap();
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
            let encoded_key = prefix_encode(&node.lo, key.as_ref());

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
    /// let tree = sled::Db::start(config).unwrap();
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

            let encoded_key = prefix_encode(&node.lo, key.as_ref());
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
                        let frag2 = Cow::Owned(Frag::Base(node2));
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
    /// let t = sled::Db::start(config).unwrap();
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
    /// let t = sled::Db::start(config).unwrap();
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
    /// let t = sled::Db::start(config).unwrap();
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
    /// let t = sled::Db::start(config).unwrap();
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
    /// let t = sled::Db::start(config).unwrap();
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
                        parent_split.at.clone(),
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
                Frag::Base(rhs),
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
        let new_root = Frag::Base(Node {
            id: new_root_pid,
            data: Data::Index(new_root_vec),
            next: None,
            lo: vec![],
            hi: vec![],
        });
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
        let cas = meta::cas_root(
            &*self.pages,
            self.tree_id.clone(),
            Some(from),
            new_root_pid,
            guard,
        );
        if cas.is_ok() {
            debug!(
                "root hoist from {} to {} successful",
                from, new_root_pid
            );
            Ok(())
        } else {
            debug!(
                "root hoist from {} to {} failed: {:?}",
                from, new_root_pid, cas
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
                    prefix_cmp_encoded(k, key.as_ref(), &last_node.lo)
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

        let mut cursor =
            meta::pid_for_name(&*self.pages, &self.tree_id, guard)
                .unwrap()
                .unwrap();
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
                cursor = meta::pid_for_name(
                    &*self.pages,
                    &self.tree_id,
                    guard,
                )?
                .unwrap();
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
                node.lo.as_slice() <= key.as_ref(),
                "overshot key somehow"
            );

            // half-complete split detect & completion
            // (when hi is empty, it means it's unbounded)
            if !node.hi.is_empty()
                && node.hi.as_slice() <= key.as_ref()
            {
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
                                &node.lo,
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
        let guard = pin();

        let mut pid =
            meta::pid_for_name(&*self.pages, &self.tree_id, &guard)
                .unwrap()
                .unwrap();
        let mut left_most = pid;
        let mut level = 0;

        f.write_str("Tree: \n\t")?;
        self.pages.fmt(f)?;
        f.write_str("\tlevel 0:\n")?;

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
