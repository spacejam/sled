use std::{
    borrow::Cow,
    cmp::Ordering::{Greater, Less},
    fmt::{self, Debug},
    ops::{self, RangeBounds},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

use super::*;

type Path<'g> = Vec<(PageId, &'g Frag, TreePtr<'g>)>;

impl<'a> IntoIterator for &'a Tree {
    type Item = Result<(Vec<u8>, IVec)>;
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
    pub(crate) context: Context,
    pub(crate) subscriptions: Arc<Subscriptions>,
    pub(crate) root: Arc<AtomicUsize>,
}

unsafe impl Send for Tree {}

unsafe impl Sync for Tree {}

impl Tree {
    /// Set a key to a new value, returning the old value if it
    /// was set.
    pub fn set<K, V>(&self, key: K, value: V) -> Result<Option<IVec>>
    where
        K: AsRef<[u8]>,
        IVec: From<V>,
    {
        trace!("setting key {:?}", key.as_ref());
        let _measure = Measure::new(&M.tree_set);

        if self.context.read_only {
            return Err(Error::Unsupported(
                "the database is in read-only mode".to_owned(),
            ));
        }

        let value = IVec::from(value);

        loop {
            let tx = self.context.pagecache.begin()?;
            let (mut path, existing_val) =
                self.get_internal(key.as_ref(), &tx)?;
            let (leaf_id, leaf_frag, leaf_ptr) = path.pop().expect(
                "path_for_key should always return a path \
                 of length >= 2 (root + leaf)",
            );
            let node: &Node = leaf_frag.unwrap_base();
            let encoded_key = prefix_encode(&node.lo, key.as_ref());

            let mut subscriber_reservation = self.subscriptions.reserve(&key);

            let frag = Frag::Set(encoded_key, value.clone());
            let link = self.context.pagecache.link(
                leaf_id,
                leaf_ptr.clone(),
                frag.clone(),
                &tx,
            )?;
            if let Ok(new_cas_key) = link {
                // success
                if let Some(res) = subscriber_reservation.take() {
                    let event =
                        subscription::Event::Set(key.as_ref().to_vec(), value);

                    res.complete(event);
                }

                if node.should_split(self.context.blink_node_split_size as u64)
                {
                    let mut path2 = path
                        .iter()
                        .map(|&(id, f, ref p)| {
                            (id, Cow::Borrowed(f), p.clone())
                        })
                        .collect::<Vec<(PageId, Cow<'_, Frag>, _)>>();
                    let mut node2 = node.clone();
                    node2.apply(&frag, self.context.merge_operator);
                    let frag2 = Cow::Owned(Frag::Base(node2));
                    path2.push((leaf_id, frag2, new_cas_key));
                    self.recursive_split(path2, &tx)?;
                }

                tx.flush();

                return Ok(existing_val.cloned());
            }
            M.tree_looped();
        }
    }

    /// Retrieve a value from the `Tree` if it exists.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<IVec>> {
        let _measure = Measure::new(&M.tree_get);

        let tx = self.context.pagecache.begin()?;

        let (_, ret) = self.get_internal(key.as_ref(), &tx)?;

        tx.flush();

        Ok(ret.cloned())
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
    pub fn del<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<IVec>> {
        let _measure = Measure::new(&M.tree_del);

        if self.context.read_only {
            return Ok(None);
        }

        loop {
            let tx = self.context.pagecache.begin()?;

            let (mut path, existing_val) =
                self.get_internal(key.as_ref(), &tx)?;

            let mut subscriber_reservation = self.subscriptions.reserve(&key);

            let (leaf_id, leaf_frag, leaf_ptr) = path.pop().expect(
                "path_for_key should always return a path \
                 of length >= 2 (root + leaf)",
            );
            let node: &Node = leaf_frag.unwrap_base();
            let encoded_key = prefix_encode(&node.lo, key.as_ref());

            let frag = Frag::Del(encoded_key);
            let link = self.context.pagecache.link(
                leaf_id,
                leaf_ptr.clone(),
                frag,
                &tx,
            )?;

            if link.is_ok() {
                // success
                if let Some(res) = subscriber_reservation.take() {
                    let event = subscription::Event::Del(key.as_ref().to_vec());

                    res.complete(event);
                }

                tx.flush();
                return Ok(existing_val.cloned());
            }
        }
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
    /// assert_eq!(t.cas(&[1], None as Option<&[u8]>, Some("1")), Ok(Ok(())));
    ///
    /// // conditional modification
    /// assert_eq!(t.cas(&[1], Some("1"), Some("2")), Ok(Ok(())));
    ///
    /// // conditional deletion
    /// assert_eq!(t.cas(&[1], Some("2"), None as Option<&[u8]>), Ok(Ok(())));
    /// assert_eq!(t.get(&[1]), Ok(None));
    /// ```
    pub fn cas<K, OV, NV>(
        &self,
        key: K,
        old: Option<OV>,
        new: Option<NV>,
    ) -> Result<std::result::Result<(), Option<IVec>>>
    where
        K: AsRef<[u8]>,
        OV: AsRef<[u8]>,
        IVec: From<NV>,
    {
        trace!("casing key {:?}", key.as_ref());
        let _measure = Measure::new(&M.tree_cas);

        if self.context.read_only {
            return Err(Error::Unsupported(
                "can not perform a cas on a read-only Tree".into(),
            ));
        }

        let new = new.map(IVec::from);

        // we need to retry caps until old != cur, since just because
        // cap fails it doesn't mean our value was changed.
        loop {
            let tx = self.context.pagecache.begin()?;
            let (mut path, cur) = self.get_internal(key.as_ref(), &tx)?;

            let matches = match (&old, &cur) {
                (None, None) => true,
                (Some(ref o), Some(ref c)) => o.as_ref() == &***c,
                _ => false,
            };

            if !matches {
                return Ok(Err(cur.cloned()));
            }

            let mut subscriber_reservation = self.subscriptions.reserve(&key);

            let (leaf_id, leaf_frag, leaf_ptr) = path
                .pop()
                .expect("get_internal somehow returned a path of length zero");

            let (node_id, encoded_key) = {
                let node: &Node = leaf_frag.unwrap_base();
                (leaf_id, prefix_encode(&node.lo, key.as_ref()))
            };
            let frag = if let Some(ref new) = new {
                Frag::Set(encoded_key, new.clone())
            } else {
                Frag::Del(encoded_key)
            };
            let link =
                self.context.pagecache.link(node_id, leaf_ptr, frag, &tx)?;

            if link.is_ok() {
                if let Some(res) = subscriber_reservation.take() {
                    let event = if let Some(new) = new {
                        subscription::Event::Set(key.as_ref().to_vec(), new)
                    } else {
                        subscription::Event::Del(key.as_ref().to_vec())
                    };

                    res.complete(event);
                }

                tx.flush();
                return Ok(Ok(()));
            }
            M.tree_looped();
        }
    }

    /// Fetch the value, apply a function to it and return the result.
    ///
    /// # Note
    ///
    /// This may call the function multiple times if the value has been
    /// changed from other threads in the meantime.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::{ConfigBuilder, IVec, Error};
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let t = sled::Db::start(config).unwrap();
    ///
    /// assert_eq!(t.update_and_fetch(b"hello", |_old| Some(vec![1])), Ok(Some(IVec::from(vec![1]))));
    /// ```
    pub fn update_and_fetch<K, V, F>(
        &self,
        key: K,
        f: F,
    ) -> Result<Option<IVec>>
    where
        K: AsRef<[u8]>,
        F: Fn(Option<&[u8]>) -> Option<V>,
        IVec: From<V>,
    {
        let key = key.as_ref();
        let mut current = self.get(key)?;

        loop {
            let tmp = current.as_ref().map(AsRef::as_ref);
            let next = f(tmp).map(IVec::from);
            match self.cas::<_, _, IVec>(key, tmp, next.clone())? {
                Ok(()) => return Ok(next),
                Err(new_current) => current = new_current,
            }
        }
    }

    /// Fetch the value, apply a function to it and return the previous value.
    ///
    /// # Note
    ///
    /// This may call the function multiple times if the value has been
    /// changed from other threads in the meantime.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::{ConfigBuilder, Error};
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let t = sled::Db::start(config).unwrap();
    ///
    /// assert_eq!(t.fetch_and_update(b"hello", |_old| Some(vec![1])), Ok(None));
    /// ```
    pub fn fetch_and_update<K, V, F>(
        &self,
        key: K,
        f: F,
    ) -> Result<Option<IVec>>
    where
        K: AsRef<[u8]>,
        F: Fn(Option<&[u8]>) -> Option<V>,
        IVec: From<V>,
    {
        let key = key.as_ref();
        let mut current = self.get(key)?;

        loop {
            let tmp = current.as_ref().map(AsRef::as_ref);
            let next = f(tmp);
            match self.cas(key, tmp, next)? {
                Ok(()) => return Ok(current),
                Err(new_current) => current = new_current,
            }
        }
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

    /// Flushes all dirty IO buffers and calls fsync.
    /// If this succeeds, it is guaranteed that
    /// all previous writes will be recovered if
    /// the system crashes. Returns the number
    /// of bytes flushed during this call.
    pub fn flush(&self) -> Result<usize> {
        self.context.pagecache.flush()
    }

    /// Returns `true` if the `Tree` contains a value for
    /// the specified key.
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> Result<bool> {
        self.get(key).map(|v| v.is_some())
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
    ) -> Result<Option<(Key, IVec)>> {
        let _measure = Measure::new(&M.tree_get);

        // the double tx is a hack that maintains
        // correctness of the ret value
        let tx = self.context.pagecache.begin()?;

        let path = self.path_for_key(key.as_ref(), &tx)?;
        let (_last_id, last_frag, _tree_ptr) = path
            .last()
            .expect("path should always contain a last element");

        let last_node = last_frag.unwrap_base();
        let data = &last_node.data;
        let items = data.leaf_ref().expect("last_node should be a leaf");
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
            Some((prefix_decode(&last_node.lo, &*encoded_key), v.clone()))
        };

        tx.flush();

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
    ) -> Result<Option<(Key, IVec)>> {
        let _measure = Measure::new(&M.tree_get);

        let tx = self.context.pagecache.begin()?;

        let path = self.path_for_key(key.as_ref(), &tx)?;
        let (_last_id, last_frag, _tree_ptr) = path
            .last()
            .expect("path should always contain a last element");

        let last_node = last_frag.unwrap_base();
        let data = &last_node.data;
        let items = data.leaf_ref().expect("last_node should be a leaf");
        let search = leaf_search(Greater, items, |&(ref k, ref _v)| {
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
            Some((prefix_decode(&last_node.lo, &*encoded_key), v.clone()))
        };

        tx.flush();

        Ok(ret)
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
    pub fn merge<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        IVec: From<V>,
    {
        trace!("merging key {:?}", key.as_ref());
        let _measure = Measure::new(&M.tree_merge);

        if self.context.read_only {
            return Err(Error::Unsupported(
                "the database is in read-only mode".to_owned(),
            ));
        }

        let value = IVec::from(value);

        loop {
            let tx = self.context.pagecache.begin()?;

            let mut path = self.path_for_key(key.as_ref(), &tx)?;
            let (leaf_id, leaf_frag, leaf_ptr) = path.pop().expect(
                "path_for_key should always return a path \
                 of length >= 2 (root + leaf)",
            );
            let node: &Node = leaf_frag.unwrap_base();

            let mut subscriber_reservation = self.subscriptions.reserve(&key);

            let encoded_key = prefix_encode(&node.lo, key.as_ref());
            let frag = Frag::Merge(encoded_key, value.clone());

            let link = self.context.pagecache.link(
                leaf_id,
                leaf_ptr.clone(),
                frag.clone(),
                &tx,
            )?;
            if let Ok(new_cas_key) = link {
                // success
                if let Some(res) = subscriber_reservation.take() {
                    let event = subscription::Event::Merge(
                        key.as_ref().to_vec(),
                        value,
                    );

                    res.complete(event);
                }
                if node.should_split(self.context.blink_node_split_size as u64)
                {
                    let mut path2 = path
                        .iter()
                        .map(|&(id, f, ref p)| {
                            (id, Cow::Borrowed(f), p.clone())
                        })
                        .collect::<Vec<(PageId, Cow<'_, Frag>, _)>>();
                    let mut node2 = node.clone();
                    node2.apply(&frag, self.context.merge_operator);
                    let frag2 = Cow::Owned(Frag::Base(node2));
                    path2.push((leaf_id, frag2, new_cas_key));
                    self.recursive_split(path2, &tx)?;
                }
                tx.flush();
                return Ok(());
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

        let tx = match self.context.pagecache.begin() {
            Ok(tx) => tx,
            Err(e) => {
                return Iter {
                    tree: &self,
                    tx: Tx::new(0),
                    broken: Some(e),
                    done: false,
                    hi: ops::Bound::Unbounded,
                    lo: ops::Bound::Unbounded,
                    is_scan: false,
                    last_key: None,
                    last_id: None,
                };
            }
        };

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
            tx,
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
    pub fn keys<'a, K>(
        &'a self,
        key: K,
    ) -> impl 'a + DoubleEndedIterator<Item = Result<Vec<u8>>>
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
    pub fn values<'a, K>(
        &'a self,
        key: K,
    ) -> impl 'a + DoubleEndedIterator<Item = Result<IVec>>
    where
        K: AsRef<[u8]>,
    {
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

    /// Clears the `Tree`, removing all values.
    ///
    /// Note that this is not atomic.
    pub fn clear(&self) -> Result<()> {
        for k in self.keys(b"") {
            let key = k?;
            self.del(key)?;
        }
        Ok(())
    }

    /// Returns the name of the tree.
    pub fn name(&self) -> Vec<u8> {
        self.tree_id.clone()
    }

    fn recursive_split<'g>(
        &self,
        path: Vec<(PageId, Cow<'g, Frag>, TreePtr<'g>)>,
        tx: &'g Tx,
    ) -> Result<()> {
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

        let adjusted_max = |height| {
            // nodes toward the root are larger
            let threshold = std::cmp::min(height, 8) as u32;
            let multiplier = 2_u64.pow(threshold);
            self.context.blink_node_split_size as u64 * multiplier
        };

        for (height, node_parts) in path.iter().skip(1).rev().enumerate() {
            let (node_id, node_frag, node_ptr) = &node_parts;
            let node: &Node = node_frag.unwrap_base();
            if node.should_split(adjusted_max(height)) {
                // try to child split
                M.tree_child_split_attempt();

                if self
                    .child_split(*node_id, node, node_ptr.clone(), tx)?
                    .is_some()
                {
                    M.tree_child_split_success();
                } else {
                    return Ok(());
                }
            }
        }

        let (ref root_id, ref root_frag, ref root_ptr) = path[0];
        let root_node: &Node = root_frag.unwrap_base();

        if root_node.should_split(adjusted_max(path.len())) {
            M.tree_root_split_attempt();
            if let Some(parent_split) =
                self.child_split(*root_id, &root_node, root_ptr.clone(), tx)?
            {
                if self
                    .root_hoist(
                        *root_id,
                        parent_split.to,
                        parent_split.at.clone(),
                        tx,
                    )
                    .is_ok()
                {
                    M.tree_root_split_success();
                }
            }
        }

        Ok(())
    }

    fn child_split<'g>(
        &self,
        node_id: PageId,
        node: &Node,
        node_cas_key: TreePtr<'g>,
        tx: &'g Tx,
    ) -> Result<Option<ParentSplit>> {
        // split the node in half
        let rhs = node.split();

        let rhs_lo = rhs.lo.clone();

        let mut child_split = ChildSplit {
            at: rhs_lo.clone(),
            to: 0,
        };

        // install the new right side
        let (new_pid, new_ptr) =
            self.context.pagecache.allocate(Frag::Base(rhs), tx)?;

        trace!("allocated pid {} in child_split", new_pid);

        child_split.to = new_pid;

        let parent_split = ParentSplit {
            at: rhs_lo,
            to: new_pid,
        };

        // try to install a child split on the left side
        let link = self.context.pagecache.link(
            node_id,
            node_cas_key,
            Frag::ChildSplit(child_split),
            tx,
        )?;

        if link.is_err() {
            // if we failed, don't follow through with the parent split
            self.context
                .pagecache
                .free(new_pid, new_ptr, tx)?
                .expect("could not free allocated page");
            return Ok(None);
        }

        Ok(Some(parent_split))
    }

    fn root_hoist<'g>(
        &self,
        from: PageId,
        to: PageId,
        at: IVec,
        tx: &'g Tx,
    ) -> Result<()> {
        // hoist new root, pointing to lhs & rhs
        let root_lo = b"";
        let mut new_root_vec = vec![];
        new_root_vec.push((vec![0].into(), from));

        let encoded_at = prefix_encode(root_lo, &*at);
        new_root_vec.push((encoded_at, to));

        let new_root = Frag::Base(Node {
            data: Data::Index(new_root_vec.into()),
            next: None,
            lo: vec![].into(),
            hi: vec![].into(),
        });

        let (new_root_pid, new_root_ptr) =
            self.context.pagecache.allocate(new_root, tx)?;
        debug!("allocated pid {} in root_hoist", new_root_pid);

        debug_delay();

        let cas = self.context.pagecache.cas_root_in_meta(
            self.tree_id.clone(),
            Some(from),
            Some(new_root_pid),
            tx,
        )?;
        if cas.is_ok() {
            debug!("root hoist from {} to {} successful", from, new_root_pid);

            // we spin in a cas loop because it's possible
            // 2 threads are at this point, and we don't want
            // to cause roots to diverge between meta and
            // our version.
            while self.root.compare_and_swap(from, new_root_pid, SeqCst) != from
            {
            }

            Ok(())
        } else {
            debug!(
                "root hoist from {} to {} failed: {:?}",
                from, new_root_pid, cas
            );
            self.context
                .pagecache
                .free(new_root_pid, new_root_ptr, tx)?
                .expect("could not free allocated page");

            Ok(())
        }
    }

    fn get_internal<'g, K: AsRef<[u8]>>(
        &self,
        key: K,
        tx: &'g Tx,
    ) -> Result<(Path<'g>, Option<&'g IVec>)> {
        let path = self.path_for_key(key.as_ref(), tx)?;

        let ret = path.last().and_then(|(_last_id, last_frag, _tree_ptr)| {
            let last_node = last_frag.unwrap_base();
            let data = &last_node.data;
            let items = data.leaf_ref().expect("last_node should be a leaf");
            let search = items
                .binary_search_by(|&(ref k, ref _v)| {
                    prefix_cmp_encoded(k, key.as_ref(), &last_node.lo)
                })
                .ok();

            search.map(|idx| &items[idx].1)
        });

        Ok((path, ret))
    }

    #[doc(hidden)]
    pub fn key_debug_str<K: AsRef<[u8]>>(&self, key: K) -> String {
        let tx = self.context.pagecache.begin().unwrap();

        let path = self.path_for_key(key.as_ref(), &tx).expect(
            "path_for_key should always return at least 2 nodes, \
             even if the key being searched for is not present",
        );
        let mut ret = String::new();
        for (id, node, _ptr) in &path {
            ret.push_str(&*format!("\n{}: {:?}", id, node));
        }

        tx.flush();

        ret
    }

    /// returns the traversal path, completing any observed
    /// partially complete splits or merges along the way.
    pub(crate) fn path_for_key<'g, K: AsRef<[u8]>>(
        &self,
        key: K,
        tx: &'g Tx,
    ) -> Result<Path<'g>> {
        let _measure = Measure::new(&M.tree_traverse);

        let mut cursor = self.root.load(SeqCst);
        let mut path: Vec<(PageId, &'g Frag, TreePtr<'g>)> = vec![];

        // unsplit_parent is used for tracking need
        // to complete partial splits.
        let mut unsplit_parent: Option<usize> = None;

        let mut not_found_loops = 0;
        loop {
            if cursor == usize::max_value() {
                // this collection has been explicitly removed
                return Err(Error::CollectionNotFound(self.tree_id.clone()));
            }
            let get_cursor = self.context.pagecache.get(cursor, tx)?;

            if get_cursor.is_free() {
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
                PageGet::Materialized(node, cas_key) => (node, cas_key),
                broken => {
                    return Err(Error::ReportableBug(format!(
                        "got non-base node while traversing tree: {:?}",
                        broken
                    )));
                }
            };

            let node = frag.unwrap_base();

            // TODO this may need to change when handling (half) merges
            assert!(node.lo.as_ref() <= key.as_ref(), "overshot key somehow");

            // half-complete split detect & completion
            // (when hi is empty, it means it's unbounded)
            if !node.hi.is_empty() && node.hi.as_ref() <= key.as_ref() {
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
                let (parent_id, _parent_frag, parent_ptr) = &path[idx];

                let ps = Frag::ParentSplit(ParentSplit {
                    at: node.lo.clone(),
                    to: cursor,
                });

                M.tree_parent_split_attempt();
                let link = self.context.pagecache.link(
                    *parent_id,
                    parent_ptr.clone(),
                    ps,
                    tx,
                )?;
                if let Ok(_new_key) = link {
                    // TODO set parent's cas_key (not this cas_key) to
                    // new_key in the path, along with updating the
                    // parent's node in the path vec. if we don't do
                    // both, we lose the newly appended parent split.
                    M.tree_parent_split_success();
                }
            }

            path.push((cursor, frag, cas_key.clone()));

            match path
                .last()
                .expect("we just pushed to path, so it's not empty")
                .1
                .unwrap_base()
                .data
            {
                Data::Index(ref ptrs) => {
                    let old_cursor = cursor;

                    let search = binary_search_lub(ptrs, |&(ref k, ref _v)| {
                        prefix_cmp_encoded(k, key.as_ref(), &node.lo)
                    });

                    // This might be none if ord is Less and we're
                    // searching for the empty key
                    let index = search.expect("failed to traverse index");

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

    // Remove all pages for this tree from the underlying
    // PageCache. This will leave orphans behind if
    // the tree crashes during gc.
    pub(crate) fn gc_pages(
        &self,
        mut leftmost_chain: Vec<PageId>,
    ) -> Result<()> {
        let tx = self.context.pagecache.begin()?;

        while let Some(mut pid) = leftmost_chain.pop() {
            loop {
                let get_cursor = self.context.pagecache.get(pid, &tx)?;

                let (node, key) = match get_cursor {
                    PageGet::Materialized(node, key) => (node, key),
                    PageGet::Free(_) => {
                        error!("encountered Free node while GC'ing tree");
                        break;
                    }
                    broken => {
                        return Err(Error::ReportableBug(format!(
                            "got non-base node while GC'ing tree: {:?}",
                            broken
                        )));
                    }
                };

                let ret = self.context.pagecache.free(pid, key.clone(), &tx)?;

                if ret.is_ok() {
                    let next_pid = node.unwrap_base().next.unwrap_or(0);
                    if next_pid == 0 {
                        break;
                    }
                    assert_ne!(pid, next_pid);
                    pid = next_pid;
                }
            }
        }

        Ok(())
    }
}

impl Debug for Tree {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> std::result::Result<(), fmt::Error> {
        let tx = self.context.pagecache.begin().unwrap();

        let mut pid = self.root.load(SeqCst);
        let mut left_most = pid;
        let mut level = 0;

        f.write_str("Tree: \n\t")?;
        self.context.pagecache.fmt(f)?;
        f.write_str("\tlevel 0:\n")?;

        loop {
            let get_res = self.context.pagecache.get(pid, &tx);
            let node = match get_res {
                Ok(PageGet::Materialized(ref frag, ref _ptr)) => {
                    frag.unwrap_base()
                }
                broken => {
                    panic!("pagecache returned non-base node: {:?}", broken)
                }
            };

            f.write_str("\t\t")?;
            node.fmt(f)?;
            f.write_str("\n")?;

            if let Some(next_pid) = node.next {
                pid = next_pid;
            } else {
                // we've traversed our level, time to bump down
                let left_get_res = self.context.pagecache.get(left_most, &tx);
                let left_node = match left_get_res {
                    Ok(PageGet::Materialized(mf, ..)) => mf.unwrap_base(),
                    broken => {
                        panic!("pagecache returned non-base node: {:?}", broken)
                    }
                };

                match &left_node.data {
                    Data::Index(ptrs) => {
                        if let Some(&(ref _sep, ref next_pid)) = ptrs.front() {
                            pid = *next_pid;
                            left_most = *next_pid;
                            level += 1;
                            f.write_str(&*format!("\n\tlevel {}:\n", level))?;
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

        tx.flush();

        Ok(())
    }
}
