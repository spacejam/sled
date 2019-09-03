use std::{
    fmt::{self, Debug},
    ops::{self, RangeBounds},
    sync::{
        atomic::{AtomicU64, Ordering::SeqCst},
        Arc,
    },
};

use parking_lot::RwLock;

use pagecache::Guard;

use super::*;

#[derive(Debug, Clone)]
pub(crate) struct View<'g> {
    pub ptr: TreePtr<'g>,
    pub pid: PageId,
    pub node: &'g Node,
    pub size: u64,
}

impl<'g> std::ops::Deref for View<'g> {
    type Target = Node;

    fn deref(&self) -> &Node {
        &self.node
    }
}

impl<'a> IntoIterator for &'a Tree {
    type Item = Result<(IVec, IVec)>;
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
/// use sled::{Db, IVec};
///
/// let t = Db::open("db").unwrap();
/// t.insert(b"yo!", b"v1".to_vec());
/// assert_eq!(t.get(b"yo!"), Ok(Some(IVec::from(b"v1"))));
///
/// // Atomic compare-and-swap.
/// t.cas(
///     b"yo!",      // key
///     Some(b"v1"), // old value, None for not present
///     Some(b"v2"), // new value, None for delete
/// ).unwrap();
///
/// // Iterates over key-value pairs, starting at the given key.
/// let scan_key: &[u8] = b"a non-present key before yo!";
/// let mut iter = t.range(scan_key..);
/// assert_eq!(iter.next().unwrap(), Ok((IVec::from(b"yo!"), IVec::from(b"v2"))));
/// assert_eq!(iter.next(), None);
///
/// t.remove(b"yo!");
/// assert_eq!(t.get(b"yo!"), Ok(None));
/// ```
#[derive(Clone)]
pub struct Tree {
    pub(crate) tree_id: Vec<u8>,
    pub(crate) context: Context,
    pub(crate) subscriptions: Arc<Subscriptions>,
    pub(crate) root: Arc<AtomicU64>,
    pub(crate) concurrency_control: Arc<RwLock<()>>,
    pub(crate) merge_operator: Arc<RwLock<Option<MergeOperator>>>,
}

unsafe impl Send for Tree {}

unsafe impl Sync for Tree {}

impl Tree {
    #[doc(hidden)]
    #[deprecated(since = "0.24.2", note = "replaced by `Tree::insert`")]
    pub fn set<K, V>(&self, key: K, value: V) -> Result<Option<IVec>>
    where
        K: AsRef<[u8]>,
        IVec: From<V>,
    {
        self.insert(key, value)
    }

    /// Insert a key to a new value, returning the last value if it
    /// was set.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::{ConfigBuilder, Db, IVec};
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let t = Db::start(config).unwrap();
    ///
    /// assert_eq!(t.insert(&[1,2,3], vec![0]), Ok(None));
    /// assert_eq!(t.insert(&[1,2,3], vec![1]), Ok(Some(IVec::from(&[0]))));
    /// ```
    pub fn insert<K, V>(&self, key: K, value: V) -> Result<Option<IVec>>
    where
        K: AsRef<[u8]>,
        IVec: From<V>,
    {
        let _ = self.concurrency_control.read();
        self.insert_inner(key, value)
    }

    pub(crate) fn insert_inner<K, V>(
        &self,
        key: K,
        value: V,
    ) -> Result<Option<IVec>>
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
            let guard = pin();
            let View { ptr, pid, node, .. } =
                self.node_for_key(key.as_ref(), &guard)?;

            let mut subscriber_reservation = self.subscriptions.reserve(&key);

            let (encoded_key, last_value) =
                if let Some((k, v)) = node.leaf_pair_for_key(key.as_ref()) {
                    (k.clone(), Some(v.clone()))
                } else {
                    let k = prefix_encode(&node.lo, key.as_ref());
                    let old_v = None;
                    (k, old_v)
                };
            let frag = Frag::Set(encoded_key, value.clone());
            let link = self.context.pagecache.link(
                pid,
                ptr.clone(),
                frag.clone(),
                &guard,
            )?;
            if let Ok(_new_cas_key) = link {
                // success
                if let Some(res) = subscriber_reservation.take() {
                    let event =
                        subscription::Event::Insert(key.as_ref().into(), value);

                    res.complete(event);
                }

                return Ok(last_value);
            }
            M.tree_looped();
        }
    }

    /// Perform a multi-key serializable transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::{ConfigBuilder, Db};
    ///
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let db = Db::start(config).unwrap();
    ///
    /// // Use write-only transactions as a writebatch:
    /// db.transaction(|db| {
    ///     db.insert(b"k1", b"cats")?;
    ///     db.insert(b"k2", b"dogs")?;
    ///     Ok(())
    /// }).unwrap();
    ///
    /// // Atomically swap two items:
    /// db.transaction(|db| {
    ///     let v1_option = db.remove(b"k1")?;
    ///     let v1 = v1_option.unwrap();
    ///     let v2_option = db.remove(b"k2")?;
    ///     let v2 = v2_option.unwrap();
    ///
    ///     db.insert(b"k1", v2);
    ///     db.insert(b"k2", v1);
    ///
    ///     Ok(())
    /// }).unwrap();
    ///
    /// assert_eq!(&db.get(b"k1").unwrap().unwrap(), b"dogs");
    /// assert_eq!(&db.get(b"k2").unwrap().unwrap(), b"cats");
    /// ```
    ///
    /// Transactions also work on tuples of `Tree`s,
    /// preserving serializable ACID semantics!
    /// In this example, we treat two trees like a
    /// work queue, atomically apply updates to
    /// data and move them from the unprocessed `Tree`
    /// to the processed `Tree`.
    ///
    /// ```
    /// use sled::{ConfigBuilder, Db, Transactional};
    ///
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let db = Db::start(config).unwrap();
    ///
    /// let unprocessed = db.open_tree(b"unprocessed items").unwrap();
    /// let processed = db.open_tree(b"processed items").unwrap();
    ///
    /// // An update somehow gets into the tree, which we
    /// // later trigger the atomic processing of.
    /// unprocessed.insert(b"k3", b"ligers").unwrap();
    ///
    /// // Atomically process the new item and move it
    /// // between `Tree`s.
    /// (&unprocessed, &processed).transaction(|(unprocessed, processed)| {
    ///     let unprocessed_item = unprocessed.remove(b"k3")?.unwrap();
    ///     let mut processed_item = b"yappin' ".to_vec();
    ///     processed_item.extend_from_slice(&unprocessed_item);
    ///     processed.insert(b"k3", processed_item)?;
    ///     Ok(())
    /// }).unwrap();
    ///
    /// assert_eq!(unprocessed.get(b"k3").unwrap(), None);
    /// assert_eq!(&processed.get(b"k3").unwrap().unwrap(), b"yappin' ligers");
    /// ```
    pub fn transaction<F, R>(&self, f: F) -> TransactionResult<R>
    where
        F: Fn(&TransactionalTree<'_>) -> TransactionResult<R>,
    {
        <&Self as Transactional>::transaction(&self, f)
    }

    /// Create a new batched update that can be
    /// atomically applied.
    ///
    /// It is possible to apply a `Batch` in a transaction
    /// as well, which is the way you can apply a `Batch`
    /// to multiple `Tree`s atomically.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::{Db, Batch};
    ///
    /// let db = Db::open("batch_db").unwrap();
    /// db.insert("key_0", "val_0").unwrap();
    ///
    /// let mut batch = Batch::default();
    /// batch.insert("key_a", "val_a");
    /// batch.insert("key_b", "val_b");
    /// batch.insert("key_c", "val_c");
    /// batch.remove("key_0");
    ///
    /// db.apply_batch(batch).unwrap();
    /// // key_0 no longer exists, and key_a, key_b, and key_c
    /// // now do exist.
    /// ```
    pub fn apply_batch(&self, batch: Batch) -> Result<()> {
        let _ = self.concurrency_control.write();
        self.apply_batch_inner(batch)
    }

    pub(crate) fn apply_batch_inner(&self, batch: Batch) -> Result<()> {
        let peg = self.context.pin_log()?;
        for (k, v_opt) in batch.writes {
            if let Some(v) = v_opt {
                self.insert_inner(k, v)?;
            } else {
                self.remove_inner(k)?;
            }
        }

        // when the peg drops, it ensures all updates
        // written to the log since its creation are
        // recovered atomically
        peg.seal_batch()
    }

    /// Retrieve a value from the `Tree` if it exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::{ConfigBuilder, Db, IVec};
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let t = Db::start(config).unwrap();
    ///
    /// t.insert(&[0], vec![0]).unwrap();
    /// assert_eq!(t.get(&[0]), Ok(Some(IVec::from(vec![0]))));
    /// assert_eq!(t.get(&[1]), Ok(None));
    /// ```
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<IVec>> {
        let _ = self.concurrency_control.read();
        self.get_inner(key)
    }

    pub(crate) fn get_inner<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> Result<Option<IVec>> {
        let _measure = Measure::new(&M.tree_get);

        trace!("getting key {:?}", key.as_ref());

        let guard = pin();

        let View { node, .. } = self.node_for_key(key.as_ref(), &guard)?;

        let pair = node.leaf_pair_for_key(key.as_ref());
        let val = pair.map(|kv| kv.1.clone());

        Ok(val)
    }

    #[doc(hidden)]
    #[deprecated(since = "0.24.2", note = "replaced by `Tree::remove`")]
    pub fn del<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<IVec>> {
        self.remove(key)
    }

    /// Delete a value, returning the old value if it existed.
    ///
    /// # Examples
    ///
    /// ```
    /// let config = sled::ConfigBuilder::new().temporary(true).build();
    /// let t = sled::Db::start(config).unwrap();
    /// t.insert(&[1], vec![1]);
    /// assert_eq!(t.remove(&[1]), Ok(Some(sled::IVec::from(vec![1]))));
    /// assert_eq!(t.remove(&[1]), Ok(None));
    /// ```
    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<IVec>> {
        let _ = self.concurrency_control.read();
        self.remove_inner(key)
    }

    pub(crate) fn remove_inner<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> Result<Option<IVec>> {
        let _measure = Measure::new(&M.tree_del);

        trace!("removing key {:?}", key.as_ref());

        if self.context.read_only {
            return Ok(None);
        }

        loop {
            let guard = pin();

            let View { ptr, pid, node, .. } =
                self.node_for_key(key.as_ref(), &guard)?;

            let mut subscriber_reservation = self.subscriptions.reserve(&key);

            let (encoded_key, existing_val) =
                if let Some((k, v)) = node.leaf_pair_for_key(key.as_ref()) {
                    (k.clone(), Some(v.clone()))
                } else {
                    let encoded_key = prefix_encode(&node.lo, key.as_ref());
                    let encoded_val = None;
                    (encoded_key, encoded_val)
                };

            let frag = Frag::Del(encoded_key);

            let link =
                self.context
                    .pagecache
                    .link(pid, ptr.clone(), frag, &guard)?;

            if link.is_ok() {
                // success
                if let Some(res) = subscriber_reservation.take() {
                    let event =
                        subscription::Event::Remove(key.as_ref().into());

                    res.complete(event);
                }

                return Ok(existing_val);
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
    /// let config = sled::ConfigBuilder::new().temporary(true).build();
    /// let t = sled::Db::start(config).unwrap();
    ///
    /// // unique creation
    /// assert_eq!(t.cas(&[1], None as Option<&[u8]>, Some(&[10])), Ok(Ok(())));
    ///
    /// // conditional modification
    /// assert_eq!(t.cas(&[1], Some(&[10]), Some(&[20])), Ok(Ok(())));
    ///
    /// // conditional deletion
    /// assert_eq!(t.cas(&[1], Some(&[20]), None as Option<&[u8]>), Ok(Ok(())));
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

        let _ = self.concurrency_control.read();

        if self.context.read_only {
            return Err(Error::Unsupported(
                "can not perform a cas on a read-only Tree".into(),
            ));
        }

        let new = new.map(IVec::from);

        // we need to retry caps until old != cur, since just because
        // cap fails it doesn't mean our value was changed.
        loop {
            let guard = pin();
            let View { ptr, pid, node, .. } =
                self.node_for_key(key.as_ref(), &guard)?;

            let (encoded_key, current_value) =
                if let Some((k, v)) = node.leaf_pair_for_key(key.as_ref()) {
                    (k.clone(), Some(v.clone()))
                } else {
                    let k = prefix_encode(&node.lo, key.as_ref());
                    let old_v = None;
                    (k, old_v)
                };

            let matches = match (&old, &current_value) {
                (None, None) => true,
                (Some(ref o), Some(ref c)) => o.as_ref() == &**c,
                _ => false,
            };

            if !matches {
                return Ok(Err(current_value));
            }

            let mut subscriber_reservation = self.subscriptions.reserve(&key);

            let frag = if let Some(ref new) = new {
                Frag::Set(encoded_key, new.clone())
            } else {
                Frag::Del(encoded_key)
            };
            let link = self.context.pagecache.link(pid, ptr, frag, &guard)?;

            if link.is_ok() {
                if let Some(res) = subscriber_reservation.take() {
                    let event = if let Some(new) = new {
                        subscription::Event::Insert(key.as_ref().into(), new)
                    } else {
                        subscription::Event::Remove(key.as_ref().into())
                    };

                    res.complete(event);
                }

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
    /// use std::convert::TryInto;
    /// use sled::{ConfigBuilder, Error, IVec};
    ///
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let tree = sled::Db::start(config).unwrap();
    ///
    /// fn u64_to_ivec(number: u64) -> IVec {
    ///     IVec::from(number.to_be_bytes().to_vec())
    /// }
    ///
    /// let zero = u64_to_ivec(0);
    /// let one = u64_to_ivec(1);
    /// let two = u64_to_ivec(2);
    /// let three = u64_to_ivec(3);
    ///
    /// fn increment(old: Option<&[u8]>) -> Option<Vec<u8>> {
    ///     let number = match old {
    ///         Some(bytes) => {
    ///             let array: [u8; 8] = bytes.try_into().unwrap();
    ///             let number = u64::from_be_bytes(array);
    ///             number + 1
    ///         },
    ///         None => 0,
    ///     };
    ///
    ///     Some(number.to_be_bytes().to_vec())
    /// }
    ///
    /// assert_eq!(tree.update_and_fetch("counter", increment), Ok(Some(zero)));
    /// assert_eq!(tree.update_and_fetch("counter", increment), Ok(Some(one)));
    /// assert_eq!(tree.update_and_fetch("counter", increment), Ok(Some(two)));
    /// assert_eq!(tree.update_and_fetch("counter", increment), Ok(Some(three)));
    /// ```
    pub fn update_and_fetch<K, V, F>(
        &self,
        key: K,
        mut f: F,
    ) -> Result<Option<IVec>>
    where
        K: AsRef<[u8]>,
        F: FnMut(Option<&[u8]>) -> Option<V>,
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
    /// use std::convert::TryInto;
    /// use sled::{ConfigBuilder, Error, IVec};
    ///
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let tree = sled::Db::start(config).unwrap();
    ///
    /// fn u64_to_ivec(number: u64) -> IVec {
    ///     IVec::from(number.to_be_bytes().to_vec())
    /// }
    ///
    /// let zero = u64_to_ivec(0);
    /// let one = u64_to_ivec(1);
    /// let two = u64_to_ivec(2);
    ///
    /// fn increment(old: Option<&[u8]>) -> Option<Vec<u8>> {
    ///     let number = match old {
    ///         Some(bytes) => {
    ///             let array: [u8; 8] = bytes.try_into().unwrap();
    ///             let number = u64::from_be_bytes(array);
    ///             number + 1
    ///         },
    ///         None => 0,
    ///     };
    ///
    ///     Some(number.to_be_bytes().to_vec())
    /// }
    ///
    /// assert_eq!(tree.fetch_and_update("counter", increment), Ok(None));
    /// assert_eq!(tree.fetch_and_update("counter", increment), Ok(Some(zero)));
    /// assert_eq!(tree.fetch_and_update("counter", increment), Ok(Some(one)));
    /// assert_eq!(tree.fetch_and_update("counter", increment), Ok(Some(two)));
    /// ```
    pub fn fetch_and_update<K, V, F>(
        &self,
        key: K,
        mut f: F,
    ) -> Result<Option<IVec>>
    where
        K: AsRef<[u8]>,
        F: FnMut(Option<&[u8]>) -> Option<V>,
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
    ///     tree.insert(vec![0], vec![1]).unwrap();
    /// });
    ///
    /// // events is a blocking `Iterator` over `Event`s
    /// for event in events.take(1) {
    ///     match event {
    ///         Event::Insert(key, value) => assert_eq!(key.as_ref(), &[0]),
    ///         Event::Remove(key) => {}
    ///     }
    /// }
    ///
    /// thread.join().unwrap();
    /// ```
    pub fn watch_prefix(&self, prefix: Vec<u8>) -> Subscriber {
        self.subscriptions.register(prefix)
    }

    /// Synchronously flushes all dirty IO buffers and calls
    /// fsync. If this succeeds, it is guaranteed that all
    /// previous writes will be recovered if the system
    /// crashes. Returns the number of bytes flushed during
    /// this call.
    ///
    /// Flushing can take quite a lot of time, and you should
    /// measure the performance impact of using it on
    /// realistic sustained workloads running on realistic
    /// hardware.
    pub fn flush(&self) -> Result<usize> {
        self.context.pagecache.flush()
    }

    /// Asynchronously flushes all dirty IO buffers
    /// and calls fsync. If this succeeds, it is
    /// guaranteed that all previous writes will
    /// be recovered if the system crashes. Returns
    /// the number of bytes flushed during this call.
    ///
    /// Flushing can take quite a lot of time, and you
    /// should measure the performance impact of
    /// using it on realistic sustained workloads
    /// running on realistic hardware.
    pub fn flush_async(
        &self,
    ) -> impl std::future::Future<Output = Result<usize>> {
        let pagecache = self.context.pagecache.clone();
        threadpool::spawn(move || pagecache.flush())
    }

    /// Returns `true` if the `Tree` contains a value for
    /// the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// let config = sled::ConfigBuilder::new().temporary(true).build();
    /// let t = sled::Db::start(config).unwrap();
    ///
    /// t.insert(&[0], vec![0]).unwrap();
    /// assert!(t.contains_key(&[0]).unwrap());
    /// assert!(!t.contains_key(&[1]).unwrap());
    /// ```
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> Result<bool> {
        self.get(key).map(|v| v.is_some())
    }

    /// Retrieve the key and value before the provided key,
    /// if one exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::{ConfigBuilder, Db, IVec};
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let tree = Db::start(config).unwrap();
    ///
    /// for i in 0..10 {
    ///     tree.insert(&[i], vec![i]).expect("should write successfully");
    /// }
    ///
    /// assert_eq!(tree.get_lt(&[]), Ok(None));
    /// assert_eq!(tree.get_lt(&[0]), Ok(None));
    /// assert_eq!(tree.get_lt(&[1]), Ok(Some((IVec::from(&[0]), IVec::from(&[0])))));
    /// assert_eq!(tree.get_lt(&[9]), Ok(Some((IVec::from(&[8]), IVec::from(&[8])))));
    /// assert_eq!(tree.get_lt(&[10]), Ok(Some((IVec::from(&[9]), IVec::from(&[9])))));
    /// assert_eq!(tree.get_lt(&[255]), Ok(Some((IVec::from(&[9]), IVec::from(&[9])))));
    /// ```
    pub fn get_lt<K>(&self, key: K) -> Result<Option<(IVec, IVec)>>
    where
        K: AsRef<[u8]>,
    {
        let _measure = Measure::new(&M.tree_get);
        let _ = self.concurrency_control.read();
        self.range(..key).next_back().transpose()
    }

    /// Retrieve the next key and value from the `Tree` after the
    /// provided key.
    ///
    /// # Note
    /// The order follows the Ord implementation for `Vec<u8>`:
    ///
    /// `[] < [0] < [255] < [255, 0] < [255, 255] ...`
    ///
    /// To retain the ordering of numerical types use big endian reprensentation
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::{ConfigBuilder, Db, IVec};
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let tree = Db::start(config).unwrap();
    ///
    /// for i in 0..10 {
    ///     tree.insert(&[i], vec![i]).expect("should write successfully");
    /// }
    ///
    /// assert_eq!(tree.get_gt(&[]), Ok(Some((IVec::from(&[0]), IVec::from(&[0])))));
    /// assert_eq!(tree.get_gt(&[0]), Ok(Some((IVec::from(&[1]), IVec::from(&[1])))));
    /// assert_eq!(tree.get_gt(&[1]), Ok(Some((IVec::from(&[2]), IVec::from(&[2])))));
    /// assert_eq!(tree.get_gt(&[8]), Ok(Some((IVec::from(&[9]), IVec::from(&[9])))));
    /// assert_eq!(tree.get_gt(&[9]), Ok(None));
    ///
    /// tree.insert(500u16.to_be_bytes(), vec![10] );
    /// assert_eq!(tree.get_gt(&499u16.to_be_bytes()),
    ///            Ok(Some((IVec::from(&500u16.to_be_bytes()), IVec::from(&[10])))));
    /// ```
    pub fn get_gt<K>(&self, key: K) -> Result<Option<(IVec, IVec)>>
    where
        K: AsRef<[u8]>,
    {
        let _measure = Measure::new(&M.tree_get);
        let _ = self.concurrency_control.read();
        self.range((ops::Bound::Excluded(key), ops::Bound::Unbounded))
            .next()
            .transpose()
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
    /// use sled::{ConfigBuilder, Db, IVec};
    ///
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
    /// let config = ConfigBuilder::new()
    ///   .temporary(true)
    ///   .build();
    ///
    /// let tree = Db::start(config).unwrap();
    /// tree.set_merge_operator(concatenate_merge);
    ///
    /// let k = b"k1";
    ///
    /// tree.insert(k, vec![0]);
    /// tree.merge(k, vec![1]);
    /// tree.merge(k, vec![2]);
    /// assert_eq!(tree.get(k), Ok(Some(IVec::from(vec![0, 1, 2]))));
    ///
    /// // Replace previously merged data. The merge function will not be called.
    /// tree.insert(k, vec![3]);
    /// assert_eq!(tree.get(k), Ok(Some(IVec::from(vec![3]))));
    ///
    /// // Merges on non-present values will cause the merge function to be called
    /// // with `old_value == None`. If the merge function returns something (which it
    /// // does, in this case) a new value will be inserted.
    /// tree.remove(k);
    /// tree.merge(k, vec![4]);
    /// assert_eq!(tree.get(k), Ok(Some(IVec::from(vec![4]))));
    /// ```
    pub fn merge<K, V>(&self, key: K, value: V) -> Result<Option<IVec>>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let _ = self.concurrency_control.read();
        self.merge_inner(key, value)
    }

    pub(crate) fn merge_inner<K, V>(
        &self,
        key: K,
        value: V,
    ) -> Result<Option<IVec>>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        trace!("merging key {:?}", key.as_ref());
        let _measure = Measure::new(&M.tree_merge);

        if self.context.read_only {
            return Err(Error::Unsupported(
                "the database is in read-only mode".to_owned(),
            ));
        }

        let merge_operator_opt = self.merge_operator.read();

        if merge_operator_opt.is_none() {
            return Err(Error::Unsupported(
                "must set a merge operator on this Tree \
                 before calling merge by calling \
                 Tree::set_merge_operator"
                    .to_owned(),
            ));
        }

        let merge_operator = merge_operator_opt.unwrap();

        loop {
            let guard = pin();
            let View { ptr, pid, node, .. } =
                self.node_for_key(key.as_ref(), &guard)?;

            let (encoded_key, current_value) =
                if let Some((k, v)) = node.leaf_pair_for_key(key.as_ref()) {
                    (k.clone(), Some(v.clone()))
                } else {
                    let k = prefix_encode(&node.lo, key.as_ref());
                    let old_v = None;
                    (k, old_v)
                };

            let tmp = current_value.as_ref().map(AsRef::as_ref);
            let new = merge_operator(key.as_ref(), tmp, value.as_ref())
                .map(IVec::from);

            let mut subscriber_reservation = self.subscriptions.reserve(&key);

            let frag = if let Some(ref new) = new {
                Frag::Set(encoded_key, new.clone())
            } else {
                Frag::Del(encoded_key)
            };
            let link = self.context.pagecache.link(pid, ptr, frag, &guard)?;

            if link.is_ok() {
                if let Some(res) = subscriber_reservation.take() {
                    let event = if let Some(new) = &new {
                        subscription::Event::Insert(
                            key.as_ref().into(),
                            new.clone(),
                        )
                    } else {
                        subscription::Event::Remove(key.as_ref().into())
                    };

                    res.complete(event);
                }

                return Ok(new);
            }
            M.tree_looped();
        }
    }

    /// Sets a merge operator for use with the `merge` function.
    ///
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
    /// use sled::{ConfigBuilder, Db, IVec};
    ///
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
    /// let config = ConfigBuilder::new()
    ///   .temporary(true)
    ///   .build();
    ///
    /// let tree = Db::start(config).unwrap();
    /// tree.set_merge_operator(concatenate_merge);
    ///
    /// let k = b"k1";
    ///
    /// tree.insert(k, vec![0]);
    /// tree.merge(k, vec![1]);
    /// tree.merge(k, vec![2]);
    /// assert_eq!(tree.get(k), Ok(Some(IVec::from(vec![0, 1, 2]))));
    ///
    /// // Replace previously merged data. The merge function will not be called.
    /// tree.insert(k, vec![3]);
    /// assert_eq!(tree.get(k), Ok(Some(IVec::from(vec![3]))));
    ///
    /// // Merges on non-present values will cause the merge function to be called
    /// // with `old_value == None`. If the merge function returns something (which it
    /// // does, in this case) a new value will be inserted.
    /// tree.remove(k);
    /// tree.merge(k, vec![4]);
    /// assert_eq!(tree.get(k), Ok(Some(IVec::from(vec![4]))));
    /// ```
    pub fn set_merge_operator(&self, merge_operator: MergeOperator) {
        let mut mo_write = self.merge_operator.write();
        *mo_write = Some(merge_operator);
    }

    /// Create a double-ended iterator over the tuples of keys and
    /// values in this tree.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::{ConfigBuilder, Db, IVec};
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let t = Db::start(config).unwrap();
    /// t.insert(&[1], vec![10]);
    /// t.insert(&[2], vec![20]);
    /// t.insert(&[3], vec![30]);
    /// let mut iter = t.iter();
    /// assert_eq!(iter.next().unwrap(), Ok((IVec::from(&[1]), IVec::from(&[10]))));
    /// assert_eq!(iter.next().unwrap(), Ok((IVec::from(&[2]), IVec::from(&[20]))));
    /// assert_eq!(iter.next().unwrap(), Ok((IVec::from(&[3]), IVec::from(&[30]))));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn iter(&self) -> Iter<'_> {
        self.range::<Vec<u8>, _>(..)
    }

    /// Create a double-ended iterator over tuples of keys and values,
    /// where the keys fall within the specified range.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::{ConfigBuilder, Db, IVec};
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let t = Db::start(config).unwrap();
    ///
    /// t.insert(&[0], vec![0]).unwrap();
    /// t.insert(&[1], vec![10]).unwrap();
    /// t.insert(&[2], vec![20]).unwrap();
    /// t.insert(&[3], vec![30]).unwrap();
    /// t.insert(&[4], vec![40]).unwrap();
    /// t.insert(&[5], vec![50]).unwrap();
    ///
    /// let start: &[u8] = &[2];
    /// let end: &[u8] = &[4];
    /// let mut r = t.range(start..end);
    /// assert_eq!(r.next().unwrap(), Ok((IVec::from(&[2]), IVec::from(&[20]))));
    /// assert_eq!(r.next().unwrap(), Ok((IVec::from(&[3]), IVec::from(&[30]))));
    /// assert_eq!(r.next(), None);
    ///
    /// let mut r = t.range(start..end).rev();
    /// assert_eq!(r.next().unwrap(), Ok((IVec::from(&[3]), IVec::from(&[30]))));
    /// assert_eq!(r.next().unwrap(), Ok((IVec::from(&[2]), IVec::from(&[20]))));
    /// assert_eq!(r.next(), None);
    /// ```
    pub fn range<K, R>(&self, range: R) -> Iter<'_>
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
    {
        let _measure = Measure::new(&M.tree_scan);

        let lo = match range.start_bound() {
            ops::Bound::Included(ref start) => {
                ops::Bound::Included(IVec::from(start.as_ref()))
            }
            ops::Bound::Excluded(ref start) => {
                ops::Bound::Excluded(IVec::from(start.as_ref()))
            }
            ops::Bound::Unbounded => ops::Bound::Included(IVec::from(&[])),
        };

        let hi = match range.end_bound() {
            ops::Bound::Included(ref end) => {
                ops::Bound::Included(IVec::from(end.as_ref()))
            }
            ops::Bound::Excluded(ref end) => {
                ops::Bound::Excluded(IVec::from(end.as_ref()))
            }
            ops::Bound::Unbounded => ops::Bound::Unbounded,
        };

        Iter {
            tree: &self,
            hi,
            lo,
            cached_node: None,
            guard: pin(),
            going_forward: true,
        }
    }

    /// Create an iterator over tuples of keys and values,
    /// where the all the keys starts with the given prefix.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::{ConfigBuilder, Db, IVec};
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let t = Db::start(config).unwrap();
    ///
    /// t.insert(&[0, 0, 0], vec![0, 0, 0]).unwrap();
    /// t.insert(&[0, 0, 1], vec![0, 0, 1]).unwrap();
    /// t.insert(&[0, 0, 2], vec![0, 0, 2]).unwrap();
    /// t.insert(&[0, 0, 3], vec![0, 0, 3]).unwrap();
    /// t.insert(&[0, 1, 0], vec![0, 1, 0]).unwrap();
    /// t.insert(&[0, 1, 1], vec![0, 1, 1]).unwrap();
    ///
    /// let prefix: &[u8] = &[0, 0];
    /// let mut r = t.scan_prefix(prefix);
    /// assert_eq!(r.next(), Some(Ok((IVec::from(&[0, 0, 0]), IVec::from(&[0, 0, 0])))));
    /// assert_eq!(r.next(), Some(Ok((IVec::from(&[0, 0, 1]), IVec::from(&[0, 0, 1])))));
    /// assert_eq!(r.next(), Some(Ok((IVec::from(&[0, 0, 2]), IVec::from(&[0, 0, 2])))));
    /// assert_eq!(r.next(), Some(Ok((IVec::from(&[0, 0, 3]), IVec::from(&[0, 0, 3])))));
    /// assert_eq!(r.next(), None);
    /// ```
    pub fn scan_prefix<P>(&self, prefix: P) -> Iter<'_>
    where
        P: AsRef<[u8]>,
    {
        let prefix = prefix.as_ref();
        let mut upper = prefix.to_vec();

        while let Some(last) = upper.pop() {
            if last < u8::max_value() {
                upper.push(last + 1);
                return self.range(prefix..&upper);
            }
        }

        self.range(prefix..)
    }

    /// Atomically removes the maximum item in the `Tree` instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::{ConfigBuilder, Db, IVec};
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let t = Db::start(config).unwrap();
    ///
    /// t.insert(&[0], vec![0]).unwrap();
    /// t.insert(&[1], vec![10]).unwrap();
    /// t.insert(&[2], vec![20]).unwrap();
    /// t.insert(&[3], vec![30]).unwrap();
    /// t.insert(&[4], vec![40]).unwrap();
    /// t.insert(&[5], vec![50]).unwrap();
    ///
    /// assert_eq!(&t.pop_max().unwrap().unwrap().0, &[5]);
    /// assert_eq!(&t.pop_max().unwrap().unwrap().0, &[4]);
    /// assert_eq!(&t.pop_max().unwrap().unwrap().0, &[3]);
    /// assert_eq!(&t.pop_max().unwrap().unwrap().0, &[2]);
    /// assert_eq!(&t.pop_max().unwrap().unwrap().0, &[1]);
    /// assert_eq!(&t.pop_max().unwrap().unwrap().0, &[0]);
    /// assert_eq!(t.pop_max().unwrap(), None);
    /// ```
    pub fn pop_max(&self) -> Result<Option<(IVec, IVec)>> {
        loop {
            if let Some(first_res) = self.iter().next_back() {
                let first = first_res?;
                if self
                    .cas(&first.0, Some(&first.1), None as Option<&[u8]>)
                    .is_ok()
                {
                    return Ok(Some(first));
                }
            // try again
            } else {
                return Ok(None);
            }
        }
    }

    /// Atomically removes the minimum item in the `Tree` instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::{ConfigBuilder, Db, IVec};
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let t = Db::start(config).unwrap();
    ///
    /// t.insert(&[0], vec![0]).unwrap();
    /// t.insert(&[1], vec![10]).unwrap();
    /// t.insert(&[2], vec![20]).unwrap();
    /// t.insert(&[3], vec![30]).unwrap();
    /// t.insert(&[4], vec![40]).unwrap();
    /// t.insert(&[5], vec![50]).unwrap();
    ///
    /// assert_eq!(&t.pop_min().unwrap().unwrap().0, &[0]);
    /// assert_eq!(&t.pop_min().unwrap().unwrap().0, &[1]);
    /// assert_eq!(&t.pop_min().unwrap().unwrap().0, &[2]);
    /// assert_eq!(&t.pop_min().unwrap().unwrap().0, &[3]);
    /// assert_eq!(&t.pop_min().unwrap().unwrap().0, &[4]);
    /// assert_eq!(&t.pop_min().unwrap().unwrap().0, &[5]);
    /// assert_eq!(t.pop_min().unwrap(), None);
    /// ```
    pub fn pop_min(&self) -> Result<Option<(IVec, IVec)>> {
        loop {
            if let Some(first_res) = self.iter().next() {
                let first = first_res?;
                if self
                    .cas(&first.0, Some(&first.1), None as Option<&[u8]>)
                    .is_ok()
                {
                    return Ok(Some(first));
                }
            // try again
            } else {
                return Ok(None);
            }
        }
    }

    /// Returns the number of elements in this tree.
    ///
    /// Beware: performs a full O(n) scan under the hood.
    ///
    /// # Examples
    ///
    /// ```
    /// let config = sled::ConfigBuilder::new().temporary(true).build();
    /// let t = sled::Db::start(config).unwrap();
    /// t.insert(b"a", vec![0]);
    /// t.insert(b"b", vec![1]);
    /// assert_eq!(t.len(), 2);
    /// ```
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
        for k in self.iter().keys() {
            let key = k?;
            self.remove(key)?;
        }
        Ok(())
    }

    /// Returns the name of the tree.
    pub fn name(&self) -> Vec<u8> {
        self.tree_id.clone()
    }

    fn split_node<'g>(
        &self,
        node_view: &View<'g>,
        parent_view: &Option<View<'g>>,
        root_pid: PageId,
        guard: &'g Guard,
    ) -> Result<()> {
        trace!("splitting node {}", node_view.pid);
        // split node
        let (mut lhs, rhs) = node_view.node.clone().split();
        let rhs_lo = rhs.lo.clone();

        // install right side
        let (rhs_pid, rhs_ptr) =
            self.context.pagecache.allocate(Frag::Base(rhs), guard)?;

        // replace node, pointing next to installed right
        lhs.next = Some(rhs_pid);
        let replace = self.context.pagecache.replace(
            node_view.pid,
            node_view.ptr.clone(),
            Frag::Base(lhs),
            guard,
        )?;
        M.tree_child_split_attempt();
        if replace.is_err() {
            // if we failed, don't follow through with the
            // parent split or root hoist.
            self.context
                .pagecache
                .free(rhs_pid, rhs_ptr, guard)?
                .expect("could not free allocated page");
            return Ok(());
        }
        M.tree_child_split_success();

        // either install parent split or hoist root
        if let Some(parent_view) = parent_view {
            M.tree_parent_split_attempt();
            let mut parent = parent_view.node.clone();
            let split_applied = parent.parent_split(&rhs_lo, rhs_pid);

            if !split_applied {
                // due to deep races, it's possible for the
                // parent to already have a node for this lo key.
                // if this is the case, we can skip the parent split
                // because it's probably going to fail anyway.
                return Ok(());
            }

            let replace = self.context.pagecache.replace(
                parent_view.pid,
                parent_view.ptr.clone(),
                Frag::Base(parent),
                guard,
            )?;
            if replace.is_ok() {
                M.tree_parent_split_success();
            } else {
                // Parent splits are an optimization
                // so we don't need to care if we
                // failed.
            }
        } else {
            M.tree_root_split_attempt();
            if self.root_hoist(root_pid, rhs_pid, &rhs_lo, guard)? {
                M.tree_root_split_success();
            }
        }

        Ok(())
    }

    fn root_hoist<'g>(
        &self,
        from: PageId,
        to: PageId,
        at: &IVec,
        guard: &'g Guard,
    ) -> Result<bool> {
        // hoist new root, pointing to lhs & rhs
        let root_lo = b"";
        let mut new_root_vec = vec![];
        new_root_vec.push((vec![0].into(), from));

        let encoded_at = prefix_encode(root_lo, &*at);
        new_root_vec.push((encoded_at, to));

        let new_root = Frag::Base(Node {
            data: Data::Index(new_root_vec),
            next: None,
            lo: vec![].into(),
            hi: vec![].into(),
            merging_child: None,
            merging: false,
        });

        let (new_root_pid, new_root_ptr) =
            self.context.pagecache.allocate(new_root, guard)?;
        debug!("allocated pid {} in root_hoist", new_root_pid);

        debug_delay();

        let cas = self.context.pagecache.cas_root_in_meta(
            &self.tree_id,
            Some(from),
            Some(new_root_pid),
            guard,
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

            Ok(true)
        } else {
            debug!(
                "root hoist from {} to {} failed: {:?}",
                from, new_root_pid, cas
            );
            self.context
                .pagecache
                .free(new_root_pid, new_root_ptr, guard)?
                .expect("could not free allocated page");

            Ok(false)
        }
    }

    pub(crate) fn view_for_pid<'g>(
        &self,
        pid: PageId,
        guard: &'g Guard,
    ) -> Result<Option<View<'g>>> {
        loop {
            let frag_opt = self.context.pagecache.get(pid, guard)?;
            if let Some((tree_ptr, Frag::Base(ref leaf), size)) = &frag_opt {
                let view = View {
                    node: leaf,
                    ptr: tree_ptr.clone(),
                    pid,
                    size: *size,
                };
                if leaf.merging_child.is_some() {
                    self.merge_node(&view, leaf.merging_child.unwrap(), guard)?;
                } else {
                    return Ok(Some(view));
                }
            } else {
                return Ok(None);
            }
        }
    }

    /// returns the traversal path, completing any observed
    /// partially complete splits or merges along the way.
    pub(crate) fn node_for_key<'g, K>(
        &self,
        key: K,
        guard: &'g Guard,
    ) -> Result<View<'g>>
    where
        K: AsRef<[u8]>,
    {
        #[cfg(feature = "lock_free_delays")]
        const MAX_LOOPS: usize = usize::max_value();

        #[cfg(not(feature = "lock_free_delays"))]
        const MAX_LOOPS: usize = 1_000_000;

        let _measure = Measure::new(&M.tree_traverse);

        let mut cursor = self.root.load(SeqCst);
        let mut root_pid = cursor;
        let mut parent_view = None;
        let mut unsplit_parent = None;
        let mut took_leftmost_branch = false;

        macro_rules! retry {
            () => {
                trace!(
                    "retrying at line {} when cursor was {}",
                    line!(),
                    cursor
                );
                cursor = self.root.load(SeqCst);
                root_pid = cursor;
                parent_view = None;
                unsplit_parent = None;
                took_leftmost_branch = false;
                continue;
            };
        }

        for _ in 0..MAX_LOOPS {
            if cursor == u64::max_value() {
                // this collection has been explicitly removed
                return Err(Error::CollectionNotFound(self.tree_id.clone()));
            }

            let node_opt = self.view_for_pid(cursor, guard)?;

            let view = if let Some(view) = node_opt {
                view
            } else {
                retry!();
            };

            // When we encounter a merge intention, we collaboratively help out
            if view.merging_child.is_some() {
                self.merge_node(
                    &view,
                    view.node.merging_child.unwrap(),
                    guard,
                )?;
                retry!();
            } else if view.merging {
                // we missed the parent merge intention due to a benign race,
                // so go around again and try to help out if necessary
                retry!();
            }

            let overshot = key.as_ref() < view.lo.as_ref();
            let undershot =
                key.as_ref() >= view.hi.as_ref() && !view.hi.is_empty();

            if overshot {
                // merge interfered, reload root and retry
                retry!();
            }

            if view.should_split() {
                self.split_node(&view, &parent_view, root_pid, guard)?;
                retry!();
            }

            if undershot {
                // half-complete split detect & completion
                cursor = view.next.expect(
                    "if our hi bound is not Inf (inity), \
                     we should have a right sibling",
                );
                if unsplit_parent.is_none() && parent_view.is_some() {
                    unsplit_parent = parent_view.clone();
                } else if parent_view.is_none() && view.lo.is_empty() {
                    assert_eq!(view.pid, root_pid);
                    // we have found a partially-split root
                    if self.root_hoist(
                        root_pid,
                        view.next.unwrap(),
                        &view.hi,
                        guard,
                    )? {
                        M.tree_root_split_success();
                        retry!();
                    }
                }
                continue;
            } else if let Some(unsplit_parent) = unsplit_parent.take() {
                // we have found the proper page for
                // our cooperative parent split
                let mut parent = unsplit_parent.node.clone();
                let split_applied =
                    parent.parent_split(view.lo.as_ref(), cursor);

                if !split_applied {
                    // due to deep races, it's possible for the
                    // parent to already have a node for this lo key.
                    // if this is the case, we can skip the parent split
                    // because it's probably going to fail anyway.
                    retry!();
                }

                M.tree_parent_split_attempt();
                let replace = self.context.pagecache.replace(
                    unsplit_parent.pid,
                    unsplit_parent.ptr.clone(),
                    Frag::Base(parent),
                    guard,
                )?;
                if replace.is_ok() {
                    M.tree_parent_split_success();
                }
            }

            // detect whether a node is mergable, and begin
            // the merge process.
            // NB we can never begin merging a node that is
            // the leftmost child of an index, because it
            // would be merged into a different index, which
            // would add considerable complexity to this already
            // fairly complex implementation.
            if view.should_merge() && !took_leftmost_branch {
                if let Some(ref mut parent) = parent_view {
                    assert!(parent.node.merging_child.is_none());
                    if parent.node.can_merge_child() {
                        let frag = Frag::ParentMergeIntention(cursor);

                        let link = self.context.pagecache.link(
                            parent.pid,
                            parent.ptr.clone(),
                            frag,
                            guard,
                        )?;

                        if let Ok(new_parent_ptr) = link {
                            parent.ptr = new_parent_ptr;
                            self.merge_node(&parent, cursor, guard)?;
                            retry!();
                        }
                    }
                }
            }

            if view.data.is_index() {
                let next = view.index_next_node(key.as_ref());
                took_leftmost_branch = next.0 == 0;
                parent_view = Some(view);
                cursor = next.1;
            } else {
                assert!(!overshot && !undershot);
                return Ok(view);
            }
        }
        panic!(
            "cannot find pid {} in view_for_key, looking for key {:?} in tree",
            cursor,
            key.as_ref(),
        );
    }

    pub(crate) fn merge_node<'g>(
        &self,
        parent_view: &View<'g>,
        child_pid: PageId,
        guard: &'g Guard,
    ) -> Result<()> {
        // Get the child node and try to install a `MergeCap` frag.
        // In case we succeed, we break, otherwise we try from the start.
        let child_view = loop {
            let mut child_view = if let Some(child_view) =
                self.view_for_pid(child_pid, guard)?
            {
                child_view
            } else {
                // the child was already freed, meaning
                // somebody completed this whole loop already
                return Ok(());
            };

            if child_view.merging {
                trace!("child pid {} already merging", child_pid);
                break child_view;
            }

            let install_frag = self.context.pagecache.link(
                child_pid,
                child_view.ptr.clone(),
                Frag::ChildMergeCap,
                guard,
            )?;
            match install_frag {
                Ok(new_ptr) => {
                    trace!("child pid {} merge capped", child_pid);
                    child_view.ptr = new_ptr;
                    break child_view;
                }
                Err(Some((_, _))) => {
                    trace!(
                        "child pid {} merge cap failed, retrying",
                        child_pid
                    );
                    continue;
                }
                Err(None) => {
                    trace!("child pid {} already freed", child_pid);
                    return Ok(());
                }
            }
        };

        trace!(
            "merging child pid {} of parent pid {}",
            child_pid,
            parent_view.pid
        );

        let index = parent_view.node.data.index_ref().unwrap();
        let child_index =
            index.iter().position(|(_, pid)| pid == &child_pid).unwrap();

        assert_ne!(
            child_index, 0,
            "merging child must not be the \
             leftmost child of its parent"
        );

        let mut merge_index = child_index - 1;

        // we assume caller only merges when
        // the node to be merged is not the
        // leftmost child.
        let mut cursor_pid = index[merge_index].1;

        // searching for the left sibling to merge the target page into
        loop {
            // The only way this child could have been freed is if the original
            // merge has already been handled. Only in that case can this child
            // have been freed
            trace!(
                "cursor_pid is {} while looking for left sibling",
                cursor_pid
            );
            let cursor_view = if let Some(cursor_view) =
                self.view_for_pid(cursor_pid, guard)?
            {
                cursor_view
            } else {
                trace!(
                    "couldn't retrieve frags for freed \
                     (possibly outdated) prospective left \
                     sibling with pid {}",
                    cursor_pid
                );

                if merge_index == 0 {
                    trace!(
                        "failed to find any left sibling for \
                         merging pid {}, which means this merge \
                         must have already completed.",
                        child_pid
                    );
                    return Ok(());
                }

                merge_index -= 1;
                cursor_pid = index[merge_index].1;

                continue;
            };

            // This means that `cursor_node` is the node we want to replace
            if cursor_view.next == Some(child_pid) {
                trace!(
                    "found left sibling pid {} points to merging node pid {}",
                    cursor_view.pid,
                    child_pid
                );
                let cursor_node = cursor_view.node;
                let cursor_cas_key = cursor_view.ptr;

                let replacement = cursor_node.receive_merge(child_view.node);
                let replace = self.context.pagecache.replace(
                    cursor_pid,
                    cursor_cas_key,
                    Frag::Base(replacement),
                    guard,
                )?;
                match replace {
                    Ok(_) => {
                        trace!(
                            "merged node pid {} into left sibling pid {}",
                            child_pid,
                            cursor_pid
                        );
                        break;
                    }
                    Err(None) => {
                        trace!(
                            "failed to merge pid {} into \
                             pid {} since pid {} doesn't exist anymore",
                            child_pid,
                            cursor_pid,
                            cursor_pid
                        );
                        return Ok(());
                    }
                    Err(_) => {
                        trace!(
                            "failed to merge pid {} into \
                             pid {} due to cas failure",
                            child_pid,
                            cursor_pid
                        );
                        continue;
                    }
                }
            } else if cursor_view.hi >= child_view.lo {
                // we overshot the node being merged,
                trace!(
                    "cursor pid {} has hi key {:?}, which is \
                     >= merging child pid {}'s lo key of {:?}, breaking",
                    cursor_pid,
                    cursor_view.hi,
                    child_pid,
                    child_view.lo
                );
                break;
            } else {
                // In case we didn't find the child, we get the next cursor node
                if let Some(next) = cursor_view.next {
                    trace!(
                        "traversing from cursor pid {} to right sibling pid {}",
                        cursor_pid,
                        next
                    );
                    cursor_pid = next;
                } else {
                    trace!(
                        "hit the right side of the tree without finding \
                         a left sibling for merging child pid {}",
                        child_pid
                    );
                    break;
                }
            }
        }

        let mut parent_cas_key = parent_view.ptr.clone();

        trace!(
            "trying to install parent merge \
             confirmation of merged child pid {} for parent pid {}",
            child_pid,
            parent_view.pid
        );
        loop {
            let linked = self.context.pagecache.link(
                parent_view.pid,
                parent_cas_key,
                Frag::ParentMergeConfirm,
                guard,
            )?;
            match linked {
                Ok(_) => {
                    trace!(
                        "ParentMergeConfirm succeeded on parent pid {}, \
                         now freeing child pid {}",
                        parent_view.pid,
                        child_pid
                    );
                    break;
                }
                Err(None) => {
                    trace!(
                        "ParentMergeConfirm \
                         failed on (now freed) parent pid {}",
                        parent_view.pid
                    );
                    return Ok(());
                }
                Err(_) => {
                    let parent_view = if let Some(parent_view) =
                        self.view_for_pid(parent_view.pid, guard)?
                    {
                        trace!(
                            "failed to confirm merge \
                             on parent pid {}, trying again",
                            parent_view.pid
                        );
                        parent_view
                    } else {
                        trace!(
                            "failed to confirm merge \
                             on parent pid {}, which was freed",
                            parent_view.pid
                        );
                        return Ok(());
                    };

                    if parent_view.merging_child != Some(child_pid) {
                        trace!(
                            "someone else must have already \
                             completed the merge, and now the \
                             merging child for parent pid {} is {:?}",
                            parent_view.pid,
                            parent_view.merging_child
                        );
                        return Ok(());
                    }

                    parent_cas_key = parent_view.ptr;
                }
            }
        }

        match self
            .context
            .pagecache
            .free(child_pid, child_view.ptr, guard)?
        {
            Ok(_) => {
                // we freed it
                trace!("freed merged pid {}", child_pid);
            }
            Err(None) => {
                // someone else freed it
                trace!("someone else freed merged pid {}", child_pid);
            }
            Err(Some(_)) => {
                trace!(
                    "someone was able to reuse freed merged pid {}",
                    child_pid
                );
                // it was reused somehow after we
                // observed it as in the merging state
                panic!(
                    "somehow the merging child was reused \
                     before all threads that witnessed its previous \
                     merge have left their epoch"
                )
            }
        }

        trace!("finished with merge of pid {}", child_pid);
        Ok(())
    }

    // Remove all pages for this tree from the underlying
    // PageCache. This will leave orphans behind if
    // the tree crashes during gc.
    pub(crate) fn gc_pages(
        &self,
        mut leftmost_chain: Vec<PageId>,
    ) -> Result<()> {
        let guard = pin();

        while let Some(mut pid) = leftmost_chain.pop() {
            loop {
                let cursor_view =
                    if let Some(view) = self.view_for_pid(pid, &guard)? {
                        view
                    } else {
                        trace!("encountered Free node while GC'ing tree");
                        break;
                    };

                let ret = self.context.pagecache.free(
                    pid,
                    cursor_view.ptr.clone(),
                    &guard,
                )?;

                if ret.is_ok() {
                    let next_pid = if let Some(next_pid) = cursor_view.next {
                        next_pid
                    } else {
                        break;
                    };
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
        let guard = pin();

        let mut pid = self.root.load(SeqCst);
        let mut left_most = pid;
        let mut level = 0;

        f.write_str("Tree: \n\t")?;
        self.context.pagecache.fmt(f)?;
        f.write_str("\tlevel 0:\n")?;

        loop {
            let get_res = self.view_for_pid(pid, &guard);
            let node = match get_res {
                Ok(Some(ref view)) => view.node,
                broken => {
                    error!(
                        "Tree::fmt failed to read node {} \
                         that has been freed: {:?}",
                        pid, broken
                    );
                    break;
                }
            };

            write!(f, "\t\t{}: ", pid)?;
            node.fmt(f)?;
            f.write_str("\n")?;

            if let Some(next_pid) = node.next {
                pid = next_pid;
            } else {
                // we've traversed our level, time to bump down
                let left_get_res = self.view_for_pid(left_most, &guard);
                let left_node = match left_get_res {
                    Ok(Some(ref view)) => view.node,
                    broken => {
                        panic!("pagecache returned non-base node: {:?}", broken)
                    }
                };

                match &left_node.data {
                    Data::Index(ptrs) => {
                        if let Some(&(ref _sep, ref next_pid)) = ptrs.first() {
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

        Ok(())
    }
}
