use std::{
    fmt::{self, Debug},
    ops::{self, RangeBounds},
    sync::{
        atomic::{AtomicU64, Ordering::SeqCst},
        Arc,
    },
};

use super::*;

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
/// let t = Db::start_default("db").unwrap();
/// t.set(b"yo!", b"v1".to_vec());
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
/// let mut iter = t.range(b"a non-present key before yo!"..);
/// assert_eq!(iter.next().unwrap(), Ok((b"yo!".to_vec(), IVec::from(b"v2"))));
/// assert_eq!(iter.next(), None);
///
/// t.del(b"yo!");
/// assert_eq!(t.get(b"yo!"), Ok(None));
/// ```
#[derive(Clone)]
pub struct Tree {
    pub(crate) tree_id: Vec<u8>,
    pub(crate) context: Context,
    pub(crate) subscriptions: Arc<Subscriptions>,
    pub(crate) root: Arc<AtomicU64>,
}

unsafe impl Send for Tree {}

unsafe impl Sync for Tree {}

impl Tree {
    /// Set a key to a new value, returning the last value if it
    /// was set.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::{ConfigBuilder, Db, IVec};
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let t = Db::start(config).unwrap();
    ///
    /// assert_eq!(t.set(&[0], vec![0]), Ok(None));
    /// assert_eq!(t.set(&[0], vec![1]), Ok(Some(IVec::from(vec![0]))));
    /// ```
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
            let view = self.view_for_key(key.as_ref(), &tx)?;
            let encoded_key = prefix_encode(view.lo, key.as_ref());

            let mut subscriber_reservation = self.subscriptions.reserve(&key);

            let last_value =
                view.leaf_value_for_key(&encoded_key, &self.context);
            let frag = Frag::Set(encoded_key, value.clone());
            let link = self.context.pagecache.link(
                view.pid,
                view.ptr.clone(),
                frag.clone(),
                &tx,
            )?;
            if let Ok(_new_cas_key) = link {
                // success
                if let Some(res) = subscriber_reservation.take() {
                    let event =
                        subscription::Event::Set(key.as_ref().to_vec(), value);

                    res.complete(event);
                }

                tx.flush();

                return Ok(last_value);
            }
            M.tree_looped();
        }
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
    /// t.set(&[0], vec![0]).unwrap();
    /// assert_eq!(t.get(&[0]), Ok(Some(IVec::from(vec![0]))));
    /// assert_eq!(t.get(&[1]), Ok(None));
    /// ```
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<IVec>> {
        let _measure = Measure::new(&M.tree_get);
        trace!("getting key {:?}", key.as_ref());

        let tx = self.context.pagecache.begin()?;

        let view = self.view_for_key(key.as_ref(), &tx)?;

        tx.flush();

        Ok(view.leaf_value_for_key(key.as_ref(), &self.context))
    }

    /// Delete a value, returning the old value if it existed.
    ///
    /// # Examples
    ///
    /// ```
    /// let config = sled::ConfigBuilder::new().temporary(true).build();
    /// let t = sled::Db::start(config).unwrap();
    /// t.set(&[1], vec![1]);
    /// assert_eq!(t.del(&[1]), Ok(Some(sled::IVec::from(vec![1]))));
    /// assert_eq!(t.del(&[1]), Ok(None));
    /// ```
    pub fn del<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<IVec>> {
        let _measure = Measure::new(&M.tree_del);

        if self.context.read_only {
            return Ok(None);
        }

        loop {
            let tx = self.context.pagecache.begin()?;

            let view = self.view_for_key(key.as_ref(), &tx)?;
            let existing_val =
                view.leaf_value_for_key(key.as_ref(), &self.context);

            let mut subscriber_reservation = self.subscriptions.reserve(&key);

            let encoded_key = prefix_encode(view.lo, key.as_ref());

            let frag = Frag::Del(encoded_key);

            let link = self.context.pagecache.link(
                view.pid,
                view.ptr.clone(),
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
            let view = self.view_for_key(key.as_ref(), &tx)?;
            let cur = view.leaf_value_for_key(key.as_ref(), &self.context);

            let matches = match (&old, &cur) {
                (None, None) => true,
                (Some(ref o), Some(ref c)) => o.as_ref() == &**c,
                _ => false,
            };

            if !matches {
                return Ok(Err(cur));
            }

            let mut subscriber_reservation = self.subscriptions.reserve(&key);

            let encoded_key = prefix_encode(view.lo, key.as_ref());
            let frag = if let Some(ref new) = new {
                Frag::Set(encoded_key, new.clone())
            } else {
                Frag::Del(encoded_key)
            };
            let link =
                self.context.pagecache.link(view.pid, view.ptr, frag, &tx)?;

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
    ///
    /// # Examples
    ///
    /// ```
    /// let config = sled::ConfigBuilder::new().temporary(true).build();
    /// let t = sled::Db::start(config).unwrap();
    ///
    /// t.set(&[0], vec![0]).unwrap();
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
    ///     tree.set(&[i], vec![i]).expect("should write successfully");
    /// }
    ///
    /// assert_eq!(tree.get_lt(&[]), Ok(None));
    /// assert_eq!(tree.get_lt(&[0]), Ok(None));
    /// assert_eq!(tree.get_lt(&[1]), Ok(Some((vec![0], IVec::from(vec![0])))));
    /// assert_eq!(tree.get_lt(&[9]), Ok(Some((vec![8], IVec::from(vec![8])))));
    /// assert_eq!(tree.get_lt(&[10]), Ok(Some((vec![9], IVec::from(vec![9])))));
    /// assert_eq!(tree.get_lt(&[255]), Ok(Some((vec![9], IVec::from(vec![9])))));
    /// ```
    pub fn get_lt<K>(&self, key: K) -> Result<Option<(IVec, IVec)>>
    where
        K: AsRef<[u8]>,
    {
        let _measure = Measure::new(&M.tree_get);
        self.range(..key).next_back().transpose()
    }

    /// Retrieve the next key and value from the `Tree` after the
    /// provided key.
    ///
    /// # Examples
    ///
    /// ```
    /// use sled::{ConfigBuilder, Db, IVec};
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let tree = Db::start(config).unwrap();
    ///
    /// for i in 0..10 {
    ///     tree.set(&[i], vec![i]).expect("should write successfully");
    /// }
    ///
    /// assert_eq!(tree.get_gt(&[]), Ok(Some((vec![0], IVec::from(vec![0])))));
    /// assert_eq!(tree.get_gt(&[0]), Ok(Some((vec![1], IVec::from(vec![1])))));
    /// assert_eq!(tree.get_gt(&[1]), Ok(Some((vec![2], IVec::from(vec![2])))));
    /// assert_eq!(tree.get_gt(&[8]), Ok(Some((vec![9], IVec::from(vec![9])))));
    /// assert_eq!(tree.get_gt(&[9]), Ok(None));
    /// ```
    pub fn get_gt<K>(&self, key: K) -> Result<Option<(IVec, IVec)>>
    where
        K: AsRef<[u8]>,
    {
        let _measure = Measure::new(&M.tree_get);
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
    ///   .merge_operator(concatenate_merge)
    ///   .build();
    ///
    /// let tree = Db::start(config).unwrap();
    ///
    /// let k = b"k1";
    ///
    /// tree.set(k, vec![0]);
    /// tree.merge(k, vec![1]);
    /// tree.merge(k, vec![2]);
    /// assert_eq!(tree.get(k), Ok(Some(IVec::from(vec![0, 1, 2]))));
    ///
    /// // Replace previously merged data. The merge function will not be called.
    /// tree.set(k, vec![3]);
    /// assert_eq!(tree.get(k), Ok(Some(IVec::from(vec![3]))));
    ///
    /// // Merges on non-present values will cause the merge function to be called
    /// // with `old_value == None`. If the merge function returns something (which it
    /// // does, in this case) a new value will be inserted.
    /// tree.del(k);
    /// tree.merge(k, vec![4]);
    /// assert_eq!(tree.get(k), Ok(Some(IVec::from(vec![4]))));
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

        if self.context.merge_operator.is_none() {
            return Err(Error::Unsupported(
                "must set a merge_operator on config \
                 before calling merge"
                    .to_owned(),
            ));
        }

        let value = IVec::from(value);

        loop {
            let tx = self.context.pagecache.begin()?;

            let view = self.view_for_key(key.as_ref(), &tx)?;

            let mut subscriber_reservation = self.subscriptions.reserve(&key);

            let encoded_key = prefix_encode(view.lo, key.as_ref());
            let frag = Frag::Merge(encoded_key, value.clone());

            let link = self.context.pagecache.link(
                view.pid,
                view.ptr.clone(),
                frag.clone(),
                &tx,
            )?;
            if let Ok(_new_cas_key) = link {
                // success
                if let Some(res) = subscriber_reservation.take() {
                    let event = subscription::Event::Merge(
                        key.as_ref().to_vec(),
                        value,
                    );

                    res.complete(event);
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
    /// use sled::{ConfigBuilder, Db, IVec};
    /// let config = ConfigBuilder::new().temporary(true).build();
    /// let t = Db::start(config).unwrap();
    /// t.set(&[1], vec![10]);
    /// t.set(&[2], vec![20]);
    /// t.set(&[3], vec![30]);
    /// let mut iter = t.iter();
    /// assert_eq!(iter.next().unwrap(), Ok((vec![1], vec![10].into())));
    /// assert_eq!(iter.next().unwrap(), Ok((vec![2], vec![20].into())));
    /// assert_eq!(iter.next().unwrap(), Ok((vec![3], vec![30].into())));
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
    /// t.set(&[0], vec![0]).unwrap();
    /// t.set(&[1], vec![10]).unwrap();
    /// t.set(&[2], vec![20]).unwrap();
    /// t.set(&[3], vec![30]).unwrap();
    /// t.set(&[4], vec![40]).unwrap();
    /// t.set(&[5], vec![50]).unwrap();
    ///
    /// let start: &[u8] = &[2];
    /// let end: &[u8] = &[4];
    /// let mut r = t.range(start..end);
    /// assert_eq!(r.next().unwrap(), Ok((vec![2], IVec::from(vec![20]))));
    /// assert_eq!(r.next().unwrap(), Ok((vec![3], IVec::from(vec![30]))));
    /// assert_eq!(r.next(), None);
    ///
    /// let mut r = t.range(start..end).rev();
    /// assert_eq!(r.next().unwrap(), Ok((vec![3], IVec::from(vec![30]))));
    /// assert_eq!(r.next().unwrap(), Ok((vec![2], IVec::from(vec![20]))));
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
            cached_view: None,
            tx: self.context.pagecache.begin(),
            going_forward: true,
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
    /// t.set(b"a", vec![0]);
    /// t.set(b"b", vec![1]);
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
            self.del(key)?;
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
        root_pid: Option<PageId>,
        tx: &'g Tx<'g, BLinkMaterializer, Frag>,
    ) -> Result<()> {
        // split node
        let (mut lhs, rhs) = node_view.split(&self.context);
        let rhs_lo = rhs.lo.clone();

        // install right side
        let (rhs_pid, rhs_ptr) =
            self.context.pagecache.allocate(Frag::Base(rhs), tx)?;

        // replace node, pointing next to installed right
        lhs.next = Some(rhs_pid);
        let replace = self.context.pagecache.replace(
            node_view.pid,
            node_view.ptr.clone(),
            Frag::Base(lhs),
            tx,
        )?;
        M.tree_child_split_attempt();
        if replace.is_err() {
            // if we failed, don't follow through with the
            // parent split or root hoist.
            self.context
                .pagecache
                .free(rhs_pid, rhs_ptr, tx)?
                .expect("could not free allocated page");
            return Ok(());
        }
        M.tree_child_split_success();

        // either install parent split or hoist root
        if let Some(parent_view) = parent_view {
            M.tree_parent_split_attempt();
            let mut parent = parent_view.compact(&self.context);
            parent.parent_split(&rhs_lo, rhs_pid);
            let replace = self.context.pagecache.replace(
                parent_view.pid,
                parent_view.ptr.clone(),
                Frag::Base(parent),
                tx,
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
            if self
                .root_hoist(root_pid.unwrap(), rhs_pid, rhs_lo, tx)
                .is_ok()
            {
                M.tree_root_split_success();
            }
        }

        Ok(())
    }

    fn root_hoist<'g>(
        &self,
        from: PageId,
        to: PageId,
        at: IVec,
        tx: &'g Tx<BLinkMaterializer, Frag>,
    ) -> Result<()> {
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

    pub(crate) fn view_for_pid<'g>(
        &self,
        pid: PageId,
        tx: &'g Tx<BLinkMaterializer, Frag>,
    ) -> Result<Option<View<'g>>> {
        let frag_opt = self.context.pagecache.get(pid, tx)?;
        if let Some((tree_ptr, frags)) = frag_opt {
            Ok(Some(View::new(pid, tree_ptr, frags)))
        } else {
            Ok(None)
        }
    }

    /// returns the traversal path, completing any observed
    /// partially complete splits or merges along the way.
    pub(crate) fn view_for_key<'g, K>(
        &self,
        key: K,
        tx: &'g Tx<BLinkMaterializer, Frag>,
    ) -> Result<View<'g>>
    where
        K: AsRef<[u8]>,
    {
        let _measure = Measure::new(&M.tree_traverse);

        let mut cursor = self.root.load(SeqCst);
        let mut root_pid = Some(cursor);
        let mut parent_view = None;
        let mut unsplit_parent = None;
        let mut last_branch = 0;

        macro_rules! retry {
            () => {
                cursor = self.root.load(SeqCst);
                root_pid = Some(cursor);
                parent_view = None;
                unsplit_parent = None;
                last_branch = 0;
                continue;
            };
        }

        let mut not_found_loops = 0;
        loop {
            not_found_loops += 1;
            debug_assert_ne!(
                not_found_loops, 1000,
                "cannot find pid {} in view_for_key, looking for key {:?} in tree {:?}",
                cursor, key.as_ref(), self
            );

            if cursor == u64::max_value() {
                // this collection has been explicitly removed
                return Err(Error::CollectionNotFound(self.tree_id.clone()));
            }

            let view_opt = self.view_for_pid(cursor, tx)?;

            let view = if let Some(view) = view_opt {
                view
            } else {
                println!("didn't find view for pid {}", cursor);
                retry!();
            };

            let overshot = key.as_ref() < view.lo.as_ref();
            let undershot =
                key.as_ref() > view.hi.as_ref() && !view.hi.is_empty();

            if overshot {
                // merge interfered, reload root and retry
                retry!();
            }

            if view.should_split(self.context.blink_node_split_size as u64) {
                self.split_node(&view, &parent_view, root_pid, tx)?;
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
                    assert_eq!(view.pid, root_pid.unwrap());
                    // we have found a partially-split root
                    if self
                        .root_hoist(
                            root_pid.unwrap(),
                            view.next.unwrap(),
                            view.hi.into(),
                            tx,
                        )
                        .is_ok()
                    {
                        M.tree_root_split_success();
                        retry!();
                    }
                }
                continue;
            } else if let Some(unsplit_parent) = unsplit_parent.take() {
                // we have found the proper page for
                // our cooperative parent split
                let mut parent = unsplit_parent.compact(&self.context);
                parent.parent_split(view.lo.as_ref(), cursor);

                M.tree_parent_split_attempt();
                let replace = self.context.pagecache.replace(
                    unsplit_parent.pid,
                    unsplit_parent.ptr.clone(),
                    Frag::Base(parent),
                    tx,
                )?;
                if replace.is_ok() {
                    M.tree_parent_split_success();
                }
            }

            /*
            // When we encounter a merge intention, we collaboratively help out
            if view.merging_child.is_some() {
                self.merge_node(&view, tx)?;
                retry!();
            }

            if last_branch != 0 && view.should_merge(
                (self.context.blink_node_split_size
                    / self.context.blink_node_merge_ratio)
                    as u64,
            ) {
                if let Some(ref mut parent_view) = parent_view {
                    if parent_view.can_merge_child() {
                        let frag = Frag::ParentMergeIntention(view.pid);

                        let link = self.context.pagecache.link(
                            parent_view.pid,
                            parent_view.ptr.clone(),
                            frag,
                            tx,
                        )?;

                        if let Ok(new_parent_ptr) = link {
                            parent_view.ptr = new_parent_ptr;
                            parent_view.merging_child = Some(view.pid);
                            self.merge_node(&parent_view, tx)?;
                            retry!();
                        }
                    }
                }
            }
            */

            if view.is_index {
                let next = view.index_next_node(key.as_ref());
                last_branch = next.0;
                cursor = next.1;
                parent_view = Some(view);
            } else {
                assert!(!overshot && !undershot);
                return Ok(view);
            }
        }
    }

    pub(crate) fn merge_node<'g>(
        &self,
        parent: &View,
        tx: &'g Tx<BLinkMaterializer, Frag>,
    ) -> Result<()> {
        let child_pid = parent.merging_child.unwrap();

        // Get the child node and try to install a `MergeCap` frag.
        // In case we succeed, we break, otherwise we try from the start.
        let child_node = loop {
            let child_view =
                if let Some(child_view) = self.view_for_pid(child_pid, tx)? {
                    child_view
                } else {
                    return Ok(());
                };

            let child_node = child_view.compact(&self.context);

            if child_node.merging {
                break child_node;
            }

            let install_frag = self.context.pagecache.link(
                child_pid,
                child_view.ptr,
                Frag::ChildMergeCap,
                tx,
            )?;
            match install_frag {
                Ok(_) => break child_node,
                Err(Some((_, _))) => continue,
                Err(None) => return Ok(()),
            }
        };

        let index = parent.base_data.index_ref().unwrap();
        let merge_index =
            index.iter().position(|(_, pid)| pid == &child_pid).unwrap();

        let mut cursor_pid = (index[merge_index - 1]).1;

        loop {
            // The only way this child could have been freed is if the original merge has
            // already been handled. Only in that case can this child have been freed
            let cursor_view =
                if let Some(cursor_view) = self.view_for_pid(cursor_pid, tx)? {
                    cursor_view
                } else {
                    return Ok(());
                };

            // Make sure we don't overseek cursor
            // We break instead of returning because otherwise a thread that
            // collaboratively wants to complete the merge could never reach
            // the point where it can install the merge confirmation frag.
            if cursor_view.lo >= &child_node.lo {
                break;
            }

            // This means that `cursor_node` is the node we want to replace
            if cursor_view.next == Some(child_pid) {
                let cursor_node = cursor_view.compact(&self.context);
                let cursor_cas_key = cursor_view.ptr;

                let replacement = cursor_node.receive_merge(&child_node);
                let replace = self.context.pagecache.replace(
                    cursor_pid,
                    cursor_cas_key,
                    Frag::Base(replacement),
                    tx,
                )?;
                match replace {
                    Ok(_) => break,
                    Err(None) => return Ok(()),
                    Err(_) => continue,
                }
            } else {
                // In case we didn't find the child, we get the next cursor node
                if let Some(next) = cursor_view.next {
                    cursor_pid = next;
                } else {
                    return Ok(());
                }
            }
        }

        let mut parent_cas_key = parent.ptr.clone();

        loop {
            let linked = self.context.pagecache.link(
                parent.pid,
                parent_cas_key,
                Frag::ParentMergeConfirm,
                tx,
            )?;
            match linked {
                Ok(_) => break,
                Err(None) => break,
                Err(_) => {
                    let parent_view = if let Some(parent_view) =
                        self.view_for_pid(parent.pid, tx)?
                    {
                        parent_view
                    } else {
                        break;
                    };

                    if parent_view.merging_child != Some(child_pid) {
                        break;
                    }

                    parent_cas_key = parent_view.ptr;
                }
            }
        }

        Ok(())
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
                let cursor_view =
                    if let Some(view) = self.view_for_pid(pid, &tx)? {
                        view
                    } else {
                        error!("encountered Free node while GC'ing tree");
                        break;
                    };

                let ret =
                    self.context.pagecache.free(pid, cursor_view.ptr, &tx)?;

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
        let tx = self.context.pagecache.begin().unwrap();

        let mut pid = self.root.load(SeqCst);
        let mut left_most = pid;
        let mut level = 0;

        f.write_str("Tree: \n\t")?;
        self.context.pagecache.fmt(f)?;
        f.write_str("\tlevel 0:\n")?;

        loop {
            let get_res = self.view_for_pid(pid, &tx);
            let node = match get_res {
                Ok(Some(ref view)) => view.compact(&self.context),
                broken => {
                    panic!("pagecache returned non-base node: {:?}", broken)
                }
            };

            write!(f, "\t\t{}: ", pid)?;
            node.fmt(f)?;
            f.write_str("\n")?;

            if let Some(next_pid) = node.next {
                pid = next_pid;
            } else {
                // we've traversed our level, time to bump down
                let left_get_res = self.view_for_pid(left_most, &tx);
                let left_node = match left_get_res {
                    Ok(Some(ref view)) => view.compact(&self.context),
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

        tx.flush();

        Ok(())
    }
}
