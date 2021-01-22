use std::{
    borrow::Cow,
    fmt::{self, Debug},
    num::NonZeroU64,
    ops::{self, Deref, RangeBounds},
    sync::atomic::Ordering::SeqCst,
};

use parking_lot::RwLock;

use crate::{atomic_shim::AtomicU64, pagecache::NodeView, *};

#[derive(Debug, Clone)]
pub(crate) struct View<'g> {
    pub node_view: NodeView<'g>,
    pub pid: PageId,
    pub size: u64,
}

impl<'g> Deref for View<'g> {
    type Target = Node;

    fn deref(&self) -> &Node {
        &*self.node_view
    }
}

impl IntoIterator for &'_ Tree {
    type Item = Result<(IVec, IVec)>;
    type IntoIter = Iter;

    fn into_iter(self) -> Iter {
        self.iter()
    }
}

const fn out_of_bounds(numba: usize) -> bool {
    numba > MAX_BLOB
}

#[cold]
fn bounds_error() -> Result<()> {
    Err(Error::Unsupported(
        "Keys and values are limited to \
        128gb on 64-bit platforms and
        512mb on 32-bit platforms.".to_string()
    ))
}

/// A flash-sympathetic persistent lock-free B+ tree.
///
/// A `Tree` represents a single logical keyspace / namespace / bucket.
///
/// Separate `Trees` may be opened to separate concerns using
/// `Db::open_tree`.
///
/// `Db` implements `Deref<Target = Tree>` such that a `Db` acts
/// like the "default" `Tree`. This is the only `Tree` that cannot
/// be deleted via `Db::drop_tree`.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use sled::IVec;
///
/// # let _ = std::fs::remove_dir_all("db");
/// let db: sled::Db = sled::open("db")?;
/// db.insert(b"yo!", b"v1".to_vec());
/// assert_eq!(db.get(b"yo!"), Ok(Some(IVec::from(b"v1"))));
///
/// // Atomic compare-and-swap.
/// db.compare_and_swap(
///     b"yo!",      // key
///     Some(b"v1"), // old value, None for not present
///     Some(b"v2"), // new value, None for delete
/// )?;
///
/// // Iterates over key-value pairs, starting at the given key.
/// let scan_key: &[u8] = b"a non-present key before yo!";
/// let mut iter = db.range(scan_key..);
/// assert_eq!(
///     iter.next().unwrap(),
///     Ok((IVec::from(b"yo!"), IVec::from(b"v2")))
/// );
/// assert_eq!(iter.next(), None);
///
/// db.remove(b"yo!");
/// assert_eq!(db.get(b"yo!"), Ok(None));
///
/// let other_tree: sled::Tree = db.open_tree(b"cool db facts")?;
/// other_tree.insert(
///     b"k1",
///     &b"a Db acts like a Tree due to implementing Deref<Target = Tree>"[..]
/// )?;
/// # let _ = std::fs::remove_dir_all("db");
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct Tree(pub(crate) Arc<TreeInner>);

#[allow(clippy::module_name_repetitions)]
pub struct TreeInner {
    pub(crate) tree_id: IVec,
    pub(crate) context: Context,
    pub(crate) subscribers: Subscribers,
    pub(crate) root: AtomicU64,
    pub(crate) merge_operator: RwLock<Option<Box<dyn MergeOperator>>>,
}

impl Drop for TreeInner {
    fn drop(&mut self) {
        // Flush the underlying system in a loop until we
        // have flushed all dirty data.
        loop {
            match self.context.pagecache.flush() {
                Ok(0) => return,
                Ok(_) => continue,
                Err(e) => {
                    error!("failed to flush data to disk: {:?}", e);
                    return;
                }
            }
        }
    }
}

impl Deref for Tree {
    type Target = TreeInner;

    fn deref(&self) -> &TreeInner {
        &self.0
    }
}

impl Tree {
    /// Insert a key to a new value, returning the last value if it
    /// was set.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// assert_eq!(db.insert(&[1, 2, 3], vec![0]), Ok(None));
    /// assert_eq!(db.insert(&[1, 2, 3], vec![1]), Ok(Some(sled::IVec::from(&[0]))));
    /// # Ok(()) }
    /// ```
    pub fn insert<K, V>(&self, key: K, value: V) -> Result<Option<IVec>>
    where
        K: AsRef<[u8]>,
        V: Into<IVec>,
    {
        let value = value.into();
        let mut guard = pin();
        let _cc = concurrency_control::read();
        loop {
            trace!("setting key {:?}", key.as_ref());
            if let Ok(res) = self.insert_inner(
                key.as_ref(),
                Some(value.clone()),
                false,
                &mut guard,
            )? {
                return Ok(res);
            }
        }
    }

    pub(crate) fn insert_inner(
        &self,
        key: &[u8],
        value: Option<IVec>,
        is_transactional: bool,
        guard: &mut Guard,
    ) -> Result<Conflictable<Option<IVec>>> {
        #[cfg(feature = "metrics")]
        let _measure = if value.is_some() {
            Measure::new(&M.tree_set)
        } else {
            Measure::new(&M.tree_del)
        };

        if out_of_bounds(key.len()) {
            bounds_error()?;
        }

        let View { node_view, pid, .. } =
            self.view_for_key(key.as_ref(), guard)?;

        let mut subscriber_reservation = if is_transactional {
            None
        } else {
            Some(self.subscribers.reserve(&key))
        };

        let (encoded_key, last_value) = node_view.node_kv_pair(key.as_ref());
        let last_value = last_value.map(IVec::from);

        if value == last_value {
            // short-circuit a no-op set or delete
            return Ok(Ok(value));
        }

        let frag = if let Some(value) = value.clone() {
            if out_of_bounds(value.len()) {
                bounds_error()?;
            }
            Link::Set(encoded_key, value)
        } else {
            Link::Del(encoded_key)
        };

        let link =
            self.context.pagecache.link(pid, node_view.0, frag, guard)?;

        if link.is_ok() {
            // success
            if let Some(Some(res)) = subscriber_reservation.take() {
                let event = subscriber::Event::single_update(
                    self.clone(),
                    key.as_ref().into(),
                    value,
                );

                res.complete(&event);
            }

            Ok(Ok(last_value))
        } else {
            #[cfg(feature = "metrics")]
            M.tree_looped();
            Ok(Err(Conflict))
        }
    }

    /// Perform a multi-key serializable transaction.
    ///
    /// sled transactions are **optimistic** which means that
    /// they may re-run in cases where conflicts are detected.
    /// Do not perform IO or interact with state outside
    /// of the closure unless it is idempotent, because
    /// it may re-run several times.
    ///
    /// # Examples
    ///
    /// ```
    /// # use sled::{transaction::TransactionResult, Config};
    /// # fn main() -> TransactionResult<()> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// // Use write-only transactions as a writebatch:
    /// db.transaction(|tx_db| {
    ///     tx_db.insert(b"k1", b"cats")?;
    ///     tx_db.insert(b"k2", b"dogs")?;
    ///     Ok(())
    /// })?;
    ///
    /// // Atomically swap two items:
    /// db.transaction(|tx_db| {
    ///     let v1_option = tx_db.remove(b"k1")?;
    ///     let v1 = v1_option.unwrap();
    ///     let v2_option = tx_db.remove(b"k2")?;
    ///     let v2 = v2_option.unwrap();
    ///
    ///     tx_db.insert(b"k1", v2)?;
    ///     tx_db.insert(b"k2", v1)?;
    ///
    ///     Ok(())
    /// })?;
    ///
    /// assert_eq!(&db.get(b"k1")?.unwrap(), b"dogs");
    /// assert_eq!(&db.get(b"k2")?.unwrap(), b"cats");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A transaction may return information from
    /// an intentionally-cancelled transaction by using
    /// the abort function inside the closure in
    /// combination with the try operator.
    ///
    /// ```
    /// use sled::{transaction::{abort, TransactionError, TransactionResult}, Config};
    ///
    /// #[derive(Debug, PartialEq)]
    /// struct MyBullshitError;
    ///
    /// fn main() -> TransactionResult<(), MyBullshitError> {
    ///     let config = Config::new().temporary(true);
    ///     let db = config.open()?;
    ///
    ///     // Use write-only transactions as a writebatch:
    ///     let res = db.transaction(|tx_db| {
    ///         tx_db.insert(b"k1", b"cats")?;
    ///         tx_db.insert(b"k2", b"dogs")?;
    ///         // aborting will cause all writes to roll-back.
    ///         if true {
    ///             abort(MyBullshitError)?;
    ///         }
    ///         Ok(42)
    ///     }).unwrap_err();
    ///
    ///     assert_eq!(res, TransactionError::Abort(MyBullshitError));
    ///     assert_eq!(db.get(b"k1")?, None);
    ///     assert_eq!(db.get(b"k2")?, None);
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    ///
    /// Transactions also work on tuples of `Tree`s,
    /// preserving serializable ACID semantics!
    /// In this example, we treat two trees like a
    /// work queue, atomically apply updates to
    /// data and move them from the unprocessed `Tree`
    /// to the processed `Tree`.
    ///
    /// ```
    /// # use sled::transaction::TransactionResult;
    /// # fn main() -> TransactionResult<()> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// use sled::Transactional;
    ///
    /// let unprocessed = db.open_tree(b"unprocessed items")?;
    /// let processed = db.open_tree(b"processed items")?;
    ///
    /// // An update somehow gets into the tree, which we
    /// // later trigger the atomic processing of.
    /// unprocessed.insert(b"k3", b"ligers")?;
    ///
    /// // Atomically process the new item and move it
    /// // between `Tree`s.
    /// (&unprocessed, &processed)
    ///     .transaction(|(tx_unprocessed, tx_processed)| {
    ///         let unprocessed_item = tx_unprocessed.remove(b"k3")?.unwrap();
    ///         let mut processed_item = b"yappin' ".to_vec();
    ///         processed_item.extend_from_slice(&unprocessed_item);
    ///         tx_processed.insert(b"k3", processed_item)?;
    ///         Ok(())
    ///     })?;
    ///
    /// assert_eq!(unprocessed.get(b"k3").unwrap(), None);
    /// assert_eq!(&processed.get(b"k3").unwrap().unwrap(), b"yappin' ligers");
    /// # Ok(()) }
    /// ```
    pub fn transaction<F, A, E>(
        &self,
        f: F,
    ) -> transaction::TransactionResult<A, E>
    where
        F: Fn(
            &transaction::TransactionalTree,
        ) -> transaction::ConflictableTransactionResult<A, E>,
    {
        Transactional::transaction(&self, f)
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
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// db.insert("key_0", "val_0")?;
    ///
    /// let mut batch = sled::Batch::default();
    /// batch.insert("key_a", "val_a");
    /// batch.insert("key_b", "val_b");
    /// batch.insert("key_c", "val_c");
    /// batch.remove("key_0");
    ///
    /// db.apply_batch(batch)?;
    /// // key_0 no longer exists, and key_a, key_b, and key_c
    /// // now do exist.
    /// # Ok(()) }
    /// ```
    pub fn apply_batch(&self, batch: Batch) -> Result<()> {
        let _cc = concurrency_control::write();
        let mut guard = pin();
        self.apply_batch_inner(batch, None, &mut guard)
    }

    pub(crate) fn apply_batch_inner(
        &self,
        batch: Batch,
        transaction_batch: Option<Event>,
        guard: &mut Guard,
    ) -> Result<()> {
        let peg = if transaction_batch.is_none() {
            Some(self.context.pin_log(guard)?)
        } else {
            None
        };

        trace!("applying batch {:?}", batch);

        let mut subscriber_reservation = self.subscribers.reserve_batch(&batch);

        for (k, v_opt) in &batch.writes {
            loop {
                if self
                    .insert_inner(
                        k,
                        v_opt.clone(),
                        transaction_batch.is_some(),
                        guard,
                    )?
                    .is_ok()
                {
                    break;
                }
            }
        }

        if let Some(res) = subscriber_reservation.take() {
            if let Some(transaction_batch) = transaction_batch {
                res.complete(&transaction_batch);
            } else {
                res.complete(&Event::single_batch(self.clone(), batch));
            }
        }

        if let Some(peg) = peg {
            // when the peg drops, it ensures all updates
            // written to the log since its creation are
            // recovered atomically
            peg.seal_batch()
        } else {
            Ok(())
        }
    }

    /// Retrieve a value from the `Tree` if it exists.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// db.insert(&[0], vec![0])?;
    /// assert_eq!(db.get(&[0]), Ok(Some(sled::IVec::from(vec![0]))));
    /// assert_eq!(db.get(&[1]), Ok(None));
    /// # Ok(()) }
    /// ```
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<IVec>> {
        let mut guard = pin();
        let _cc = concurrency_control::read();
        loop {
            if let Ok(get) = self.get_inner(key.as_ref(), &mut guard)? {
                return Ok(get);
            }
        }
    }

    /// Pass the result of getting a key's value to a closure
    /// without making a new allocation. This effectively
    /// "pushes" your provided code to the data without ever copying
    /// the data, rather than "pulling" a copy of the data to whatever code
    /// is calling `get`.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// db.insert(&[0], vec![0])?;
    /// db.get_zero_copy(&[0], |value_opt| {
    ///     assert_eq!(
    ///         value_opt,
    ///         Some(&[0][..])
    ///     )
    /// });
    /// db.get_zero_copy(&[1], |value_opt| assert!(value_opt.is_none()));
    /// # Ok(()) }
    /// ```
    pub fn get_zero_copy<K: AsRef<[u8]>, B, F: FnOnce(Option<&[u8]>) -> B>(
        &self,
        key: K,
        f: F,
    ) -> Result<B> {
        let guard = pin();
        let _cc = concurrency_control::read();

        #[cfg(feature = "metrics")]
        let _measure = Measure::new(&M.tree_get);

        trace!("getting key {:?}", key.as_ref());

        let View { node_view, .. } =
            self.view_for_key(key.as_ref(), &guard)?;

        let pair = node_view.node_kv_pair(key.as_ref());

        let ret = f(pair.1);

        Ok(ret)
    }

    pub(crate) fn get_inner(
        &self,
        key: &[u8],
        guard: &mut Guard,
    ) -> Result<Conflictable<Option<IVec>>> {
        #[cfg(feature = "metrics")]
        let _measure = Measure::new(&M.tree_get);

        trace!("getting key {:?}", key);

        let View { node_view,  .. } =
            self.view_for_key(key.as_ref(), guard)?;

        let pair = node_view.node_kv_pair(key.as_ref());
        let val = pair.1.map(IVec::from);

        Ok(Ok(val))
    }

    /// Delete a value, returning the old value if it existed.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// db.insert(&[1], vec![1]);
    /// assert_eq!(db.remove(&[1]), Ok(Some(sled::IVec::from(vec![1]))));
    /// assert_eq!(db.remove(&[1]), Ok(None));
    /// # Ok(()) }
    /// ```
    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<IVec>> {
        let mut guard = pin();
        let _cc = concurrency_control::read();
        loop {
            trace!("removing key {:?}", key.as_ref());

            if let Ok(res) =
                self.insert_inner(key.as_ref(), None, false, &mut guard)?
            {
                return Ok(res);
            }
        }
    }

    /// Compare and swap. Capable of unique creation, conditional modification,
    /// or deletion. If old is `None`, this will only set the value if it
    /// doesn't exist yet. If new is `None`, will delete the value if old is
    /// correct. If both old and new are `Some`, will modify the value if
    /// old is correct.
    ///
    /// It returns `Ok(Ok(()))` if operation finishes successfully.
    ///
    /// If it fails it returns:
    ///     - `Ok(Err(CompareAndSwapError(current, proposed)))` if operation
    ///       failed to setup a new value. `CompareAndSwapError` contains
    ///       current and proposed values.
    ///     - `Err(Error::Unsupported)` if the database is opened in read-only
    ///       mode.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// // unique creation
    /// assert_eq!(
    ///     db.compare_and_swap(&[1], None as Option<&[u8]>, Some(&[10])),
    ///     Ok(Ok(()))
    /// );
    ///
    /// // conditional modification
    /// assert_eq!(
    ///     db.compare_and_swap(&[1], Some(&[10]), Some(&[20])),
    ///     Ok(Ok(()))
    /// );
    ///
    /// // failed conditional modification -- the current value is returned in
    /// // the error variant
    /// let operation = db.compare_and_swap(&[1], Some(&[30]), Some(&[40]));
    /// assert!(operation.is_ok()); // the operation succeeded
    /// let modification = operation.unwrap();
    /// assert!(modification.is_err());
    /// let actual_value = modification.unwrap_err();
    /// assert_eq!(actual_value.current.map(|ivec| ivec.to_vec()), Some(vec![20]));
    ///
    /// // conditional deletion
    /// assert_eq!(
    ///     db.compare_and_swap(&[1], Some(&[20]), None as Option<&[u8]>),
    ///     Ok(Ok(()))
    /// );
    /// assert_eq!(db.get(&[1]), Ok(None));
    /// # Ok(()) }
    /// ```
    #[allow(clippy::needless_pass_by_value)]
    pub fn compare_and_swap<K, OV, NV>(
        &self,
        key: K,
        old: Option<OV>,
        new: Option<NV>,
    ) -> CompareAndSwapResult
    where
        K: AsRef<[u8]>,
        OV: AsRef<[u8]>,
        NV: Into<IVec>,
    {
        trace!("cas'ing key {:?}", key.as_ref());
        #[cfg(feature = "metrics")]
        let _measure = Measure::new(&M.tree_cas);

        let guard = pin();
        let _cc = concurrency_control::read();

        let new = new.map(Into::into);

        // we need to retry caps until old != cur, since just because
        // cap fails it doesn't mean our value was changed.
        loop {
            let View { pid, node_view, .. } =
                self.view_for_key(key.as_ref(), &guard)?;

            let (encoded_key, current_value) =
                node_view.node_kv_pair(key.as_ref());
            let matches = match (old.as_ref(), &current_value) {
                (None, None) => true,
                (Some(o), Some(c)) => o.as_ref() == &**c,
                _ => false,
            };

            if !matches {
                return Ok(Err(CompareAndSwapError {
                    current: current_value.map(IVec::from),
                    proposed: new,
                }));
            }

            if current_value == new.as_ref().map(AsRef::as_ref) {
                // short-circuit no-op write. this is still correct
                // because we verified that the input matches, so
                // doing the work has the same semantic effect as not
                // doing it in this case.
                return Ok(Ok(()));
            }

            let mut subscriber_reservation = self.subscribers.reserve(&key);

            let frag = if let Some(ref new) = new {
                Link::Set(encoded_key, new.clone())
            } else {
                Link::Del(encoded_key)
            };
            let link =
                self.context.pagecache.link(pid, node_view.0, frag, &guard)?;

            if link.is_ok() {
                if let Some(res) = subscriber_reservation.take() {
                    let event = subscriber::Event::single_update(
                        self.clone(),
                        key.as_ref().into(),
                        new,
                    );

                    res.complete(&event);
                }

                return Ok(Ok(()));
            }
            #[cfg(feature = "metrics")]
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
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sled::{Config, Error, IVec};
    /// use std::convert::TryInto;
    ///
    /// let config = Config::new().temporary(true);
    /// let db = config.open()?;
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
    ///         }
    ///         None => 0,
    ///     };
    ///
    ///     Some(number.to_be_bytes().to_vec())
    /// }
    ///
    /// assert_eq!(db.update_and_fetch("counter", increment), Ok(Some(zero)));
    /// assert_eq!(db.update_and_fetch("counter", increment), Ok(Some(one)));
    /// assert_eq!(db.update_and_fetch("counter", increment), Ok(Some(two)));
    /// assert_eq!(db.update_and_fetch("counter", increment), Ok(Some(three)));
    /// # Ok(()) }
    /// ```
    pub fn update_and_fetch<K, V, F>(
        &self,
        key: K,
        mut f: F,
    ) -> Result<Option<IVec>>
    where
        K: AsRef<[u8]>,
        F: FnMut(Option<&[u8]>) -> Option<V>,
        V: Into<IVec>,
    {
        let key_ref = key.as_ref();
        let mut current = self.get(key_ref)?;

        loop {
            let tmp = current.as_ref().map(AsRef::as_ref);
            let next = f(tmp).map(Into::into);
            match self.compare_and_swap::<_, _, IVec>(
                key_ref,
                tmp,
                next.clone(),
            )? {
                Ok(()) => return Ok(next),
                Err(CompareAndSwapError { current: cur, .. }) => {
                    current = cur;
                }
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
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sled::{Config, Error, IVec};
    /// use std::convert::TryInto;
    ///
    /// let config = Config::new().temporary(true);
    /// let db = config.open()?;
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
    ///         }
    ///         None => 0,
    ///     };
    ///
    ///     Some(number.to_be_bytes().to_vec())
    /// }
    ///
    /// assert_eq!(db.fetch_and_update("counter", increment), Ok(None));
    /// assert_eq!(db.fetch_and_update("counter", increment), Ok(Some(zero)));
    /// assert_eq!(db.fetch_and_update("counter", increment), Ok(Some(one)));
    /// assert_eq!(db.fetch_and_update("counter", increment), Ok(Some(two)));
    /// # Ok(()) }
    /// ```
    pub fn fetch_and_update<K, V, F>(
        &self,
        key: K,
        mut f: F,
    ) -> Result<Option<IVec>>
    where
        K: AsRef<[u8]>,
        F: FnMut(Option<&[u8]>) -> Option<V>,
        V: Into<IVec>,
    {
        let key_ref = key.as_ref();
        let mut current = self.get(key_ref)?;

        loop {
            let tmp = current.as_ref().map(AsRef::as_ref);
            let next = f(tmp);
            match self.compare_and_swap(key_ref, tmp, next)? {
                Ok(()) => return Ok(current),
                Err(CompareAndSwapError { current: cur, .. }) => {
                    current = cur;
                }
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
    /// `Subscriber` implements both `Iterator<Item = Event>`
    /// and `Future<Output=Option<Event>>`
    ///
    /// # Examples
    ///
    /// Synchronous, blocking subscriber:
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// // watch all events by subscribing to the empty prefix
    /// let mut subscriber = db.watch_prefix(vec![]);
    ///
    /// let tree_2 = db.clone();
    /// let thread = std::thread::spawn(move || {
    ///     db.insert(vec![0], vec![1])
    /// });
    ///
    /// // `Subscription` implements `Iterator<Item=Event>`
    /// for event in subscriber.take(1) {
    ///     // Events occur due to single key operations,
    ///     // batches, or transactions. The tree is included
    ///     // so that you may perform a new transaction or
    ///     // operation in response to the event.
    ///     for (tree, key, value_opt) in &event {
    ///         if let Some(value) = value_opt {
    ///             // key `key` was set to value `value`
    ///         } else {
    ///             // key `key` was removed
    ///         }
    ///     }
    /// }
    ///
    /// # thread.join().unwrap();
    /// # Ok(()) }
    /// ```
    /// Asynchronous, non-blocking subscriber:
    ///
    /// `Subscription` implements `Future<Output=Option<Event>>`.
    ///
    /// ```
    /// # async fn foo() {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open().unwrap();
    /// # let mut subscriber = db.watch_prefix(vec![]);
    /// while let Some(event) = (&mut subscriber).await {
    ///     /* use it */
    /// }
    /// # }
    /// ```
    pub fn watch_prefix<P: AsRef<[u8]>>(&self, prefix: P) -> Subscriber {
        self.subscribers.register(prefix.as_ref())
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
    // this clippy check is mis-firing on async code.
    #[allow(clippy::used_underscore_binding)]
    pub async fn flush_async(&self) -> Result<usize> {
        let pagecache = self.context.pagecache.clone();
        if let Some(result) =
            threadpool::spawn(move || pagecache.flush())?.await
        {
            result
        } else {
            Err(Error::ReportableBug(
                "threadpool failed to complete \
                action before shutdown"
                    .to_string(),
            ))
        }
    }

    /// Returns `true` if the `Tree` contains a value for
    /// the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// db.insert(&[0], vec![0])?;
    /// assert!(db.contains_key(&[0])?);
    /// assert!(!db.contains_key(&[1])?);
    /// # Ok(()) }
    /// ```
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> Result<bool> {
        self.get(key).map(|v| v.is_some())
    }

    /// Retrieve the key and value before the provided key,
    /// if one exists.
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
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sled::IVec;
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// for i in 0..10 {
    ///     db.insert(&[i], vec![i])
    ///         .expect("should write successfully");
    /// }
    ///
    /// assert_eq!(db.get_lt(&[]), Ok(None));
    /// assert_eq!(db.get_lt(&[0]), Ok(None));
    /// assert_eq!(
    ///     db.get_lt(&[1]),
    ///     Ok(Some((IVec::from(&[0]), IVec::from(&[0]))))
    /// );
    /// assert_eq!(
    ///     db.get_lt(&[9]),
    ///     Ok(Some((IVec::from(&[8]), IVec::from(&[8]))))
    /// );
    /// assert_eq!(
    ///     db.get_lt(&[10]),
    ///     Ok(Some((IVec::from(&[9]), IVec::from(&[9]))))
    /// );
    /// assert_eq!(
    ///     db.get_lt(&[255]),
    ///     Ok(Some((IVec::from(&[9]), IVec::from(&[9]))))
    /// );
    /// # Ok(()) }
    /// ```
    pub fn get_lt<K>(&self, key: K) -> Result<Option<(IVec, IVec)>>
    where
        K: AsRef<[u8]>,
    {
        #[cfg(feature = "metrics")]
        let _measure = Measure::new(&M.tree_get);
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
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sled::IVec;
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// for i in 0..10 {
    ///     db.insert(&[i], vec![i])?;
    /// }
    ///
    /// assert_eq!(
    ///     db.get_gt(&[]),
    ///     Ok(Some((IVec::from(&[0]), IVec::from(&[0]))))
    /// );
    /// assert_eq!(
    ///     db.get_gt(&[0]),
    ///     Ok(Some((IVec::from(&[1]), IVec::from(&[1]))))
    /// );
    /// assert_eq!(
    ///     db.get_gt(&[1]),
    ///     Ok(Some((IVec::from(&[2]), IVec::from(&[2]))))
    /// );
    /// assert_eq!(
    ///     db.get_gt(&[8]),
    ///     Ok(Some((IVec::from(&[9]), IVec::from(&[9]))))
    /// );
    /// assert_eq!(db.get_gt(&[9]), Ok(None));
    ///
    /// db.insert(500u16.to_be_bytes(), vec![10]);
    /// assert_eq!(
    ///     db.get_gt(&499u16.to_be_bytes()),
    ///     Ok(Some((IVec::from(&500u16.to_be_bytes()), IVec::from(&[10]))))
    /// );
    /// # Ok(()) }
    /// ```
    pub fn get_gt<K>(&self, key: K) -> Result<Option<(IVec, IVec)>>
    where
        K: AsRef<[u8]>,
    {
        #[cfg(feature = "metrics")]
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
    /// Calling `merge` will return an `Unsupported` error if it
    /// is called without first setting a merge operator function.
    ///
    /// Merge operators are shared by all instances of a particular
    /// `Tree`. Different merge operators may be set on different
    /// `Tree`s.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// use sled::IVec;
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
    /// db.set_merge_operator(concatenate_merge);
    ///
    /// let k = b"k1";
    ///
    /// db.insert(k, vec![0]);
    /// db.merge(k, vec![1]);
    /// db.merge(k, vec![2]);
    /// assert_eq!(db.get(k), Ok(Some(IVec::from(vec![0, 1, 2]))));
    ///
    /// // Replace previously merged data. The merge function will not be called.
    /// db.insert(k, vec![3]);
    /// assert_eq!(db.get(k), Ok(Some(IVec::from(vec![3]))));
    ///
    /// // Merges on non-present values will cause the merge function to be called
    /// // with `old_value == None`. If the merge function returns something (which it
    /// // does, in this case) a new value will be inserted.
    /// db.remove(k);
    /// db.merge(k, vec![4]);
    /// assert_eq!(db.get(k), Ok(Some(IVec::from(vec![4]))));
    /// # Ok(()) }
    /// ```
    pub fn merge<K, V>(&self, key: K, value: V) -> Result<Option<IVec>>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let _cc = concurrency_control::read();
        loop {
            if let Ok(merge) = self.merge_inner(key.as_ref(), value.as_ref())? {
                return Ok(merge);
            }
        }
    }

    pub(crate) fn merge_inner(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Result<Conflictable<Option<IVec>>> {
        trace!("merging key {:?}", key);
        #[cfg(feature = "metrics")]
        let _measure = Measure::new(&M.tree_merge);

        let merge_operator_opt = self.merge_operator.read();

        if merge_operator_opt.is_none() {
            return Err(Error::Unsupported(
                "must set a merge operator on this Tree \
                 before calling merge by calling \
                 Tree::set_merge_operator"
                    .to_owned(),
            ));
        }

        let merge_operator = merge_operator_opt.as_ref().unwrap();

        loop {
            let guard = pin();
            let View { pid, node_view, .. } =
                self.view_for_key(key.as_ref(), &guard)?;

            let (encoded_key, current_value) =
                node_view.node_kv_pair(key.as_ref());
            let tmp = current_value.as_ref().map(AsRef::as_ref);
            let new = merge_operator(key, tmp, value).map(IVec::from);

            if new.as_ref().map(AsRef::as_ref) == current_value {
                // short-circuit no-op write
                return Ok(Ok(new));
            }

            let mut subscriber_reservation = self.subscribers.reserve(&key);

            let frag = if let Some(ref new) = new {
                Link::Set(encoded_key, new.clone())
            } else {
                Link::Del(encoded_key)
            };
            let link =
                self.context.pagecache.link(pid, node_view.0, frag, &guard)?;

            if link.is_ok() {
                if let Some(res) = subscriber_reservation.take() {
                    let event = subscriber::Event::single_update(
                        self.clone(),
                        key.as_ref().into(),
                        new.clone(),
                    );

                    res.complete(&event);
                }

                return Ok(Ok(new));
            }
            #[cfg(feature = "metrics")]
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
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// use sled::IVec;
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
    /// db.set_merge_operator(concatenate_merge);
    ///
    /// let k = b"k1";
    ///
    /// db.insert(k, vec![0]);
    /// db.merge(k, vec![1]);
    /// db.merge(k, vec![2]);
    /// assert_eq!(db.get(k), Ok(Some(IVec::from(vec![0, 1, 2]))));
    ///
    /// // Replace previously merged data. The merge function will not be called.
    /// db.insert(k, vec![3]);
    /// assert_eq!(db.get(k), Ok(Some(IVec::from(vec![3]))));
    ///
    /// // Merges on non-present values will cause the merge function to be called
    /// // with `old_value == None`. If the merge function returns something (which it
    /// // does, in this case) a new value will be inserted.
    /// db.remove(k);
    /// db.merge(k, vec![4]);
    /// assert_eq!(db.get(k), Ok(Some(IVec::from(vec![4]))));
    /// # Ok(()) }
    /// ```
    pub fn set_merge_operator(
        &self,
        merge_operator: impl MergeOperator + 'static,
    ) {
        let mut mo_write = self.merge_operator.write();
        *mo_write = Some(Box::new(merge_operator));
    }

    /// Create a double-ended iterator over the tuples of keys and
    /// values in this tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// use sled::IVec;
    /// db.insert(&[1], vec![10]);
    /// db.insert(&[2], vec![20]);
    /// db.insert(&[3], vec![30]);
    /// let mut iter = db.iter();
    /// assert_eq!(
    ///     iter.next().unwrap(),
    ///     Ok((IVec::from(&[1]), IVec::from(&[10])))
    /// );
    /// assert_eq!(
    ///     iter.next().unwrap(),
    ///     Ok((IVec::from(&[2]), IVec::from(&[20])))
    /// );
    /// assert_eq!(
    ///     iter.next().unwrap(),
    ///     Ok((IVec::from(&[3]), IVec::from(&[30])))
    /// );
    /// assert_eq!(iter.next(), None);
    /// # Ok(()) }
    /// ```
    pub fn iter(&self) -> Iter {
        self.range::<Vec<u8>, _>(..)
    }

    /// Create a double-ended iterator over tuples of keys and values,
    /// where the keys fall within the specified range.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// use sled::IVec;
    /// db.insert(&[0], vec![0])?;
    /// db.insert(&[1], vec![10])?;
    /// db.insert(&[2], vec![20])?;
    /// db.insert(&[3], vec![30])?;
    /// db.insert(&[4], vec![40])?;
    /// db.insert(&[5], vec![50])?;
    ///
    /// let start: &[u8] = &[2];
    /// let end: &[u8] = &[4];
    /// let mut r = db.range(start..end);
    /// assert_eq!(r.next().unwrap(), Ok((IVec::from(&[2]), IVec::from(&[20]))));
    /// assert_eq!(r.next().unwrap(), Ok((IVec::from(&[3]), IVec::from(&[30]))));
    /// assert_eq!(r.next(), None);
    ///
    /// let mut r = db.range(start..end).rev();
    /// assert_eq!(r.next().unwrap(), Ok((IVec::from(&[3]), IVec::from(&[30]))));
    /// assert_eq!(r.next().unwrap(), Ok((IVec::from(&[2]), IVec::from(&[20]))));
    /// assert_eq!(r.next(), None);
    /// # Ok(()) }
    /// ```
    pub fn range<K, R>(&self, range: R) -> Iter
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
    {
        let lo = match range.start_bound() {
            ops::Bound::Included(start) => {
                ops::Bound::Included(IVec::from(start.as_ref()))
            }
            ops::Bound::Excluded(start) => {
                ops::Bound::Excluded(IVec::from(start.as_ref()))
            }
            ops::Bound::Unbounded => ops::Bound::Included(IVec::from(&[])),
        };

        let hi = match range.end_bound() {
            ops::Bound::Included(end) => {
                ops::Bound::Included(IVec::from(end.as_ref()))
            }
            ops::Bound::Excluded(end) => {
                ops::Bound::Excluded(IVec::from(end.as_ref()))
            }
            ops::Bound::Unbounded => ops::Bound::Unbounded,
        };

        Iter {
            tree: self.clone(),
            hi,
            lo,
            cached_node: None,
            going_forward: true,
        }
    }

    /// Create an iterator over tuples of keys and values,
    /// where the all the keys starts with the given prefix.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// use sled::IVec;
    /// db.insert(&[0, 0, 0], vec![0, 0, 0])?;
    /// db.insert(&[0, 0, 1], vec![0, 0, 1])?;
    /// db.insert(&[0, 0, 2], vec![0, 0, 2])?;
    /// db.insert(&[0, 0, 3], vec![0, 0, 3])?;
    /// db.insert(&[0, 1, 0], vec![0, 1, 0])?;
    /// db.insert(&[0, 1, 1], vec![0, 1, 1])?;
    ///
    /// let prefix: &[u8] = &[0, 0];
    /// let mut r = db.scan_prefix(prefix);
    /// assert_eq!(
    ///     r.next(),
    ///     Some(Ok((IVec::from(&[0, 0, 0]), IVec::from(&[0, 0, 0]))))
    /// );
    /// assert_eq!(
    ///     r.next(),
    ///     Some(Ok((IVec::from(&[0, 0, 1]), IVec::from(&[0, 0, 1]))))
    /// );
    /// assert_eq!(
    ///     r.next(),
    ///     Some(Ok((IVec::from(&[0, 0, 2]), IVec::from(&[0, 0, 2]))))
    /// );
    /// assert_eq!(
    ///     r.next(),
    ///     Some(Ok((IVec::from(&[0, 0, 3]), IVec::from(&[0, 0, 3]))))
    /// );
    /// assert_eq!(r.next(), None);
    /// # Ok(()) }
    /// ```
    pub fn scan_prefix<P>(&self, prefix: P) -> Iter
    where
        P: AsRef<[u8]>,
    {
        let prefix_ref = prefix.as_ref();
        let mut upper = prefix_ref.to_vec();

        while let Some(last) = upper.pop() {
            if last < u8::max_value() {
                upper.push(last + 1);
                return self.range(prefix_ref..&upper);
            }
        }

        self.range(prefix..)
    }

    /// Returns the first key and value in the `Tree`, or
    /// `None` if the `Tree` is empty.
    pub fn first(&self) -> Result<Option<(IVec, IVec)>> {
        self.iter().next().transpose()
    }

    /// Returns the last key and value in the `Tree`, or
    /// `None` if the `Tree` is empty.
    pub fn last(&self) -> Result<Option<(IVec, IVec)>> {
        self.iter().next_back().transpose()
    }

    /// Atomically removes the maximum item in the `Tree` instance.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// db.insert(&[0], vec![0])?;
    /// db.insert(&[1], vec![10])?;
    /// db.insert(&[2], vec![20])?;
    /// db.insert(&[3], vec![30])?;
    /// db.insert(&[4], vec![40])?;
    /// db.insert(&[5], vec![50])?;
    ///
    /// assert_eq!(&db.pop_max()?.unwrap().0, &[5]);
    /// assert_eq!(&db.pop_max()?.unwrap().0, &[4]);
    /// assert_eq!(&db.pop_max()?.unwrap().0, &[3]);
    /// assert_eq!(&db.pop_max()?.unwrap().0, &[2]);
    /// assert_eq!(&db.pop_max()?.unwrap().0, &[1]);
    /// assert_eq!(&db.pop_max()?.unwrap().0, &[0]);
    /// assert_eq!(db.pop_max()?, None);
    /// # Ok(()) }
    /// ```
    pub fn pop_max(&self) -> Result<Option<(IVec, IVec)>> {
        loop {
            if let Some(first_res) = self.iter().next_back() {
                let first = first_res?;
                if self
                    .compare_and_swap::<_, _, &[u8]>(
                        &first.0,
                        Some(&first.1),
                        None,
                    )?
                    .is_ok()
                {
                    trace!("pop_max removed item {:?}", first);
                    return Ok(Some(first));
                }
            // try again
            } else {
                trace!("pop_max removed nothing from empty tree");
                return Ok(None);
            }
        }
    }

    /// Atomically removes the minimum item in the `Tree` instance.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// db.insert(&[0], vec![0])?;
    /// db.insert(&[1], vec![10])?;
    /// db.insert(&[2], vec![20])?;
    /// db.insert(&[3], vec![30])?;
    /// db.insert(&[4], vec![40])?;
    /// db.insert(&[5], vec![50])?;
    ///
    /// assert_eq!(&db.pop_min()?.unwrap().0, &[0]);
    /// assert_eq!(&db.pop_min()?.unwrap().0, &[1]);
    /// assert_eq!(&db.pop_min()?.unwrap().0, &[2]);
    /// assert_eq!(&db.pop_min()?.unwrap().0, &[3]);
    /// assert_eq!(&db.pop_min()?.unwrap().0, &[4]);
    /// assert_eq!(&db.pop_min()?.unwrap().0, &[5]);
    /// assert_eq!(db.pop_min()?, None);
    /// # Ok(()) }
    /// ```
    pub fn pop_min(&self) -> Result<Option<(IVec, IVec)>> {
        loop {
            if let Some(first_res) = self.iter().next() {
                let first = first_res?;
                if self
                    .compare_and_swap::<_, _, &[u8]>(
                        &first.0,
                        Some(&first.1),
                        None,
                    )?
                    .is_ok()
                {
                    trace!("pop_min removed item {:?}", first);
                    return Ok(Some(first));
                }
            // try again
            } else {
                trace!("pop_min removed nothing from empty tree");
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
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::new().temporary(true);
    /// # let db = config.open()?;
    /// db.insert(b"a", vec![0]);
    /// db.insert(b"b", vec![1]);
    /// assert_eq!(db.len(), 2);
    /// # Ok(()) }
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
            let _old = self.remove(key)?;
        }
        Ok(())
    }

    /// Returns the name of the tree.
    pub fn name(&self) -> IVec {
        self.tree_id.clone()
    }

    /// Returns the CRC32 of all keys and values
    /// in this Tree.
    ///
    /// This is O(N) and locks the underlying tree
    /// for the duration of the entire scan.
    pub fn checksum(&self) -> Result<u32> {
        let mut hasher = crc32fast::Hasher::new();
        let mut iter = self.iter();
        let _cc = concurrency_control::write();
        while let Some(kv_res) = iter.next_inner() {
            let (k, v) = kv_res?;
            hasher.update(&k);
            hasher.update(&v);
        }
        Ok(hasher.finalize())
    }

    fn split_node<'g>(
        &self,
        view: &View<'g>,
        parent_view: &Option<View<'g>>,
        root_pid: PageId,
        guard: &'g Guard,
    ) -> Result<()> {
        trace!("splitting node with pid {}", view.pid);
        // split node
        let (mut lhs, rhs) = view.deref().split();
        let rhs_lo = rhs.lo().to_vec();

        // install right side
        let (rhs_pid, rhs_ptr) = self.context.pagecache.allocate(rhs, guard)?;

        // replace node, pointing next to installed right
        lhs.set_next(Some(NonZeroU64::new(rhs_pid).unwrap()));
        let replace = self.context.pagecache.replace(
            view.pid,
            view.node_view.0,
            &lhs,
            guard,
        )?;
        #[cfg(feature = "metrics")]
        M.tree_child_split_attempt();
        if replace.is_err() {
            // if we failed, don't follow through with the
            // parent split or root hoist.
            let _new_stack = self
                .context
                .pagecache
                .free(rhs_pid, rhs_ptr, guard)?
                .expect("could not free allocated page");
            return Ok(());
        }
        #[cfg(feature = "metrics")]
        M.tree_child_split_success();

        // either install parent split or hoist root
        if let Some(parent_view) = parent_view {
            #[cfg(feature = "metrics")]
            M.tree_parent_split_attempt();
            let split_applied = parent_view.parent_split(&rhs_lo, rhs_pid);

            if split_applied.is_none() {
                // due to deep races, it's possible for the
                // parent to already have a node for this lo key.
                // if this is the case, we can skip the parent split
                // because it's probably going to fail anyway.
                return Ok(());
            }

            let parent = split_applied.unwrap();

            let replace = self.context.pagecache.replace(
                parent_view.pid,
                parent_view.node_view.0,
                &parent,
                guard,
            )?;
            trace!(
                "parent_split at {:?} child pid {} \
                parent pid {} success: {}",
                rhs_lo, rhs_pid, parent_view.pid, replace.is_ok()
            );
            if replace.is_ok() {
                #[cfg(feature = "metrics")]
                M.tree_parent_split_success();
            } else {
                // Parent splits are an optimization
                // so we don't need to care if we
                // failed.
            }
        } else {
            let _ = self.root_hoist(root_pid, rhs_pid, &rhs_lo, guard)?;
        }

        Ok(())
    }

    fn root_hoist(
        &self,
        from: PageId,
        to: PageId,
        at: &[u8],
        guard: &Guard,
    ) -> Result<bool> {
        #[cfg(feature = "metrics")]
        M.tree_root_split_attempt();
        // hoist new root, pointing to lhs & rhs

        let new_root = Node::new_hoisted_root(from, at, to);

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
            #[cfg(feature = "metrics")]
            M.tree_root_split_success();

            // we spin in a cas loop because it's possible
            // 2 threads are at this point, and we don't want
            // to cause roots to diverge between meta and
            // our version.
            while self
                .root
                .compare_exchange(from, new_root_pid, SeqCst, SeqCst)
                .is_err()
            {
                // `hint::spin_loop` requires Rust 1.49.
                #[allow(deprecated)]
                std::sync::atomic::spin_loop_hint();
            }

            Ok(true)
        } else {
            debug!(
                "root hoist from {} to {} failed: {:?}",
                from, new_root_pid, cas
            );
            let _new_stack = self
                .context
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
            let node_view_opt = self.context.pagecache.get(pid, guard)?;
            // if let Some((tree_ptr, ref leaf, size)) = &frag_opt {
            if let Some(node_view) = &node_view_opt {
                let size = node_view.0.log_size();
                let view = View { node_view: *node_view, pid, size };
                if view.merging_child.is_some() {
                    self.merge_node(
                        &view,
                        view.merging_child.unwrap().get(),
                        guard,
                    )?;
                } else {
                    return Ok(Some(view));
                }
            } else {
                return Ok(None);
            }
        }
    }

    // Returns the traversal path, completing any observed
    // partially complete splits or merges along the way.
    //
    // We intentionally leave the cyclometric complexity
    // high because attempts to split it up have made
    // the inherent complexity of the operation more
    // challenging to understand.
    #[allow(clippy::cognitive_complexity)]
    pub(crate) fn view_for_key<'g, K>(
        &self,
        key: K,
        guard: &'g Guard,
    ) -> Result<View<'g>>
    where
        K: AsRef<[u8]>,
    {
        #[cfg(any(test, feature = "lock_free_delays"))]
        const MAX_LOOPS: usize = usize::max_value();

        #[cfg(not(any(test, feature = "lock_free_delays")))]
        const MAX_LOOPS: usize = 1_000_000;

        #[cfg(feature = "metrics")]
        let _measure = Measure::new(&M.tree_traverse);

        let mut cursor = self.root.load(Acquire);
        let mut root_pid = cursor;
        let mut parent_view = None;
        let mut unsplit_parent = None;
        let mut took_leftmost_branch = false;

        // only merge or split nodes a few times
        let mut smo_budget = 3_u8;

        for _ in 0..MAX_LOOPS {
            macro_rules! retry {
                () => {
                    trace!(
                        "retrying at line {} when cursor was {}",
                        line!(),
                        cursor
                    );
                    smo_budget = smo_budget.saturating_sub(1);
                    cursor = self.root.load(Acquire);
                    root_pid = cursor;
                    parent_view = None;
                    unsplit_parent = None;
                    took_leftmost_branch = false;
                    continue;
                };
            }

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
                    view.merging_child.unwrap().get(),
                    guard,
                )?;
                retry!();
            } else if view.merging {
                // we missed the parent merge intention due to a benign race,
                // so go around again and try to help out if necessary
                retry!();
            }

            let overshot = key.as_ref() < view.lo();
            let undershot = if let Some(hi) = view.hi() {
                key.as_ref() >= hi
            } else {
                false
            };

            if overshot {
                // merge interfered, reload root and retry
                log::trace!("overshot searching for {:?} on node {:?}", key.as_ref(), view.deref());
                retry!();
            }

            if smo_budget > 0 && view.should_split() {
                self.split_node(&view, &parent_view, root_pid, guard)?;
                retry!();
            }

            if undershot {
                // half-complete split detect & completion
                let right_sibling = view
                    .next
                    .expect(
                        "if our hi bound is not Inf (inity), \
                         we should have a right sibling",
                    )
                    .get();
                trace!("seeking right on undershot node, from {} to {}", cursor, right_sibling);
                cursor = right_sibling;
                if unsplit_parent.is_none() && parent_view.is_some() {
                    unsplit_parent = parent_view.clone();
                } else if parent_view.is_none() && view.lo().is_empty() {
                    assert!(unsplit_parent.is_none());
                    assert_eq!(view.pid, root_pid);
                    // we have found a partially-split root
                    if self.root_hoist(
                        root_pid,
                        view.next.unwrap().get(),
                        view.hi().unwrap(),
                        guard,
                    )? {
                        #[cfg(feature = "metrics")]
                        M.tree_root_split_success();
                        retry!();
                    }
                }

                continue;
            } else if let Some(unsplit_parent) = unsplit_parent.take() {
                // we have found the proper page for
                // our cooperative parent split
                trace!(
                    "trying to apply split of child with \
                    lo key of {:?} to parent pid {}",
                    view.lo(),
                    unsplit_parent.pid
                );
                let split_applied =
                    unsplit_parent.parent_split(view.lo(), cursor);

                if split_applied.is_none() {
                    // Due to deep races, it's possible for the
                    // parent to already have a node for this lo key.
                    // if this is the case, we can skip the parent split
                    // because it's probably going to fail anyway.
                    //
                    // If a test is failing because of retrying in a
                    // loop here, this has happened often histically
                    // due to the Node::index_next_node method
                    // returning a child that is off-by-one to the
                    // left, always causing an undershoot.
                    log::trace!("failed to apply parent split of \
                        ({:?}, {}) to parent node {:?}",
                        view.lo(), cursor, unsplit_parent
                    );
                    retry!();
                }

                let parent: Node = split_applied.unwrap();

                #[cfg(feature = "metrics")]
                M.tree_parent_split_attempt();
                let replace = self.context.pagecache.replace(
                    unsplit_parent.pid,
                    unsplit_parent.node_view.0,
                    &parent,
                    guard,
                )?;
                if replace.is_ok() {
                    #[cfg(feature = "metrics")]
                    M.tree_parent_split_success();
                }
            }

            // detect whether a node is mergeable, and begin
            // the merge process.
            // NB we can never begin merging a node that is
            // the leftmost child of an index, because it
            // would be merged into a different index, which
            // would add considerable complexity to this already
            // fairly complex implementation.
            if smo_budget > 0
                && !took_leftmost_branch
                && parent_view.is_some()
                && view.should_merge()
            {
                let parent = parent_view.as_mut().unwrap();
                assert!(parent.merging_child.is_none());
                if parent.can_merge_child(cursor) {
                    let frag = Link::ParentMergeIntention(cursor);

                    let link = self.context.pagecache.link(
                        parent.pid,
                        parent.node_view.0,
                        frag,
                        guard,
                    )?;

                    if let Ok(new_parent_ptr) = link {
                        parent.node_view = NodeView(new_parent_ptr);
                        self.merge_node(parent, cursor, guard)?;
                        retry!();
                    }
                }
            }

            if view.is_index {
                let next = view.index_next_node(key.as_ref());
                log::trace!("found next {} from node {:?}", next.1, view.deref());
                took_leftmost_branch = next.0;
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

    fn cap_merging_child<'g>(
        &'g self,
        child_pid: PageId,
        guard: &'g Guard,
    ) -> Result<Option<View<'g>>> {
        // Get the child node and try to install a `MergeCap` frag.
        // In case we succeed, we break, otherwise we try from the start.
        loop {
            let mut child_view = if let Some(child_view) =
                self.view_for_pid(child_pid, guard)?
            {
                child_view
            } else {
                // the child was already freed, meaning
                // somebody completed this whole loop already
                return Ok(None);
            };

            if child_view.merging {
                trace!("child pid {} already merging", child_pid);
                return Ok(Some(child_view));
            }

            let install_frag = self.context.pagecache.link(
                child_pid,
                child_view.node_view.0,
                Link::ChildMergeCap,
                guard,
            )?;
            match install_frag {
                Ok(new_ptr) => {
                    trace!("child pid {} merge capped", child_pid);
                    child_view.node_view = NodeView(new_ptr);
                    return Ok(Some(child_view));
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
                    return Ok(None);
                }
            }
        }
    }

    fn install_parent_merge<'g>(
        &self,
        parent_view: &View<'g>,
        child_pid: PageId,
        guard: &'g Guard,
    ) -> Result<bool> {
        let mut parent_view = Cow::Borrowed(parent_view);
        loop {
            let linked = self.context.pagecache.link(
                parent_view.pid,
                parent_view.node_view.0,
                Link::ParentMergeConfirm,
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
                    return Ok(true);
                }
                Err(None) => {
                    trace!(
                        "ParentMergeConfirm \
                         failed on (now freed) parent pid {}",
                        parent_view.pid
                    );
                    return Ok(false);
                }
                Err(_) => {
                    let new_parent_view = if let Some(new_parent_view) =
                        self.view_for_pid(parent_view.pid, guard)?
                    {
                        trace!(
                            "failed to confirm merge \
                             on parent pid {}, trying again",
                            parent_view.pid
                        );
                        new_parent_view
                    } else {
                        trace!(
                            "failed to confirm merge \
                             on parent pid {}, which was freed",
                            parent_view.pid
                        );
                        return Ok(false);
                    };

                    if new_parent_view.merging_child.map(NonZeroU64::get)
                        != Some(child_pid)
                    {
                        trace!(
                            "someone else must have already \
                             completed the merge, and now the \
                             merging child for parent pid {} is {:?}",
                            new_parent_view.pid,
                            new_parent_view.merging_child
                        );
                        return Ok(false);
                    }

                    parent_view = Cow::Owned(new_parent_view);
                }
            }
        }
    }

    pub(crate) fn merge_node<'g>(
        &self,
        parent_view: &View<'g>,
        child_pid: PageId,
        guard: &'g Guard,
    ) -> Result<()> {
        trace!(
            "merging child pid {} of parent pid {}",
            child_pid,
            parent_view.pid
        );

        let child_view = if let Some(merging_child) =
            self.cap_merging_child(child_pid, guard)?
        {
            merging_child
        } else {
            return Ok(());
        };

        assert!(parent_view.is_index);
        let child_index = parent_view
            .iter_index_pids()
            .position(|pid| pid == child_pid)
            .unwrap();

        assert_ne!(
            child_index, 0,
            "merging child must not be the \
             leftmost child of its parent"
        );

        let mut merge_index = child_index - 1;

        // we assume caller only merges when
        // the node to be merged is not the
        // leftmost child.
        let mut cursor_pid = parent_view.iter_index_pids().nth(merge_index).unwrap();

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
                cursor_pid = parent_view.iter_index_pids().nth(merge_index).unwrap();

                continue;
            };

            // This means that `cursor_node` is the node we want to replace
            if cursor_view.next.map(NonZeroU64::get) == Some(child_pid) {
                trace!(
                    "found left sibling pid {} points to merging node pid {}",
                    cursor_view.pid,
                    child_pid
                );
                let cursor_node = cursor_view.node_view;

                let replacement = cursor_node.receive_merge(&child_view);
                let replace = self.context.pagecache.replace(
                    cursor_pid,
                    cursor_node.0,
                    &replacement,
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
            } else if cursor_view.hi() >= Some(child_view.lo()) {
                // we overshot the node being merged,
                trace!(
                    "cursor pid {} has hi key {:?}, which is \
                     >= merging child pid {}'s lo key of {:?}, breaking",
                    cursor_pid,
                    cursor_view.hi(),
                    child_pid,
                    child_view.lo()
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
                    cursor_pid = next.get();
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

        trace!(
            "trying to install parent merge \
             confirmation of merged child pid {} for parent pid {}",
            child_pid,
            parent_view.pid
        );

        let should_continue =
            self.install_parent_merge(parent_view, child_pid, guard)?;

        if !should_continue {
            return Ok(());
        }

        match self.context.pagecache.free(
            child_pid,
            child_view.node_view.0,
            guard,
        )? {
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
                        trace!(
                            "encountered Free node pid {} while GC'ing tree",
                            pid
                        );
                        break;
                    };

                let ret = self.context.pagecache.free(
                    pid,
                    cursor_view.node_view.0,
                    &guard,
                )?;

                if ret.is_ok() {
                    let next_pid = if let Some(next_pid) = cursor_view.next {
                        next_pid
                    } else {
                        break;
                    };
                    assert_ne!(pid, next_pid.get());
                    pid = next_pid.get();
                }
            }
        }

        Ok(())
    }

    #[doc(hidden)]
    pub fn verify_integrity(&self) -> Result<()> {
        // verification happens in attempt_fmt
        self.attempt_fmt()?;
        Ok(())
    }

    // format and verify tree integrity
    fn attempt_fmt(&self) -> Result<Option<String>> {
        let mut f = String::new();
        let guard = pin();

        let mut pid = self.root.load(Acquire);
        if pid == 0 {
            panic!("somehow tree root was 0");
        }
        let mut left_most = pid;
        let mut level = 0;
        let mut expected_pids = FastSet8::default();
        let mut referenced_pids = FastSet8::default();
        let mut loop_detector = FastSet8::default();

        expected_pids.insert(pid);

        f.push_str("\tlevel 0:\n");

        loop {
            let get_res = self.view_for_pid(pid, &guard);
            let node = match get_res {
                Ok(Some(ref view)) => {
                    expected_pids.remove(&pid);
                    if loop_detector.contains(&pid) {
                        if cfg!(feature = "testing") {
                            panic!(
                                "detected a loop while iterating over the Tree. \
                                pid {} was encountered multiple times",
                                pid
                            );
                        } else {
                            error!(
                                "detected a loop while iterating over the Tree. \
                                pid {} was encountered multiple times",
                                pid
                            );
                        }
                    } else {
                        loop_detector.insert(pid);
                    }

                    view.deref()
                }
                Ok(None) => {
                    if cfg!(feature = "testing") {
                        error!(
                            "Tree::fmt failed to read node pid {} \
                             that has been freed",
                            pid,
                        );
                        return Ok(None);
                    } else {
                        error!(
                            "Tree::fmt failed to read node pid {} \
                             that has been freed",
                            pid,
                        );
                    }
                    break;
                }
                Err(e) => {
                    error!(
                        "hit error while trying to pull pid {}: {:?}",
                        pid, e
                    );
                    return Err(e);
                }
            };

            f.push_str(&format!("\t\t{}: {:?}\n", pid, node));

            if node.is_index {
                for pid in node.iter_index_pids() {
                    referenced_pids.insert(pid);
                }
            }

            if let Some(next_pid) = node.next {
                pid = next_pid.get();
            } else {
                // we've traversed our level, time to bump down
                let left_get_res = self.view_for_pid(left_most, &guard);
                let left_node = if let Ok(Some(ref view)) = left_get_res {
                    view
                } else {
                    panic!(
                        "pagecache returned non-base node: {:?}",
                        left_get_res
                    )
                };

                if left_node.is_index {
                    if let Some(next_pid) = left_node.iter_index_pids().next() {
                        pid = next_pid;
                        left_most = next_pid;
                        log::trace!("set left_most to {}", next_pid);
                        level += 1;
                        f.push_str(&format!("\n\tlevel {}:\n", level));
                        assert!(
                            expected_pids.is_empty(),
                            "expected pids {:?} but never \
                                saw them on this level. tree so far: {}",
                            expected_pids,
                            f
                        );
                        std::mem::swap(
                            &mut expected_pids,
                            &mut referenced_pids,
                        );
                    } else {
                        panic!("trying to debug print empty index node");
                    }
                } else {
                    // we've reached the end of our tree, all leafs are on
                    // the lowest level.
                    break;
                }
            }
        }

        Ok(Some(f))
    }
}

impl Debug for Tree {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> std::result::Result<(), fmt::Error> {
        f.write_str("Tree: \n\t")?;
        self.context.pagecache.fmt(f)?;

        if let Some(fmt) = self.attempt_fmt().map_err(|_| std::fmt::Error)? {
            f.write_str(&fmt)?;
            return Ok(());
        }

        if cfg!(feature = "testing") {
            panic!(
                "failed to fmt Tree due to expected page disappearing part-way through"
            );
        } else {
            log::error!(
                "failed to fmt Tree due to expected page disappearing part-way through"
            );
            Ok(())
        }
    }
}

/// Compare and swap result.
///
/// It returns `Ok(Ok(()))` if operation finishes successfully and
///     - `Ok(Err(CompareAndSwapError(current, proposed)))` if operation failed
///       to setup a new value. `CompareAndSwapError` contains current and
///       proposed values.
///     - `Err(Error::Unsupported)` if the database is opened in read-only mode.
///       otherwise.
pub type CompareAndSwapResult =
    Result<std::result::Result<(), CompareAndSwapError>>;

impl From<Error> for CompareAndSwapResult {
    fn from(error: Error) -> Self {
        Err(error)
    }
}

/// Compare and swap error.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompareAndSwapError {
    /// The current value which caused your CAS to fail.
    pub current: Option<IVec>,
    /// Returned value that was proposed unsuccessfully.
    pub proposed: Option<IVec>,
}

impl fmt::Display for CompareAndSwapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Compare and swap conflict")
    }
}

impl std::error::Error for CompareAndSwapError {}
