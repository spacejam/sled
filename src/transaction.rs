//! Fully serializable (ACID) multi-`Tree` transactions
//!
//! # Examples
//! ```
//! # use sled::{TransactionResult, Config};
//! # fn main() -> TransactionResult<()> {
//!
//! let config = Config::new().temporary(true);
//! let db1 = config.open().unwrap();
//! let db = db1.open_tree(b"a").unwrap();
//!
//! // Use write-only transactions as a writebatch:
//! db.transaction(|db| {
//!     db.insert(b"k1", b"cats")?;
//!     db.insert(b"k2", b"dogs")?;
//!     Ok(())
//! })?;
//!
//! // Atomically swap two items:
//! db.transaction(|db| {
//!     let v1_option = db.remove(b"k1")?;
//!     let v1 = v1_option.unwrap();
//!     let v2_option = db.remove(b"k2")?;
//!     let v2 = v2_option.unwrap();
//!
//!     db.insert(b"k1", v2)?;
//!     db.insert(b"k2", v1)?;
//!
//!     Ok(())
//! })?;
//!
//! assert_eq!(&db.get(b"k1")?.unwrap(), b"dogs");
//! assert_eq!(&db.get(b"k2")?.unwrap(), b"cats");
//! # Ok(())
//! # }
//! ```
//!
//! Transactions also work on tuples of `Tree`s,
//! preserving serializable ACID semantics!
//! In this example, we treat two trees like a
//! work queue, atomically apply updates to
//! data and move them from the unprocessed `Tree`
//! to the processed `Tree`.
//!
//! ```
//! use sled::{Config, Transactional};
//!
//! let config = Config::new().temporary(true);
//! let db = config.open().unwrap();
//!
//! let unprocessed = db.open_tree(b"unprocessed items").unwrap();
//! let processed = db.open_tree(b"processed items").unwrap();
//!
//! // An update somehow gets into the tree, which we
//! // later trigger the atomic processing of.
//! unprocessed.insert(b"k3", b"ligers").unwrap();
//!
//! // Atomically process the new item and move it
//! // between `Tree`s.
//! (&unprocessed, &processed)
//!     .transaction(|(unprocessed, processed)| {
//!         let unprocessed_item = unprocessed.remove(b"k3")?.unwrap();
//!         let mut processed_item = b"yappin' ".to_vec();
//!         processed_item.extend_from_slice(&unprocessed_item);
//!         processed.insert(b"k3", processed_item)?;
//!         Ok(())
//!     })
//!     .unwrap();
//!
//! assert_eq!(unprocessed.get(b"k3").unwrap(), None);
//! assert_eq!(&processed.get(b"k3").unwrap().unwrap(), b"yappin' ligers");
//! ```
use std::{cell::RefCell, collections::HashMap, fmt, rc::Rc};

use crate::{pin, Batch, Error, Guard, IVec, Protector, Result, Tree};

/// A transaction that will
/// be applied atomically to the
/// Tree.
#[derive(Clone)]
pub struct TransactionalTree {
    pub(super) tree: Tree,
    pub(super) writes: Rc<RefCell<HashMap<IVec, Option<IVec>>>>,
    pub(super) read_cache: Rc<RefCell<HashMap<IVec, Option<IVec>>>>,
}

/// An error type that is returned from the closure
/// passed to the `transaction` method.
#[derive(Debug, Clone, PartialEq)]
pub enum UnabortableTransactionError {
    /// An internal conflict has occurred and the `transaction` method will
    /// retry the passed-in closure until it succeeds. This should never be
    /// returned directly from the user's closure, as it will create an
    /// infinite loop that never returns. This is why it is hidden.
    Conflict,
    /// A serious underlying storage issue has occurred that requires
    /// attention from an operator or a remediating system, such as
    /// corruption.
    Storage(Error),
}

impl fmt::Display for UnabortableTransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use UnabortableTransactionError::*;
        match self {
            Conflict => write!(f, "Conflict during transaction"),
            Storage(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for UnabortableTransactionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            UnabortableTransactionError::Storage(ref e) => Some(e),
            _ => None,
        }
    }
}

pub type UnabortableTransactionResult<T> =
    std::result::Result<T, UnabortableTransactionError>;

impl From<Error> for UnabortableTransactionError {
    fn from(error: Error) -> Self {
        UnabortableTransactionError::Storage(error)
    }
}

impl<E> From<UnabortableTransactionError> for ConflictableTransactionError<E> {
    fn from(error: UnabortableTransactionError) -> Self {
        match error {
            UnabortableTransactionError::Conflict => {
                ConflictableTransactionError::Conflict
            }
            UnabortableTransactionError::Storage(error) => {
                ConflictableTransactionError::Storage(error)
            }
        }
    }
}

/// An error type that is returned from the closure
/// passed to the `transaction` method.
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictableTransactionError<T = Error> {
    /// A user-provided error type that indicates the transaction should abort.
    /// This is passed into the return value of `transaction` as a direct Err
    /// instance, rather than forcing users to interact with this enum
    /// directly.
    Abort(T),
    #[doc(hidden)]
    /// An internal conflict has occurred and the `transaction` method will
    /// retry the passed-in closure until it succeeds. This should never be
    /// returned directly from the user's closure, as it will create an
    /// infinite loop that never returns. This is why it is hidden.
    Conflict,
    /// A serious underlying storage issue has occurred that requires
    /// attention from an operator or a remediating system, such as
    /// corruption.
    Storage(Error),
}

impl<E: fmt::Display> fmt::Display for ConflictableTransactionError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ConflictableTransactionError::*;
        match self {
            Abort(e) => e.fmt(f),
            Conflict => write!(f, "Conflict during transaction"),
            Storage(e) => e.fmt(f),
        }
    }
}

impl<E: std::error::Error> std::error::Error
    for ConflictableTransactionError<E>
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConflictableTransactionError::Storage(ref e) => Some(e),
            _ => None,
        }
    }
}

/// An error type that is returned from the closure
/// passed to the `transaction` method.
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionError<T = Error> {
    /// A user-provided error type that indicates the transaction should abort.
    /// This is passed into the return value of `transaction` as a direct Err
    /// instance, rather than forcing users to interact with this enum
    /// directly.
    Abort(T),
    /// A serious underlying storage issue has occurred that requires
    /// attention from an operator or a remediating system, such as
    /// corruption.
    Storage(Error),
}

impl<E: fmt::Display> fmt::Display for TransactionError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TransactionError::*;
        match self {
            Abort(e) => e.fmt(f),
            Storage(e) => e.fmt(f),
        }
    }
}

impl<E: std::error::Error> std::error::Error for TransactionError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TransactionError::Storage(ref e) => Some(e),
            _ => None,
        }
    }
}

/// A transaction-related `Result` which is used for transparently handling
/// concurrency-related conflicts when running transaction closures.
pub type ConflictableTransactionResult<T, E = ()> =
    std::result::Result<T, ConflictableTransactionError<E>>;

impl<T> From<Error> for ConflictableTransactionError<T> {
    fn from(error: Error) -> Self {
        ConflictableTransactionError::Storage(error)
    }
}

/// A transaction-related `Result` which is used for returning the
/// final result of a transaction after potentially running the provided
/// closure several times due to underlying conflicts.
pub type TransactionResult<T, E = ()> =
    std::result::Result<T, TransactionError<E>>;

impl<T> From<Error> for TransactionError<T> {
    fn from(error: Error) -> Self {
        TransactionError::Storage(error)
    }
}

impl TransactionalTree {
    /// Set a key to a new value
    pub fn insert<K, V>(
        &self,
        key: K,
        value: V,
    ) -> UnabortableTransactionResult<Option<IVec>>
    where
        IVec: From<K> + From<V>,
        K: AsRef<[u8]>,
    {
        let old = self.get(key.as_ref())?;
        let mut writes = self.writes.borrow_mut();
        let _last_write =
            writes.insert(IVec::from(key), Some(IVec::from(value)));
        Ok(old)
    }

    /// Remove a key
    pub fn remove<K>(
        &self,
        key: K,
    ) -> UnabortableTransactionResult<Option<IVec>>
    where
        IVec: From<K>,
        K: AsRef<[u8]>,
    {
        let old = self.get(key.as_ref());
        let mut writes = self.writes.borrow_mut();
        let _last_write = writes.insert(IVec::from(key), None);
        old
    }

    /// Get the value associated with a key
    pub fn get<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> UnabortableTransactionResult<Option<IVec>> {
        let writes = self.writes.borrow();
        if let Some(first_try) = writes.get(key.as_ref()) {
            return Ok(first_try.clone());
        }
        let mut reads = self.read_cache.borrow_mut();
        if let Some(second_try) = reads.get(key.as_ref()) {
            return Ok(second_try.clone());
        }

        // not found in a cache, need to hit the backing db
        let guard = pin();
        let get = self.tree.get_inner(key.as_ref(), &guard)?;
        let last = reads.insert(key.as_ref().into(), get.clone());
        assert!(last.is_none());

        Ok(get)
    }

    /// Atomically apply multiple inserts and removals.
    pub fn apply_batch(
        &self,
        batch: Batch,
    ) -> UnabortableTransactionResult<()> {
        for (k, v_opt) in batch.writes {
            if let Some(v) = v_opt {
                let _old = self.insert(k, v)?;
            } else {
                let _old = self.remove(k)?;
            }
        }
        Ok(())
    }

    fn stage(&self) -> UnabortableTransactionResult<Vec<Protector<'_>>> {
        let protector = self.tree.concurrency_control.write();
        Ok(vec![protector])
    }

    fn unstage(&self) {
        unimplemented!()
    }

    const fn validate(&self) -> bool {
        true
    }

    fn commit(&self) -> Result<()> {
        let writes = self.writes.borrow();
        let guard = pin();
        for (k, v_opt) in &*writes {
            if let Some(v) = v_opt {
                let _old = self.tree.insert_inner(k, v, &guard)?;
            } else {
                let _old = self.tree.remove_inner(k, &guard)?;
            }
        }
        Ok(())
    }
}

pub struct TransactionalTrees {
    inner: Vec<TransactionalTree>,
}

impl TransactionalTrees {
    fn stage(&self) -> UnabortableTransactionResult<Vec<Protector<'_>>> {
        // we want to stage our trees in
        // lexicographic order to guarantee
        // no deadlocks should they block
        // on mutexes in their own staging
        // implementations.
        let mut tree_idxs: Vec<(&[u8], usize)> = self
            .inner
            .iter()
            .enumerate()
            .map(|(idx, t)| (&*t.tree.tree_id, idx))
            .collect();
        tree_idxs.sort_unstable();

        let mut last_idx = usize::max_value();
        let mut all_guards = vec![];
        for (_, idx) in tree_idxs {
            if idx == last_idx {
                // prevents us from double-locking
                continue;
            }
            last_idx = idx;
            let mut guards = self.inner[idx].stage()?;
            all_guards.append(&mut guards);
        }
        Ok(all_guards)
    }

    fn unstage(&self) {
        for tree in &self.inner {
            tree.unstage();
        }
    }

    fn validate(&self) -> bool {
        for tree in &self.inner {
            if !tree.validate() {
                return false;
            }
        }
        true
    }

    fn commit(&self, guard: &Guard) -> Result<()> {
        let peg = self.inner[0].tree.context.pin_log(guard)?;
        for tree in &self.inner {
            tree.commit()?;
        }

        // when the peg drops, it ensures all updates
        // written to the log since its creation are
        // recovered atomically
        peg.seal_batch(guard)
    }
}

/// A simple constructor for `Err(TransactionError::Abort(_))`
pub fn abort<A, T>(t: T) -> ConflictableTransactionResult<A, T> {
    Err(ConflictableTransactionError::Abort(t))
}

/// A type that may be transacted on in sled transactions.
pub trait Transactional<E = ()> {
    /// An internal reference to an internal proxy type that
    /// mediates transactional reads and writes.
    type View;

    /// An internal function for creating a top-level
    /// transactional structure.
    fn make_overlay(&self) -> TransactionalTrees;

    /// An internal function for viewing the transactional
    /// subcomponents based on the top-level transactional
    /// structure.
    fn view_overlay(overlay: &TransactionalTrees) -> Self::View;

    /// Runs a transaction, possibly retrying the passed-in closure if
    /// a concurrent conflict is detected that would cause a violation
    /// of serializability. This is the only trait method that
    /// you're most likely to use directly.
    fn transaction<F, A>(&self, f: F) -> TransactionResult<A, E>
    where
        F: Fn(&Self::View) -> ConflictableTransactionResult<A, E>,
    {
        loop {
            let tt = self.make_overlay();
            let view = Self::view_overlay(&tt);

            // NB locks must exist until this function returns.
            let _locks = if let Ok(l) = tt.stage() {
                l
            } else {
                tt.unstage();
                continue;
            };
            let ret = f(&view);
            if !tt.validate() {
                tt.unstage();
                continue;
            }
            match ret {
                Ok(r) => {
                    let guard = pin();
                    tt.commit(&guard)?;
                    return Ok(r);
                }
                Err(ConflictableTransactionError::Abort(e)) => {
                    return Err(TransactionError::Abort(e));
                }
                Err(ConflictableTransactionError::Conflict) => continue,
                Err(ConflictableTransactionError::Storage(other)) => {
                    return Err(TransactionError::Storage(other));
                }
            }
        }
    }
}

impl<E> Transactional<E> for &Tree {
    type View = TransactionalTree;

    fn make_overlay(&self) -> TransactionalTrees {
        TransactionalTrees {
            inner: vec![TransactionalTree {
                tree: (*self).clone(),
                writes: Default::default(),
                read_cache: Default::default(),
            }],
        }
    }

    fn view_overlay(overlay: &TransactionalTrees) -> Self::View {
        overlay.inner[0].clone()
    }
}

impl<E> Transactional<E> for &&Tree {
    type View = TransactionalTree;

    fn make_overlay(&self) -> TransactionalTrees {
        TransactionalTrees {
            inner: vec![TransactionalTree {
                tree: (**self).clone(),
                writes: Default::default(),
                read_cache: Default::default(),
            }],
        }
    }

    fn view_overlay(overlay: &TransactionalTrees) -> Self::View {
        overlay.inner[0].clone()
    }
}

impl<E> Transactional<E> for Tree {
    type View = TransactionalTree;

    fn make_overlay(&self) -> TransactionalTrees {
        TransactionalTrees {
            inner: vec![TransactionalTree {
                tree: self.clone(),
                writes: Default::default(),
                read_cache: Default::default(),
            }],
        }
    }

    fn view_overlay(overlay: &TransactionalTrees) -> Self::View {
        overlay.inner[0].clone()
    }
}

macro_rules! repeat_type {
    ($t:ty, ($($literals:literal),*)) => {
        repeat_type!(IMPL $t, (), ($($literals),*))
    };
    (IMPL $t:ty, (), ($first:literal, $($rest:literal),*)) => {
        repeat_type!(IMPL $t, ($t), ($($rest),*))
    };
    (IMPL $t:ty, ($($partial:tt),*), ($first:literal, $($rest:literal),*)) => {
        repeat_type!(IMPL $t, ($t, $($partial),*), ($($rest),*))
    };
    (IMPL $t:ty, ($($partial:tt),*), ($last:literal)) => {
        ($($partial),*, $t)
    };
}

macro_rules! impl_transactional_tuple_trees {
    ($($indices:tt),+) => {
        impl Transactional for repeat_type!(&Tree, ($($indices),+)) {
            type View = repeat_type!(TransactionalTree, ($($indices),+));

            fn make_overlay(&self) -> TransactionalTrees {
                TransactionalTrees {
                    inner: vec![
                        $(
                            TransactionalTree {
                                tree: self.$indices.clone(),
                                writes: Default::default(),
                                read_cache: Default::default(),
                            }
                        ),+
                    ],
                }
            }

            fn view_overlay(overlay: &TransactionalTrees) -> Self::View {
                (
                    $(
                        overlay.inner[$indices].clone()
                    ),+
                )
            }
        }
    };
}

impl_transactional_tuple_trees!(0, 1);
impl_transactional_tuple_trees!(0, 1, 2);
impl_transactional_tuple_trees!(0, 1, 2, 3);
impl_transactional_tuple_trees!(0, 1, 2, 3, 4);
impl_transactional_tuple_trees!(0, 1, 2, 3, 4, 5);
impl_transactional_tuple_trees!(0, 1, 2, 3, 4, 5, 6);
impl_transactional_tuple_trees!(0, 1, 2, 3, 4, 5, 6, 7);
impl_transactional_tuple_trees!(0, 1, 2, 3, 4, 5, 6, 7, 8);
impl_transactional_tuple_trees!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
impl_transactional_tuple_trees!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
impl_transactional_tuple_trees!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
impl_transactional_tuple_trees!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
impl_transactional_tuple_trees!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58,
    59
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58,
    59, 60
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58,
    59, 60, 61
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58,
    59, 60, 61, 62
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58,
    59, 60, 61, 62, 63
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58,
    59, 60, 61, 62, 63, 64
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58,
    59, 60, 61, 62, 63, 64, 65
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58,
    59, 60, 61, 62, 63, 64, 65, 66
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58,
    59, 60, 61, 62, 63, 64, 65, 66, 67
);
impl_transactional_tuple_trees!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58,
    59, 60, 61, 62, 63, 64, 65, 66, 67, 68
);
