//! Fully serializable (ACID) multi-`Tree` transactions
//!
//! # Examples
//!
//! ```
//! use sled::{Db, Transactional};
//!
//! let db = Db::open("tx_db").unwrap();
//!
//! // Use write-only transactions as a writebatch:
//! db.transaction(|db| {
//!     db.insert(b"k1", b"cats")?;
//!     db.insert(b"k2", b"dogs")?;
//!     Ok(())
//! })
//! .unwrap();
//!
//! // Atomically swap two items:
//! db.transaction(|db| {
//!     let v1_option = db.remove(b"k1")?;
//!     let v1 = v1_option.unwrap();
//!     let v2_option = db.remove(b"k2")?;
//!     let v2 = v2_option.unwrap();
//!
//!     db.insert(b"k1", v2);
//!     db.insert(b"k2", v1);
//!
//!     Ok(())
//! })
//! .unwrap();
//!
//! assert_eq!(&db.get(b"k1").unwrap().unwrap(), b"dogs");
//! assert_eq!(&db.get(b"k2").unwrap().unwrap(), b"cats");
//!
//! // Transactions also work on tuples of `Tree`s,
//! // preserving serializable ACID semantics!
//! // In this example, we treat two trees like a
//! // work queue, atomically apply updates to
//! // data and move them from the unprocessed `Tree`
//! // to the processed `Tree`.
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

#![allow(unused)]
#![allow(missing_docs)]

use parking_lot::RwLockWriteGuard;
use std::{cell::RefCell, collections::HashMap, fmt, rc::Rc, sync::Arc};

use crate::{Batch, Error, IVec, Result, Tree};

/// A transaction that will
/// be applied atomically to the
/// Tree.
#[derive(Clone)]
pub struct TransactionalTree {
    pub(super) tree: Tree,
    pub(super) writes: Rc<RefCell<HashMap<IVec, Option<IVec>>>>,
    pub(super) read_cache: Rc<RefCell<HashMap<IVec, Option<IVec>>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionError {
    Conflict,
    Abort,
    Storage(Error),
}

impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TransactionError::*;
        match self {
            Conflict => write!(f, "Conflict during transaction"),
            Abort => write!(f, "Transaction was aborted"),
            Storage(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for TransactionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TransactionError::Storage(ref e) => Some(e),
            _ => None,
        }
    }
}

pub type TransactionResult<T> = std::result::Result<T, TransactionError>;

fn abort() -> TransactionError {
    TransactionError::Abort
}

impl From<Error> for TransactionError {
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
    ) -> TransactionResult<Option<IVec>>
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
    pub fn remove<K>(&self, key: K) -> TransactionResult<Option<IVec>>
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
    ) -> TransactionResult<Option<IVec>> {
        let writes = self.writes.borrow();
        if let Some(first_try) = writes.get(key.as_ref()) {
            return Ok(first_try.clone());
        }
        let mut reads = self.read_cache.borrow_mut();
        if let Some(second_try) = reads.get(key.as_ref()) {
            return Ok(second_try.clone());
        }

        // not found in a cache, need to hit the backing db
        let get = self.tree.get_inner(key.as_ref())?;
        let last = reads.insert(key.as_ref().into(), get.clone());
        assert!(last.is_none());

        Ok(get)
    }

    /// Atomically apply multiple inserts and removals.
    pub fn apply_batch(&self, batch: Batch) -> TransactionResult<()> {
        for (k, v_opt) in batch.writes {
            if let Some(v) = v_opt {
                let _old = self.insert(k, v)?;
            } else {
                let _old = self.remove(k)?;
            }
        }
        Ok(())
    }

    fn stage(&self) -> TransactionResult<Vec<RwLockWriteGuard<'_, ()>>> {
        let guard = self.tree.concurrency_control.write();
        Ok(vec![guard])
    }

    fn unstage(&self) {
        unimplemented!()
    }

    fn validate(&self) -> bool {
        true
    }

    fn commit(&self) -> Result<()> {
        let mut writes = self.writes.borrow_mut();
        for (k, v_opt) in &*writes {
            if let Some(v) = v_opt {
                let _old = self.tree.insert_inner(k, v)?;
            } else {
                let _old = self.tree.remove_inner(k)?;
            }
        }
        Ok(())
    }
}

pub struct TransactionalTrees {
    inner: Vec<TransactionalTree>,
}

impl TransactionalTrees {
    fn stage(&self) -> TransactionResult<Vec<RwLockWriteGuard<'_, ()>>> {
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

    fn commit(&self) -> Result<()> {
        let peg = self.inner[0].tree.context.pin_log()?;
        for tree in &self.inner {
            tree.commit()?;
        }

        // when the peg drops, it ensures all updates
        // written to the log since its creation are
        // recovered atomically
        peg.seal_batch()
    }
}

pub trait Transactional {
    type View;

    fn make_overlay(&self) -> TransactionalTrees;

    fn view_overlay(overlay: &TransactionalTrees) -> Self::View;

    fn transaction<F, R>(&self, f: F) -> TransactionResult<R>
    where
        F: Fn(&Self::View) -> TransactionResult<R>,
    {
        loop {
            let tt = self.make_overlay();
            let view = Self::view_overlay(&tt);
            let locks = if let Ok(l) = tt.stage() {
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
                    tt.commit()?;
                    return Ok(r);
                }
                Err(TransactionError::Abort) => {
                    return Err(TransactionError::Abort);
                }
                Err(TransactionError::Conflict) => continue,
                Err(TransactionError::Storage(e)) => {
                    return Err(TransactionError::Storage(e));
                }
            }
        }
    }
}

impl Transactional for &Tree {
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
