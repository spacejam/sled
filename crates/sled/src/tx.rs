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
//! }).unwrap();
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
//! }).unwrap();
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
//! (&unprocessed, &processed).transaction(|(unprocessed, processed)| {
//!     let unprocessed_item = unprocessed.remove(b"k3")?.unwrap();
//!     let mut processed_item = b"yappin' ".to_vec();
//!     processed_item.extend_from_slice(&unprocessed_item);
//!     processed.insert(b"k3", processed_item)?;
//!     Ok(())
//! }).unwrap();
//!
//! assert_eq!(unprocessed.get(b"k3").unwrap(), None);
//! assert_eq!(&processed.get(b"k3").unwrap().unwrap(), b"yappin' ligers");
//! ```

#![allow(unused)]
#![allow(missing_docs)]

use std::{cell::RefCell, collections::HashMap, sync::Arc};

use super::*;

/// A transaction that will
/// be applied atomically to the
/// Tree.
pub struct TransactionalTree<'a> {
    pub(super) tree: &'a Tree,
    pub(super) writes: RefCell<HashMap<IVec, Option<IVec>>>,
    pub(super) read_cache: RefCell<HashMap<IVec, Option<IVec>>>,
    pub(super) locks: RefCell<Vec<parking_lot::RwLockWriteGuard<'a, ()>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionError {
    Conflict,
    Abort,
    Storage(Error),
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

impl<'a> TransactionalTree<'a> {
    /// Set a key to a new value
    pub fn insert<K, V>(
        &self,
        key: K,
        value: V,
    ) -> TransactionResult<Option<IVec>>
    where
        IVec: From<K>,
        IVec: From<V>,
        K: AsRef<[u8]>,
    {
        let old = self.get(key.as_ref())?;
        let mut writes = self.writes.borrow_mut();
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
        writes.insert(IVec::from(key), None);
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
        reads.insert(key.as_ref().into(), get.clone());

        Ok(get)
    }

    /// Atomically apply multiple inserts and removals.
    pub fn apply_batch(&self, batch: Batch) -> TransactionResult<()> {
        for (k, v_opt) in batch.writes {
            if let Some(v) = v_opt {
                self.insert(k, v)?;
            } else {
                self.remove(k)?;
            }
        }
        Ok(())
    }

    fn stage(&self) -> bool {
        let mut locks = self.locks.borrow_mut();
        let guard = self.tree.concurrency_control.write();
        locks.push(guard);
        true
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
                self.tree.insert_inner(k, v)?;
            } else {
                self.tree.remove_inner(k)?;
            }
        }
        Ok(())
    }
}

pub struct TransactionalTrees<'a> {
    inner: Vec<TransactionalTree<'a>>,
}

impl<'a> TransactionalTrees<'a> {
    fn stage(&self) -> bool {
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
        for (_, idx) in tree_idxs {
            if idx == last_idx {
                // prevents us from double-locking
                continue;
            }
            last_idx = idx;
            if !self.inner[idx].stage() {
                return false;
            }
        }
        true
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

    fn commit(self) -> Result<()> {
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

    fn make_overlay(&self) -> TransactionalTrees<'_>;

    fn view_overlay(overlay: &TransactionalTrees<'_>) -> Self::View;

    fn transaction<F, R>(&self, f: F) -> TransactionResult<R>
    where
        F: Fn(Self::View) -> TransactionResult<R>,
    {
        loop {
            let tt = self.make_overlay();
            let view = Self::view_overlay(&tt);
            if !tt.stage() {
                tt.unstage();
                continue;
            }
            let ret = f(view);
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

impl<'a> Transactional for &'a Tree {
    type View = &'static TransactionalTree<'static>;

    fn make_overlay(&self) -> TransactionalTrees<'_> {
        TransactionalTrees {
            inner: vec![TransactionalTree {
                tree: &self,
                writes: Default::default(),
                read_cache: Default::default(),
                locks: Default::default(),
            }],
        }
    }

    fn view_overlay(overlay: &TransactionalTrees<'_>) -> Self::View {
        unsafe {
            let unsafe_ptr: &'static TransactionalTree<'static> =
                std::mem::transmute(&overlay.inner[0]);
            &*unsafe_ptr
        }
    }
}

impl<A, B> Transactional for (A, B)
where
    A: AsRef<Tree>,
    B: AsRef<Tree>,
{
    type View = (
        &'static TransactionalTree<'static>,
        &'static TransactionalTree<'static>,
    );

    fn make_overlay(&self) -> TransactionalTrees<'_> {
        TransactionalTrees {
            inner: vec![
                TransactionalTree {
                    tree: self.0.as_ref(),
                    writes: Default::default(),
                    read_cache: Default::default(),
                    locks: Default::default(),
                },
                TransactionalTree {
                    tree: self.1.as_ref(),
                    writes: Default::default(),
                    read_cache: Default::default(),
                    locks: Default::default(),
                },
            ],
        }
    }

    fn view_overlay(overlay: &TransactionalTrees<'_>) -> Self::View {
        let t1 = unsafe {
            let unsafe_ptr: &'static TransactionalTree<'static> =
                std::mem::transmute(&overlay.inner[0]);
            &*unsafe_ptr
        };
        let t2 = unsafe {
            let unsafe_ptr: &'static TransactionalTree<'static> =
                std::mem::transmute(&overlay.inner[1]);
            &*unsafe_ptr
        };
        (t1, t2)
    }
}
