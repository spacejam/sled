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
//!     db.insert(b"k1", b"cats");
//!     db.insert(b"k2", b"dogs");
//!     Ok(())
//! }).unwrap().unwrap();
//!
//! // Atomically swap two items:
//! db.transaction(|db| {
//!     let v1_option = db.get(b"k1")?;
//!     let v1 = v1_option.unwrap();
//!     let v2_option = db.get(b"k2")?;
//!     let v2 = v2_option.unwrap();
//!
//!     db.insert(b"k1", v2);
//!     db.insert(b"k2", v1);
//!     Ok(())
//! }).unwrap().unwrap();
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
//! }).unwrap().unwrap();
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
    pub(super) cache: RefCell<HashMap<IVec, Option<IVec>>>,
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum TransactionError {
    Conflict,
    Abort,
}

impl<'a> TransactionalTree<'a> {
    /// Set a key to a new value
    pub fn insert<K, V>(&self, key: K, value: V) -> TxResult<Option<IVec>>
    where
        IVec: From<K>,
        IVec: From<V>,
        K: AsRef<[u8]>,
    {
        let mut cache = self.cache.borrow_mut();
        let old = self.get(key.as_ref())?;
        cache.insert(IVec::from(key), Some(IVec::from(value)));
        Ok(old)
    }

    /// Remove a key
    pub fn remove<K>(&self, key: K) -> TxResult<Option<IVec>>
    where
        IVec: From<K>,
        K: AsRef<[u8]>,
    {
        let mut cache = self.cache.borrow_mut();
        let old = self.get(key.as_ref());
        cache.insert(IVec::from(key), None);
        old
    }

    /// Get the value associated with a key
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> TxResult<Option<IVec>> {
        unimplemented!()
    }

    fn validate(&self) -> bool {
        unimplemented!()
    }

    fn commit(&self) -> Result<()> {
        unimplemented!()
    }
}

type TxResult<T> = std::result::Result<T, TransactionError>;

fn abort() -> TransactionError {
    TransactionError::Abort
}

pub struct TransactionalTrees<'a> {
    inner: Vec<TransactionalTree<'a>>,
}

impl<'a> TransactionalTrees<'a> {
    fn validate(&self) -> bool {
        for tree in &self.inner {
            if !tree.validate() {
                return false;
            }
        }
        true
    }
    fn commit(&self) -> Result<()> {
        for tree in &self.inner {
            tree.commit()?;
        }
        Ok(())
    }
}

pub trait Transactional {
    type View;

    fn make_overlay(&self) -> TransactionalTrees<'_>;

    fn view_overlay(overlay: &TransactionalTrees<'_>) -> Self::View;

    fn transaction<F, R>(&self, f: F) -> Result<TxResult<R>>
    where
        F: Fn(Self::View) -> TxResult<R>,
    {
        loop {
            let tt = self.make_overlay();
            let view = Self::view_overlay(&tt);
            let ret = f(view);
            match ret {
                Ok(ref r) if tt.validate() => {
                    tt.commit()?;
                    return Ok(ret);
                }
                Err(TransactionError::Abort) => {
                    return Ok(Err(TransactionError::Abort))
                }
                other => continue,
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
                cache: Default::default(),
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
                    cache: Default::default(),
                },
                TransactionalTree {
                    tree: self.1.as_ref(),
                    cache: Default::default(),
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
