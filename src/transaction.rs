//! Fully serializable (ACID) multi-`Tree` transactions
//!
//! # Examples
//! ```
//! # use sled::{transaction::TransactionResult, Config};
//! # fn main() -> TransactionResult<()> {
//! # let config = Config::new().temporary(true);
//! # let db = config.open().unwrap();
//!
//! // Use write-only transactions as a writebatch:
//! sled::transaction(|| {
//!     db.insert(b"k1", b"cats")?;
//!     db.insert(b"k2", b"dogs")?;
//!     Ok(())
//! })?;
//!
//! // Atomically swap two items:
//! sled::transaction(|| {
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
//! In this example, we treat two trees like a
//! work queue, atomically apply updates to
//! data and move them from the unprocessed `Tree`
//! to the processed `Tree`.
//!
//! ```
//! # use sled::{transaction::{TransactionResult, Transactional}, Config};
//! # fn main() -> TransactionResult<()> {
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
//! sled::transaction(|| {
//!     let unprocessed_item = unprocessed.remove(b"k3")?.unwrap();
//!     let mut processed_item = b"yappin' ".to_vec();
//!     processed_item.extend_from_slice(&unprocessed_item);
//!     processed.insert(b"k3", processed_item)?;
//!     Ok(())
//! })?;
//!
//! assert_eq!(unprocessed.get(b"k3").unwrap(), None);
//! assert_eq!(&processed.get(b"k3").unwrap().unwrap(), b"yappin' ligers");
//! # Ok(())
//! # }
//! ```
#![allow(unused)]
use std::{
    cell::RefCell, panic::UnwindSafe, rc::Rc, sync::atomic::AtomicUsize,
};

use crate::{
    concurrency_control, pin, Batch, Error, Guard, IVec, PageId, Protector,
    Result, Tree, Map, Set
};

/// Fully serializable (ACID) multi-`Tree` transactions.
/// This will throw an error if Tree's from different
/// databases are used in the same transaction.
///
/// # Examples
/// ```
/// # fn main() -> sled::Result<()> {
///
/// # let db = sled::Config::new().temporary(true).open()?;
/// # let db_2 = sled::Config::new().temporary(true).open()?;
///
/// // Use write-only transactions as a writebatch:
/// sled::transaction(|| {
///     db.insert(b"k1", b"cats")?;
///     db.insert(b"k2", b"dogs")?;
///     Ok(())
/// })?;
///
/// // Atomically swap two items:
/// sled::transaction(|| {
///     let v1_option = db.remove(b"k1")?;
///     let v1 = v1_option.unwrap();
///     let v2_option = db.remove(b"k2")?;
///     let v2 = v2_option.unwrap();
///
///     db.insert(b"k1", v2)?;
///     db.insert(b"k2", v1)?;
///
///     Ok(())
/// })?;
///
/// assert_eq!(&db.get(b"k1")?.unwrap(), b"dogs");
/// assert_eq!(&db.get(b"k2")?.unwrap(), b"cats");
/// # Ok(())
/// # }
/// ```
pub fn transaction<T, R>(tx: T) -> Result<R>
where
    T: UnwindSafe + Send + Fn() -> Result<R>,
{
    let _cc =
        if using_tx() { None } else { Some(concurrency_control::write()) };

    let mut guard = pin();

    begin();

    let res = std::panic::catch_unwind(tx);

    if let Ok(Ok(_)) = res {
        commit(&mut guard)?;
    } else {
        abort();
    }

    match res {
        Ok(r) => r,
        Err(e) => {
            // we propagate the panic here after cleaning
            // up the transaction state, to avoid issues
            // for future transactions
            panic!(e)
        }
    }
}

static LOCKS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Default)]
pub(crate) struct WriteSet {
    batches: Map<Tree, Batch>,
}

fn begin() {
    TX.with(|tx| tx.borrow_mut().push(WriteSet::default()))
}

fn commit(guard: &mut Guard) -> Result<()> {
    TX.with(|txs| {
        let mut writesets = txs.borrow_mut();
        let mut child_tx: WriteSet = writesets.pop().unwrap();

        // either merge into the next level down or the backing tree
        if let Some(parent_tx) = writesets.last_mut() {
            for (tree, batch) in child_tx.batches {
                let mut entry: &mut Batch =
                    parent_tx.batches.entry(tree).or_insert(Batch::default());
                entry.merge(batch);
            }
        } else {
            if child_tx.batches.is_empty() {
                return Ok(());
            }

            let first_tree = &child_tx.batches.iter().next().unwrap().0.clone();

            // we want to use a single peg for all of the batches
            let peg = first_tree.context.pin_log(guard)?;

            for (tree, batch) in std::mem::take(&mut child_tx.batches) {
                log::trace!("applying batch {:?}", batch);
                for (k, v_opt) in batch.writes {
                    loop {
                        if tree.insert_inner(&k, v_opt.clone(), guard)?.is_ok()
                        {
                            break;
                        }
                    }
                }
            }

            // when the peg drops, it ensures all updates
            // written to the log since its creation are
            // recovered atomically
            peg.seal_batch();
        }
        Ok(())
    })
}

impl Tree {
    pub(crate) fn tx_get(
        &self,
        key: IVec,
        value: Option<IVec>,
    ) -> Option<IVec> {
    }

    pub(crate) fn tx_set(
        &self,
        key: IVec,
        value: Option<IVec>,
    ) -> Option<IVec> {
    }
}

fn abort() {
    TX.with(|tx| tx.borrow_mut().pop());
}

thread_local! {
    static TX: RefCell<Vec<WriteSet>> = RefCell::new(vec![]);
}

pub(crate) fn using_tx() -> bool {
    TX.with(|tx| !tx.borrow().is_empty())
}
