use std::sync::{atomic::AtomicUsize, Arc};

use super::*;

/// Open or create a new disk-backed Tree with its own keyspace,
/// accessible from the `Db` via the provided identifier.
pub(crate) fn open_tree<'a>(
    context: Arc<Context>,
    name: Vec<u8>,
    tx: &'a Tx,
) -> Result<Tree, ()> {
    // we loop because creating this Tree may race with
    // concurrent attempts to open the same one.
    loop {
        match context.pagecache.meta_pid_for_name(&name, tx) {
            Ok(root_id) => {
                return Ok(Tree {
                    tree_id: name,
                    context: context.clone(),
                    subscriptions: Arc::new(Subscriptions::default()),
                    root: Arc::new(AtomicUsize::new(root_id)),
                });
            }
            Err(Error::CollectionNotFound(_)) => {}
            Err(other) => return Err(other),
        }

        // set up empty leaf
        let leaf_id = context.pagecache.allocate(&tx)?;
        trace!(
            "allocated pid {} for leaf in new_tree for namespace {:?}",
            leaf_id,
            name
        );

        let leaf = Frag::Base(Node {
            id: leaf_id,
            data: Data::Leaf(vec![]),
            next: None,
            lo: vec![].into(),
            hi: vec![].into(),
        });

        let leaf_ptr = context
            .pagecache
            .replace(leaf_id, TreePtr::allocated(0), leaf, &tx)
            .map_err(|e| e.danger_cast())?;

        // set up root index
        let root_id = context.pagecache.allocate(&tx)?;

        debug!(
            "allocated pid {} for root of new_tree {:?}",
            root_id, name
        );

        // vec![0] represents a prefix-encoded empty prefix
        let root_index_vec = vec![(vec![0].into(), leaf_id)];

        let root = Frag::Base(Node {
            id: root_id,
            data: Data::Index(root_index_vec),
            next: None,
            lo: vec![].into(),
            hi: vec![].into(),
        });

        let root_ptr = context
            .pagecache
            .replace(root_id, TreePtr::allocated(0), root, &tx)
            .map_err(|e| e.danger_cast())?;

        let res = context.pagecache.cas_root_in_meta(
            name.clone(),
            None,
            Some(root_id),
            tx,
        );

        if res.is_err() {
            // clean up the tree we just created if we couldn't
            // install it.
            let mut root_ptr = root_ptr;
            loop {
                match context.pagecache.free(root_id, root_ptr, tx) {
                    Ok(_) => break,
                    Err(Error::CasFailed(Some(actual_ptr))) => {
                        root_ptr = actual_ptr.clone()
                    }
                    Err(Error::CasFailed(None)) => panic!(
                        "somehow allocated child was already freed"
                    ),
                    Err(other) => return Err(other.danger_cast()),
                }
            }

            let mut leaf_ptr = leaf_ptr;
            loop {
                match context.pagecache.free(leaf_id, leaf_ptr, tx) {
                    Ok(_) => break,
                    Err(Error::CasFailed(Some(actual_ptr))) => {
                        leaf_ptr = actual_ptr.clone()
                    }
                    Err(Error::CasFailed(None)) => panic!(
                        "somehow allocated child was already freed"
                    ),
                    Err(other) => return Err(other.danger_cast()),
                }
            }
        }

        match res {
            Err(Error::CasFailed(..)) => continue,
            Err(other) => return Err(other.danger_cast()),
            Ok(_) => {}
        }

        return Ok(Tree {
            tree_id: name,
            subscriptions: Arc::new(Subscriptions::default()),
            context: context.clone(),
            root: Arc::new(AtomicUsize::new(root_id)),
        });
    }
}
