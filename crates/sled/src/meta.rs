use std::sync::{atomic::AtomicU64, Arc};

use super::*;

/// Open or create a new disk-backed Tree with its own keyspace,
/// accessible from the `Db` via the provided identifier.
pub(crate) fn open_tree<'a>(
    context: Context,
    name: Vec<u8>,
    tx: &'a Tx<'a, BLinkMaterializer, Frag>,
) -> Result<Tree> {
    // we loop because creating this Tree may race with
    // concurrent attempts to open the same one.
    loop {
        match context.pagecache.meta_pid_for_name(&name, tx) {
            Ok(root_id) => {
                return Ok(Tree {
                    tree_id: name,
                    context: context.clone(),
                    subscriptions: Arc::new(Subscriptions::default()),
                    root: Arc::new(AtomicU64::new(root_id)),
                });
            }
            Err(Error::CollectionNotFound(_)) => {}
            Err(other) => return Err(other),
        }

        // set up empty leaf
        let leaf = Frag::Base(Node {
            data: Data::Leaf(vec![]),
            next: None,
            lo: vec![].into(),
            hi: vec![].into(),
            merging_child: None,
            merging: false,
        });

        let (leaf_id, leaf_ptr) = context.pagecache.allocate(leaf, &tx)?;

        trace!(
            "allocated pid {} for leaf in new_tree for namespace {:?}",
            leaf_id,
            name
        );

        // set up root index

        // vec![0] represents a prefix-encoded empty prefix
        let root_index_vec = vec![(vec![0].into(), leaf_id)];

        let root = Frag::Base(Node {
            data: Data::Index(root_index_vec),
            next: None,
            lo: vec![].into(),
            hi: vec![].into(),
            merging_child: None,
            merging: false,
        });

        let (root_id, root_ptr) = context.pagecache.allocate(root, &tx)?;

        debug!("allocated pid {} for root of new_tree {:?}", root_id, name);

        let res = context.pagecache.cas_root_in_meta(
            name.clone(),
            None,
            Some(root_id),
            tx,
        )?;

        if res.is_err() {
            // clean up the tree we just created if we couldn't
            // install it.
            context
                .pagecache
                .free(root_id, root_ptr, tx)?
                .expect("could not free allocated page");
            context
                .pagecache
                .free(leaf_id, leaf_ptr, tx)?
                .expect("could not free allocated page");
            continue;
        }

        return Ok(Tree {
            tree_id: name,
            subscriptions: Arc::new(Subscriptions::default()),
            context: context.clone(),
            root: Arc::new(AtomicU64::new(root_id)),
        });
    }
}
