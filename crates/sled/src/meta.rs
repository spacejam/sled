use std::sync::{atomic::AtomicUsize, Arc};

use super::*;

/// Open or create a new disk-backed Tree with its own keyspace,
/// accessible from the `Db` via the provided identifier.
pub(crate) fn open_tree<'a>(
    context: Arc<Context>,
    name: Vec<u8>,
    tx: &'a Tx,
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
            .replace(leaf_id, TreePtr::allocated(0), leaf, &tx)?
            .expect("somehow could not install new leaf");

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
            .replace(root_id, TreePtr::allocated(0), root, &tx)?
            .expect("somehow could not install new root");

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
            root: Arc::new(AtomicUsize::new(root_id)),
        });
    }
}
