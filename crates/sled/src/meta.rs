use std::sync::{atomic::AtomicU64, Arc};

use parking_lot::RwLock;

use pagecache::Guard;

use super::*;

/// Open or create a new disk-backed Tree with its own keyspace,
/// accessible from the `Db` via the provided identifier.
pub(crate) fn open_tree(
    context: &Context,
    name: Vec<u8>,
    guard: &Guard,
) -> Result<Tree> {
    // we loop because creating this Tree may race with
    // concurrent attempts to open the same one.
    loop {
        match context.pagecache.meta_pid_for_name(&name, guard) {
            Ok(root_id) => {
                return Ok(Tree(Arc::new(TreeInner {
                    tree_id: name,
                    context: context.clone(),
                    subscriptions: Subscriptions::default(),
                    root: AtomicU64::new(root_id),
                    concurrency_control: RwLock::new(()),
                    merge_operator: RwLock::new(None),
                })));
            }
            Err(Error::CollectionNotFound(_)) => {}
            Err(other) => return Err(other),
        }

        // set up empty leaf
        let leaf = Frag::base(Data::Leaf(vec![]));
        let (leaf_id, leaf_ptr) = context.pagecache.allocate(leaf, guard)?;

        trace!(
            "allocated pid {} for leaf in new_tree for namespace {:?}",
            leaf_id,
            name
        );

        // set up root index

        // vec![0] represents a prefix-encoded empty prefix
        let root_index_vec = vec![(vec![0].into(), leaf_id)];
        let root = Frag::base(Data::Index(root_index_vec));
        let (root_id, root_ptr) = context.pagecache.allocate(root, guard)?;

        debug!("allocated pid {} for root of new_tree {:?}", root_id, name);

        let res = context.pagecache.cas_root_in_meta(
            &name,
            None,
            Some(root_id),
            guard,
        )?;

        if res.is_err() {
            // clean up the tree we just created if we couldn't
            // install it.
            context
                .pagecache
                .free(root_id, root_ptr, guard)?
                .expect("could not free allocated page");
            context
                .pagecache
                .free(leaf_id, leaf_ptr, guard)?
                .expect("could not free allocated page");
            continue;
        }

        return Ok(Tree(Arc::new(TreeInner {
            tree_id: name,
            subscriptions: Subscriptions::default(),
            context: context.clone(),
            root: AtomicU64::new(root_id),
            concurrency_control: RwLock::new(()),
            merge_operator: RwLock::new(None),
        })));
    }
}
