use std::sync::{atomic::AtomicU64, Arc};

use super::*;

/// Open or create a new disk-backed Tree with its own keyspace,
/// accessible from the `Db` via the provided identifier.
pub(crate) fn open_tree<'a>(
    context: Context,
    name: Vec<u8>,
    tx: &'a Tx<Frag>,
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

/// Options and flags which can be used to configure how a tree is opened.
///
/// This builder exposes the ability to configure how a [`Tree`] is opened.
/// The `Db::open_tree` and `Db::create_tree` methods are aliases for commonly
/// used options using this builder.
pub struct TreeOpenOptions<'a> {
    context: Context,
    tx: &'a Tx<Frag>,
    create: bool,
    create_new: bool,
}

impl<'a> TreeOpenOptions<'a> {
    pub(crate) fn new(context: Context, tx: &'a Tx<Frag>) -> TreeOpenOptions<'a> {
        TreeOpenOptions { context, tx, create: false, create_new: false }
    }

    /// Sets the option for creating a new tree.
    ///
    /// This option indicates whether a new tree will be created
    /// if the tree does not yet already exist.
    pub fn create(&mut self, create: bool) -> &mut TreeOpenOptions<'a> {
        self.create = create; self
    }

    /// Sets the option to always create a new tree.
    ///
    /// This option indicates whether a new tree will be created. No tree is allowed to exist.
    pub fn create_new(&mut self, create_new: bool) -> &mut TreeOpenOptions<'a> {
        self.create_new = create_new; self
    }

    /// Opens a tree with the given `name` and the options specified by `self`.
    ///
    /// # Errors
    ///
    /// This function will return an error under a number of different
    /// circumstances.
    ///
    /// * [`CollectionNotFound`]: The specified tree does not exist and neither `create`
    ///   or `create_new` is set.
    /// * [`AlreadyExists`]: `create_new` was specified and the tree already exists.
    pub fn open(&self, name: Vec<u8>) -> Result<Tree> {
        // we loop because creating this Tree may race with
        // concurrent attempts to open the same one.
        loop {
            match self.context.pagecache.meta_pid_for_name(&name, self.tx) {
                Ok(root_id) => {
                    if self.create_new {
                        unimplemented!("tree already exist but `create_new` is `true`");
                    }
                    return Ok(Tree {
                        tree_id: name,
                        context: self.context.clone(),
                        subscriptions: Arc::new(Subscriptions::default()),
                        root: Arc::new(AtomicU64::new(root_id)),
                    });
                }
                Err(Error::CollectionNotFound(_)) => {
                    if !self.create_new && !self.create {
                        unimplemented!("tree does not exist but `create` and `create_new` are `false`")
                    }
                },
                Err(other) => return Err(other),
            }

            // set up empty leaf
            let leaf = Frag::Base(Node {
                data: Data::Leaf(vec![]),
                next: None,
                lo: vec![].into(),
                hi: vec![].into(),
            });

            let (leaf_id, leaf_ptr) = self.context.pagecache.allocate(leaf, &self.tx)?;

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
            });

            let (root_id, root_ptr) = self.context.pagecache.allocate(root, &self.tx)?;

            debug!("allocated pid {} for root of new_tree {:?}", root_id, name);

            let res = self.context.pagecache.cas_root_in_meta(
                name.clone(),
                None,
                Some(root_id),
                self.tx,
            )?;

            if res.is_err() {
                // clean up the tree we just created if we couldn't
                // install it.
                self.context
                    .pagecache
                    .free(root_id, root_ptr, self.tx)?
                    .expect("could not free allocated page");
                self.context
                    .pagecache
                    .free(leaf_id, leaf_ptr, self.tx)?
                    .expect("could not free allocated page");
                continue;
            }

            return Ok(Tree {
                tree_id: name,
                subscriptions: Arc::new(Subscriptions::default()),
                context: self.context.clone(),
                root: Arc::new(AtomicU64::new(root_id)),
            });
        }
    }
}
