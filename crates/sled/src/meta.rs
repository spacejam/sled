use std::sync::{atomic::AtomicUsize, Arc};

use super::*;

pub(crate) fn meta<'a>(
    pages: &PageCache<BLinkMaterializer, Frag, Recovery>,
    guard: &'a Guard,
) -> Result<&'a Meta, ()> {
    let meta_page_get =
        pages.get(META_PID, guard).map_err(|e| e.danger_cast())?;

    let meta = match meta_page_get {
        PageGet::Materialized(ref meta_ptr, ref _ptr) => {
            meta_ptr.unwrap_meta()
        }
        broken => {
            panic!("pagecache returned non-base node: {:?}", broken)
        }
    };

    Ok(meta)
}

pub(crate) fn pid_for_name(
    pages: &PageCache<BLinkMaterializer, Frag, Recovery>,
    name: &[u8],
    guard: &Guard,
) -> Result<usize, ()> {
    let m = meta(pages, guard)?;
    if let Some(root) = m.get_root(name) {
        Ok(root)
    } else {
        Err(Error::CollectionNotFound(name.to_vec()))
    }
}

pub(crate) fn cas_root(
    pages: &PageCache<BLinkMaterializer, Frag, Recovery>,
    name: Vec<u8>,
    old: Option<usize>,
    new: Option<usize>,
    guard: &Guard,
) -> Result<(), Option<usize>> {
    let meta_page_get =
        pages.get(META_PID, guard).map_err(|e| e.danger_cast())?;

    let (meta_key, meta) = match meta_page_get {
        PageGet::Materialized(ref meta_ptr, ref key) => {
            let meta = meta_ptr.unwrap_meta();
            (key, meta)
        }
        broken => {
            panic!("pagecache returned non-base node: {:?}", broken)
        }
    };

    let mut current: TreePtr = meta_key.clone();
    loop {
        let actual = meta.get_root(&name);
        if actual != old {
            return Err(Error::CasFailed(actual));
        }

        let mut new_meta = meta.clone();
        if let Some(new) = new {
            new_meta.set_root(name.clone(), new);
        } else {
            new_meta.del_root(&name);
        }

        let new_meta_frag = Frag::Meta(new_meta);
        let res =
            pages.replace(META_PID, current, new_meta_frag, &guard);

        match res {
            Ok(_) => return Ok(()),
            Err(Error::CasFailed(Some(actual))) => current = actual,
            Err(Error::CasFailed(None)) => return Err(Error::ReportableBug(
                "replacing the META page has failed because \
                the pagecache does not think it currently exists.".into()
            )),
            Err(other) => return Err(other.danger_cast()),
        }
    }
}

/// Open or create a new disk-backed Tree with its own keyspace,
/// accessible from the `Db` via the provided identifier.
pub(crate) fn open_tree<'a>(
    context: Arc<Context>,
    name: Vec<u8>,
    guard: &'a Guard,
) -> Result<Tree, ()> {
    // we loop because creating this Tree may race with
    // concurrent attempts to open the same one.
    loop {
        match pid_for_name(&context.pagecache, &name, guard) {
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
        let leaf_id = context.pagecache.allocate(&guard)?;
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
            .replace(leaf_id, TreePtr::allocated(), leaf, &guard)
            .map_err(|e| e.danger_cast())?;

        // set up root index
        let root_id = context.pagecache.allocate(&guard)?;

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
            .replace(root_id, TreePtr::allocated(), root, &guard)
            .map_err(|e| e.danger_cast())?;

        let res = meta::cas_root(
            &context.pagecache,
            name.clone(),
            None,
            Some(root_id),
            guard,
        );

        if res.is_err() {
            // clean up the tree we just created if we couldn't
            // install it.
            let mut root_ptr = root_ptr;
            loop {
                match context.pagecache.free(root_id, root_ptr, guard)
                {
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
                match context.pagecache.free(leaf_id, leaf_ptr, guard)
                {
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
