use std::sync::{
    atomic::Ordering::{Acquire, Relaxed, Release},
    Arc,
};

use super::*;

pub(crate) trait PageCacheExt {
    fn open_tree(
        &self,
        name: Vec<u8>,
        context: Arc<Context>,
    ) -> Result<Arc<Tree>, ()>;

    fn drop_tree(
        &self,
        name: &[u8],
        context: &Context,
    ) -> Result<bool, ()>;

    fn generate_id(&self, context: &Context) -> Result<usize, ()>;
}

impl PageCacheExt for PageCache<BLinkMaterializer, Frag, Recovery> {
    fn generate_id(&self, context: &Context) -> Result<usize, ()> {
        let ret = context.idgen.fetch_add(1, Relaxed);

        let interval = context.idgen_persist_interval;
        let necessary_persists = ret / interval * interval;
        let mut persisted = context.idgen_persists.load(Acquire);

        while persisted < necessary_persists {
            let _mu = context.idgen_persist_mu.lock().unwrap();
            persisted = context.idgen_persists.load(Acquire);
            if persisted < necessary_persists {
                // it's our responsibility to persist up to our ID
                let guard = pin();
                let (current, key) = context
                    .pagecache
                    .get(COUNTER_PID, &guard)
                    .map_err(|e| e.danger_cast())?
                    .unwrap();

                if let Frag::Counter(current) = current {
                    assert_eq!(*current, persisted);
                } else {
                    panic!(
                        "counter pid contained non-Counter: {:?}",
                        current
                    );
                }

                let counter_frag = Frag::Counter(necessary_persists);

                let old = context
                    .idgen_persists
                    .swap(necessary_persists, Release);
                assert_eq!(old, persisted);

                context
                    .pagecache
                    .replace(
                        COUNTER_PID,
                        key.clone(),
                        counter_frag,
                        &guard,
                    )
                    .map_err(|e| e.danger_cast())?;

                // during recovery we add 2x the interval. we only
                // need to block if the last one wasn't stable yet.
                if key.last_lsn() > context.pagecache.stable_lsn() {
                    context.pagecache.make_stable(key.last_lsn())?;
                }

                guard.flush();
            }
        }

        Ok(ret)
    }

    fn open_tree(
        &self,
        name: Vec<u8>,
        context: Arc<Context>,
    ) -> Result<Arc<Tree>, ()> {
        let guard = pin();

        let tenants = context.tenants.read().unwrap();
        if let Some(tree) = tenants.get(&name) {
            return Ok(tree.clone());
        }
        // drop reader lock
        drop(tenants);

        // set up empty leaf
        let leaf_id = context.pagecache.allocate(&guard)?;
        trace!(
            "allocated pid {} for leaf in open_tree for namespace {:?}",
            leaf_id,
            name,
        );

        let leaf = Frag::Base(Node {
            id: leaf_id,
            data: Data::Leaf(vec![]),
            next: None,
            lo: vec![].into(),
            hi: vec![].into(),
        });

        context
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

        context
            .pagecache
            .replace(root_id, TreePtr::allocated(), root, &guard)
            .map_err(|e| e.danger_cast())?;

        meta::cas_root(
            &context.pagecache,
            name.clone(),
            None,
            Some(root_id),
            &guard,
        )
        .map_err(|e| e.danger_cast())?;

        let context2 = context.clone();
        let mut tenants = context.tenants.write().unwrap();
        let tree = Arc::new(Tree {
            tree_id: name.clone(),
            subscriptions: Arc::new(Subscriptions::default()),
            context: context2,
        });
        tenants.insert(name, tree.clone());
        drop(tenants);

        Ok(tree)
    }

    fn drop_tree(
        &self,
        name: &[u8],
        context: &Context,
    ) -> Result<bool, ()> {
        if name == DEFAULT_TREE_ID || name == TX_TREE_ID {
            return Err(Error::Unsupported(
                "cannot remove the core structures".into(),
            ));
        }
        trace!("dropping tree {:?}", name,);

        let mut tenants = context.tenants.write().unwrap();

        let tree = if let Some(tree) = tenants.remove(&*name) {
            tree
        } else {
            return Ok(false);
        };

        let guard = pin();

        let mut root_id =
            meta::pid_for_name(&context.pagecache, &name, &guard)?;

        let leftmost_chain: Vec<PageId> = tree
            .path_for_key(b"", &guard)?
            .into_iter()
            .map(|(frag, _tp)| frag.unwrap_base().id)
            .collect();

        loop {
            let res = meta::cas_root(
                &context.pagecache,
                name.to_vec(),
                Some(root_id),
                None,
                &guard,
            )
            .map_err(|e| e.danger_cast());

            match res {
                Ok(_) => break,
                Err(Error::CasFailed(actual)) => root_id = actual,
                Err(other) => return Err(other.danger_cast()),
            }
        }

        // drop writer lock
        drop(tenants);

        guard.defer(move || tree.gc_pages(leftmost_chain));

        guard.flush();

        Ok(true)
    }
}
