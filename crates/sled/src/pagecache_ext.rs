use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use super::*;

pub(crate) trait PageCacheExt {
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
}
