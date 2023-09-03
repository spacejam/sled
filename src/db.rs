use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::io;
use std::mem::{self, ManuallyDrop};
use std::ops;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use cache_advisor::CacheAdvisor;
use concurrent_map::{ConcurrentMap, Minimum};
use inline_array::InlineArray;
use parking_lot::{
    lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard},
    RawRwLock, RwLock,
};
use stack_map::StackMap;

use crate::*;

const INDEX_FANOUT: usize = 64;
const EBR_LOCAL_GC_BUFFER_SIZE: usize = 128;

#[derive(Clone)]
struct Io<const LEAF_FANOUT: usize> {
    config: Config,
    global_error: Arc<AtomicPtr<(io::ErrorKind, String)>>,
    node_id_index: ConcurrentMap<
        u64,
        Node<LEAF_FANOUT>,
        INDEX_FANOUT,
        EBR_LOCAL_GC_BUFFER_SIZE,
    >,
    store: heap::Heap,
    cache_advisor: RefCell<CacheAdvisor>,
    flush_epoch: FlushEpochTracker,
    dirty: ConcurrentMap<(FlushEpoch, NodeId), Dirty<LEAF_FANOUT>>,
}

impl<const LEAF_FANOUT: usize> Io<LEAF_FANOUT> {
    fn check_error(&self) -> io::Result<()> {
        let err_ptr: *const (io::ErrorKind, String) =
            self.global_error.load(Ordering::Acquire);

        if err_ptr.is_null() {
            Ok(())
        } else {
            let deref: &(io::ErrorKind, String) = unsafe { &*err_ptr };
            Err(io::Error::new(deref.0, deref.1.clone()))
        }
    }

    fn set_error(&self, error: &io::Error) {
        let kind = error.kind();
        let reason = error.to_string();

        let boxed = Box::new((kind, reason));
        let ptr = Box::into_raw(boxed);

        if self
            .global_error
            .compare_exchange(
                std::ptr::null_mut(),
                ptr,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            // global fatal error already installed, drop this one
            unsafe {
                drop(Box::from_raw(ptr));
            }
        }
    }

    fn flush(&self) -> io::Result<()> {
        let mut write_batch = vec![];

        log::trace!("advancing epoch");
        let (
            previous_flush_complete_notifier,
            this_vacant_notifier,
            forward_flush_notifier,
        ) = self.flush_epoch.roll_epoch_forward();

        log::trace!("waiting for previous flush to complete");
        previous_flush_complete_notifier.wait_for_complete();

        log::trace!("waiting for our epoch to become vacant");
        let flush_through_epoch: FlushEpoch =
            this_vacant_notifier.wait_for_complete();

        log::trace!("performing flush");

        let flush_boundary = (flush_through_epoch.increment(), NodeId::MIN);

        let mut evict_after_flush = vec![];

        for ((dirty_epoch, dirty_node_id), dirty_value_initial_read) in
            self.dirty.range(..flush_boundary)
        {
            let dirty_value = self
                .dirty
                .remove(&(dirty_epoch, dirty_node_id))
                .expect("violation of flush responsibility");

            if let Dirty::NotYetSerialized { .. } = &dirty_value {
                assert_eq!(dirty_value_initial_read, dirty_value);
            }

            // drop is necessary to increase chance of Arc strong count reaching 1
            // while taking ownership of the value
            drop(dirty_value_initial_read);

            assert_eq!(dirty_epoch, flush_through_epoch);

            #[cfg(feature = "for-internal-testing-only")]
            {
                let mutation_count = lock.as_ref().unwrap().mutation_count;
                self.event_verifier.mark_flush(
                    node.id,
                    dirty_epoch,
                    mutation_count,
                );
            }

            match dirty_value {
                Dirty::MergedAndDeleted { node_id } => {
                    log::trace!(
                        "MergedAndDeleted for {:?}, adding None to write_batch",
                        node_id
                    );
                    write_batch.push(heap::Update::Free {
                        node_id: dirty_node_id,
                        collection_id: CollectionId::MIN,
                    });
                }
                Dirty::CooperativelySerialized {
                    low_key,
                    mutation_count: _,
                    mut data,
                } => {
                    Arc::make_mut(&mut data);
                    let data = Arc::into_inner(data).unwrap();
                    write_batch.push(heap::Update::Store {
                        node_id: dirty_node_id,
                        collection_id: CollectionId::MIN,
                        metadata: low_key,
                        data,
                    });
                }
                Dirty::NotYetSerialized { low_key, node } => {
                    assert_eq!(dirty_node_id, node.id, "mismatched node ID for NotYetSerialized with low key {:?}", low_key);
                    let mut lock = node.inner.write();

                    let leaf_ref: &mut Leaf<LEAF_FANOUT> =
                        lock.as_mut().unwrap();

                    let data = if leaf_ref.dirty_flush_epoch
                        == Some(flush_through_epoch)
                    {
                        if let Some(deleted_at) = leaf_ref.deleted {
                            assert!(deleted_at > flush_through_epoch);
                        }
                        leaf_ref.dirty_flush_epoch.take();
                        leaf_ref.serialize(self.config.zstd_compression_level)
                    } else {
                        // Here we expect that there was a benign data race and that another thread
                        // mutated the leaf after encountering it being dirty for our epoch, after
                        // storing a CooperativelySerialized in the dirty map.
                        let dirty_value_2_opt =
                            self.dirty.remove(&(dirty_epoch, dirty_node_id));

                        let dirty_value_2 = if let Some(dv2) = dirty_value_2_opt
                        {
                            dv2
                        } else {
                            eprintln!(
                                "violation of flush responsibility for second read \
                                of expected cooperative serialization. leaf in question's \
                                dirty_flush_epoch is {:?}, our expected key was {:?}. node.deleted: {:?}",
                                leaf_ref.dirty_flush_epoch,
                                (dirty_epoch, dirty_node_id),
                                leaf_ref.deleted,
                            );

                            std::process::abort();
                        };

                        if let Dirty::CooperativelySerialized {
                            low_key: low_key_2,
                            mutation_count: _,
                            mut data,
                        } = dirty_value_2
                        {
                            assert_eq!(low_key, low_key_2);
                            Arc::make_mut(&mut data);
                            Arc::into_inner(data).unwrap()
                        } else {
                            unreachable!("a leaf was expected to be cooperatively serialized but it was not available");
                        }
                    };

                    write_batch.push(heap::Update::Store {
                        node_id: dirty_node_id,
                        collection_id: CollectionId::MIN,
                        metadata: low_key,
                        data,
                    });

                    if leaf_ref.page_out_on_flush == Some(flush_through_epoch) {
                        // page_out_on_flush is set to false
                        // on page-in due to serde(skip)
                        evict_after_flush.push(node.clone());
                    }
                }
            }
        }

        let written_count = write_batch.len();
        if written_count > 0 {
            self.store.write_batch(write_batch)?;
            log::trace!(
                "marking {:?} as flushed - {} objects written",
                flush_through_epoch,
                written_count
            );
        }
        log::trace!(
            "marking the forward flush notifier that {:?} is flushed",
            flush_through_epoch
        );
        forward_flush_notifier.mark_complete();

        for node_to_evict in evict_after_flush {
            let mut lock = node_to_evict.inner.write();
            if let Some(ref mut leaf) = *lock {
                if leaf.page_out_on_flush == Some(flush_through_epoch) {
                    *lock = None;
                }
            }
        }

        self.store.maintenance()?;
        Ok(())
    }
}

/// sled 1.0
#[derive(Clone)]
pub struct Db<const LEAF_FANOUT: usize = 1024> {
    io: Io<LEAF_FANOUT>,
    high_level_rc: Arc<()>,
    index: ConcurrentMap<
        InlineArray,
        Node<LEAF_FANOUT>,
        INDEX_FANOUT,
        EBR_LOCAL_GC_BUFFER_SIZE,
    >,
    shutdown_sender: Option<mpsc::Sender<mpsc::Sender<()>>>,
    was_recovered: bool,
    #[cfg(feature = "for-internal-testing-only")]
    event_verifier: Arc<crate::event_verifier::EventVerifier>,
}

impl<const LEAF_FANOUT: usize> fmt::Debug for Db<LEAF_FANOUT> {
    fn fmt(&self, w: &mut fmt::Formatter<'_>) -> fmt::Result {
        let alternate = w.alternate();

        let mut debug_struct = w.debug_struct(&format!("Db<{}>", LEAF_FANOUT));

        if alternate {
            debug_struct
                .field("global_error", &self.check_error())
                .field(
                    "data",
                    &format!("{:?}", self.iter().collect::<Vec<_>>()),
                )
                .finish()
        } else {
            debug_struct.field("global_error", &self.check_error()).finish()
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Dirty<const LEAF_FANOUT: usize> {
    NotYetSerialized {
        low_key: InlineArray,
        node: Node<LEAF_FANOUT>,
    },
    CooperativelySerialized {
        low_key: InlineArray,
        data: Arc<Vec<u8>>,
        mutation_count: u64,
    },
    MergedAndDeleted {
        node_id: NodeId,
    },
}

impl<const LEAF_FANOUT: usize> Dirty<LEAF_FANOUT> {
    fn is_final_state(&self) -> bool {
        match self {
            Dirty::NotYetSerialized { .. } => false,
            Dirty::CooperativelySerialized { .. } => true,
            Dirty::MergedAndDeleted { .. } => true,
        }
    }
}

fn flusher<const LEAF_FANOUT: usize>(
    io: Io<LEAF_FANOUT>,
    shutdown_signal: mpsc::Receiver<mpsc::Sender<()>>,
    flush_every_ms: usize,
) {
    let interval = Duration::from_millis(flush_every_ms as _);
    let mut last_flush_duration = Duration::default();
    loop {
        let recv_timeout = interval
            .saturating_sub(last_flush_duration)
            .max(Duration::from_millis(1));
        if let Ok(shutdown_sender) = shutdown_signal.recv_timeout(recv_timeout)
        {
            drop(io);
            if let Err(e) = shutdown_sender.send(()) {
                log::error!(
                    "Db flusher could not ack shutdown to requestor: {e:?}"
                );
            }
            log::debug!(
                "flush thread terminating after signalling to requestor"
            );
            return;
        }

        let before_flush = Instant::now();

        let flush_result = io.flush();

        match flush_result {
            Ok(_) => {}
            Err(e) => {
                log::error!(
                    "Db flusher encountered error while flushing: {:?}",
                    e
                );
                io.set_error(&e);

                std::process::abort();

                //return;
            } /*
              Err(panicked) => {
                  eprintln!("flusher thread panicked while flushing sled database: {:?}", panicked);
                  std::process::abort();
              }
              */
        };

        last_flush_duration = before_flush.elapsed();
    }
}

impl<const LEAF_FANOUT: usize> Leaf<LEAF_FANOUT> {
    fn serialize(&self, zstd_compression_level: i32) -> Vec<u8> {
        let mut ret = vec![];

        let mut zstd_enc =
            zstd::stream::Encoder::new(&mut ret, zstd_compression_level)
                .unwrap();

        bincode::serialize_into(&mut zstd_enc, self).unwrap();

        zstd_enc.finish().unwrap();

        ret
    }

    fn deserialize(buf: &[u8]) -> io::Result<Box<Leaf<LEAF_FANOUT>>> {
        let zstd_decoded = zstd::stream::decode_all(buf).unwrap();
        let mut leaf: Box<Leaf<LEAF_FANOUT>> =
            bincode::deserialize(&zstd_decoded).unwrap();

        // use decompressed buffer length as a cheap proxy for in-memory size for now
        leaf.in_memory_size = zstd_decoded.len();

        Ok(leaf)
    }

    fn set_in_memory_size(&mut self) {
        self.in_memory_size = mem::size_of::<Leaf<LEAF_FANOUT>>()
            + self.hi.as_ref().map(|h| h.len()).unwrap_or(0)
            + self.lo.len()
            + self.data.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>();
    }

    fn split_if_full(
        &mut self,
        new_epoch: FlushEpoch,
        allocator: &heap::Heap,
    ) -> Option<(InlineArray, Node<LEAF_FANOUT>)> {
        if self.data.is_full() {
            // split
            let split_offset = if self.lo.is_empty() {
                // split left-most shard almost at the beginning for
                // optimizing downward-growing workloads
                1
            } else if self.hi.is_none() {
                // split right-most shard almost at the end for
                // optimizing upward-growing workloads
                self.data.len() - 2
            } else {
                self.data.len() / 2
            };

            let data = self.data.split_off(split_offset);

            let left_max = &self.data.last().as_ref().unwrap().0;
            let right_min = &data.first().as_ref().unwrap().0;

            // suffix truncation attempts to shrink the split key
            // so that shorter keys bubble up into the index
            let splitpoint_length = right_min
                .iter()
                .zip(left_max.iter())
                .take_while(|(a, b)| a == b)
                .count()
                + 1;

            let split_key = InlineArray::from(&right_min[..splitpoint_length]);

            let rhs_id = allocator.allocate_object_id();

            log::trace!(
                "split leaf {:?} at split key: {:?} into new node {} at {:?}",
                self.lo,
                split_key,
                rhs_id,
                new_epoch,
            );

            let mut rhs = Leaf {
                dirty_flush_epoch: Some(new_epoch),
                hi: self.hi.clone(),
                lo: split_key.clone(),
                prefix_length: 0,
                in_memory_size: 0,
                data,
                mutation_count: 0,
                page_out_on_flush: None,
                deleted: None,
            };
            rhs.set_in_memory_size();

            self.hi = Some(split_key.clone());
            self.set_in_memory_size();

            let rhs_node = Node {
                id: NodeId(rhs_id),
                inner: Arc::new(Some(Box::new(rhs)).into()),
            };

            return Some((split_key, rhs_node));
        }

        None
    }
}

#[must_use]
struct LeafReadGuard<'a, const LEAF_FANOUT: usize = 1024> {
    leaf_read: ManuallyDrop<
        ArcRwLockReadGuard<RawRwLock, Option<Box<Leaf<LEAF_FANOUT>>>>,
    >,
    low_key: InlineArray,
    inner: &'a Db<LEAF_FANOUT>,
    node_id: NodeId,
    external_cache_access_and_eviction: bool,
}

impl<'a, const LEAF_FANOUT: usize> Drop for LeafReadGuard<'a, LEAF_FANOUT> {
    fn drop(&mut self) {
        let size = self.leaf_read.as_ref().unwrap().in_memory_size;
        // we must drop our mutex before calling mark_access_and_evict
        unsafe {
            ManuallyDrop::drop(&mut self.leaf_read);
        }
        if self.external_cache_access_and_eviction {
            return;
        }
        if let Err(e) = self.inner.mark_access_and_evict(self.node_id, size) {
            self.inner.set_error(&e);
            log::error!(
                "io error while paging out dirty data: {:?} \
                for guard of leaf with low key {:?}",
                e,
                self.low_key
            );
        }
    }
}

struct LeafWriteGuard<'a, const LEAF_FANOUT: usize = 1024> {
    leaf_write: ManuallyDrop<
        ArcRwLockWriteGuard<RawRwLock, Option<Box<Leaf<LEAF_FANOUT>>>>,
    >,
    flush_epoch_guard: FlushEpochGuard<'a>,
    low_key: InlineArray,
    inner: &'a Db<LEAF_FANOUT>,
    node: Node<LEAF_FANOUT>,
    external_cache_access_and_eviction: bool,
}

impl<'a, const LEAF_FANOUT: usize> LeafWriteGuard<'a, LEAF_FANOUT> {
    fn epoch(&self) -> FlushEpoch {
        self.flush_epoch_guard.epoch()
    }

    fn handle_cache_access_and_eviction_externally(
        mut self,
    ) -> (NodeId, usize) {
        self.external_cache_access_and_eviction = true;
        (self.node.id, self.leaf_write.as_ref().unwrap().in_memory_size)
    }
}

impl<'a, const LEAF_FANOUT: usize> Drop for LeafWriteGuard<'a, LEAF_FANOUT> {
    fn drop(&mut self) {
        let size = self.leaf_write.as_ref().unwrap().in_memory_size;

        // we must drop our mutex before calling mark_access_and_evict
        unsafe {
            ManuallyDrop::drop(&mut self.leaf_write);
        }
        if self.external_cache_access_and_eviction {
            return;
        }
        if let Err(e) = self.inner.mark_access_and_evict(self.node.id, size) {
            self.inner.set_error(&e);
            log::error!("io error while paging out dirty data: {:?}", e);
        }
    }
}

impl<const LEAF_FANOUT: usize> Drop for Db<LEAF_FANOUT> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.high_level_rc) == 1 {
            if let Some(shutdown_sender) = self.shutdown_sender.take() {
                let (tx, rx) = mpsc::channel();
                if shutdown_sender.send(tx).is_ok() {
                    if let Err(e) = rx.recv() {
                        log::error!(
                            "failed to shut down flusher thread: {:?}",
                            e
                        );
                    } else {
                        log::trace!("flush thread successfully terminated");
                    }
                }
            }
        }

        if Arc::strong_count(&self.high_level_rc) == 1 {
            if let Err(e) = self.io.flush() {
                eprintln!("failed to flush Db on Drop: {e:?}");
            }

            // this is probably unnecessary but it will avoid issues
            // if egregious bugs get introduced that trigger it
            self.set_error(&io::Error::new(
                io::ErrorKind::Other,
                "system has been shut down".to_string(),
            ));
        }
    }
}

fn map_bound<T, U, F: FnOnce(T) -> U>(bound: Bound<T>, f: F) -> Bound<U> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(x) => Bound::Included(f(x)),
        Bound::Excluded(x) => Bound::Excluded(f(x)),
    }
}

impl<const LEAF_FANOUT: usize> Db<LEAF_FANOUT> {
    /// Synchronously flushes all dirty IO buffers and calls
    /// fsync. If this succeeds, it is guaranteed that all
    /// previous writes will be recovered if the system
    /// crashes. Returns the number of bytes flushed during
    /// this call.
    ///
    /// Flushing can take quite a lot of time, and you should
    /// measure the performance impact of using it on
    /// realistic sustained workloads running on realistic
    /// hardware.
    ///
    /// This is called automatically on drop of the last open Db
    /// instance.
    pub fn flush(&self) -> io::Result<()> {
        self.io.flush()
    }

    // This is only pub for an extra assertion during testing.
    #[doc(hidden)]
    pub fn check_error(&self) -> io::Result<()> {
        self.io.check_error()
    }

    fn set_error(&self, error: &io::Error) {
        self.io.set_error(error)
    }

    pub fn storage_stats(&self) -> heap::Stats {
        self.io.store.stats()
    }

    pub fn size_on_disk(&self) -> io::Result<u64> {
        use std::fs::read_dir;

        fn recurse(mut dir: std::fs::ReadDir) -> io::Result<u64> {
            dir.try_fold(0, |acc, file| {
                let file = file?;
                let size = match file.metadata()? {
                    data if data.is_dir() => recurse(read_dir(file.path())?)?,
                    data => data.len(),
                };
                Ok(acc + size)
            })
        }

        recurse(read_dir(&self.io.config.path)?)
    }

    fn leaf_for_key<'a>(
        &'a self,
        key: &[u8],
    ) -> io::Result<LeafReadGuard<'a, LEAF_FANOUT>> {
        loop {
            let (low_key, node) = self.index.get_lte(key).unwrap();
            let mut read = node.inner.read_arc();

            if read.is_none() {
                drop(read);
                let (_low_key, write, _node) = self.page_in(key)?;
                read = ArcRwLockWriteGuard::downgrade(write);
            }

            if node.id.0 == 0 {
                assert!(low_key.is_empty());
            }

            let leaf_guard = LeafReadGuard {
                leaf_read: ManuallyDrop::new(read),
                inner: self,
                low_key,
                node_id: node.id,
                external_cache_access_and_eviction: false,
            };

            let leaf = leaf_guard.leaf_read.as_ref().unwrap();

            if leaf.deleted.is_some() {
                log::trace!("retry due to deleted node in leaf_for_key");
                drop(leaf_guard);
                continue;
            }
            if &*leaf.lo > key {
                log::trace!("key undershoot in leaf_for_key");
                drop(leaf_guard);

                continue;
            }
            if let Some(ref hi) = leaf.hi {
                if &**hi <= key {
                    log::trace!("key overshoot on leaf_for_key");
                    // cache maintenance occurs in Drop for LeafReadGuard
                    drop(leaf_guard);
                    continue;
                }
            }

            return Ok(leaf_guard);
        }
    }

    /// Returns `true` if the database was
    /// recovered from a previous process.
    /// Note that database state is only
    /// guaranteed to be present up to the
    /// last call to `flush`! Otherwise state
    /// is synced to disk periodically if the
    /// `Config.sync_every_ms` configuration option
    /// is set to `Some(number_of_ms_between_syncs)`
    /// or if the IO buffer gets filled to
    /// capacity before being rotated.
    pub fn was_recovered(&self) -> bool {
        self.was_recovered
    }

    pub fn open_with_config(config: &Config) -> io::Result<Db<LEAF_FANOUT>> {
        let (store, index_data) = heap::recover(&config.path)?;

        let first_id_opt = if index_data.is_empty() {
            Some(store.allocate_object_id())
        } else {
            None
        };

        let index: ConcurrentMap<
            InlineArray,
            Node<LEAF_FANOUT>,
            INDEX_FANOUT,
            EBR_LOCAL_GC_BUFFER_SIZE,
        > = initialize(&index_data, first_id_opt);

        let node_id_index: ConcurrentMap<
            u64,
            Node<LEAF_FANOUT>,
            INDEX_FANOUT,
            EBR_LOCAL_GC_BUFFER_SIZE,
        > = index.iter().map(|(_low_key, node)| (node.id.0, node)).collect();

        if config.cache_capacity_bytes < 256 {
            log::debug!(
                "Db configured to have Config.cache_capacity_bytes \
                of under 256, so we will use the minimum of 256 bytes instead"
            );
        }
        if config.entry_cache_percent > 80 {
            log::debug!(
                "Db configured to have Config.entry_cache_percent\
                of over 80%, so we will clamp it to the maximum of 80% instead"
            );
        }

        let io = Io {
            config: config.clone(),
            node_id_index,
            cache_advisor: RefCell::new(CacheAdvisor::new(
                config.cache_capacity_bytes.max(256),
                config.entry_cache_percent.min(80),
            )),
            global_error: store.get_global_error_arc(),
            store,
            dirty: Default::default(),
            flush_epoch: Default::default(),
            #[cfg(feature = "for-internal-testing-only")]
            event_verifier: Arc::default(),
        };

        let mut ret = Db {
            io: io.clone(),
            high_level_rc: Arc::new(()),
            index,
            shutdown_sender: None,
            was_recovered: first_id_opt.is_none(),
        };

        if let Some(flush_every_ms) = ret.io.config.flush_every_ms {
            let db = ret.clone();
            let (tx, rx) = mpsc::channel();
            ret.shutdown_sender = Some(tx);
            let spawn_res = std::thread::Builder::new()
                .name("sled_flusher".into())
                .spawn(move || flusher(io, rx, flush_every_ms));

            if let Err(e) = spawn_res {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("unable to spawn flusher thread for sled database: {:?}", e)
                ));
            }
        }
        Ok(ret)
    }

    fn page_in(
        &self,
        key: &[u8],
    ) -> io::Result<(
        InlineArray,
        ArcRwLockWriteGuard<RawRwLock, Option<Box<Leaf<LEAF_FANOUT>>>>,
        Node<LEAF_FANOUT>,
    )> {
        loop {
            let (low_key, node) = self.index.get_lte(key).unwrap();
            let mut write = node.inner.write_arc();
            if write.is_none() {
                let leaf_bytes = self.io.store.read(node.id.0)?;
                let leaf: Box<Leaf<LEAF_FANOUT>> =
                    Leaf::deserialize(&leaf_bytes).unwrap();
                *write = Some(leaf);
            }
            let leaf = write.as_mut().unwrap();

            if leaf.deleted.is_some() {
                log::trace!("retry due to deleted node in page_in");
                drop(write);
                continue;
            }

            if &*leaf.lo > key {
                let size = leaf.in_memory_size;
                drop(write);
                log::trace!("key undershoot in page_in");
                self.mark_access_and_evict(node.id, size)?;

                continue;
            }

            if let Some(ref hi) = leaf.hi {
                if &**hi <= key {
                    let size = leaf.in_memory_size;
                    drop(write);
                    log::trace!("key overshoot in page_in");
                    self.mark_access_and_evict(node.id, size)?;

                    continue;
                }
            }
            return Ok((low_key, write, node));
        }
    }

    fn install_dirty(
        &self,
        flush_epoch: FlushEpoch,
        node_id: NodeId,
        dirty: Dirty<LEAF_FANOUT>,
    ) {
        // dirty can transition from:
        // None -> NotYetSerialized
        // None -> MergedAndDeleted
        // None -> CooperativelySerialized
        //
        // NotYetSerialized -> MergedAndDeleted
        // NotYetSerialized -> CooperativelySerialized
        //
        // if the new Dirty is final, we must assert that
        // we are transitioning from None or NotYetSerialized.
        //
        // if the new Dirty is not final, we must assert
        // that the old value is also not final.
        let update = |old_dirty: Option<&Dirty<LEAF_FANOUT>>| -> Option<Dirty<LEAF_FANOUT>> {
            if let Some(old) = old_dirty {
                assert!(!old.is_final_state(),
                    "tried to install another Dirty marker for a node that is already
                    finalized for this flush epoch. {:?} old: {:?} new: {:?}",
                    flush_epoch, old_dirty, dirty
                );
            }

            Some(dirty.clone())
        };

        self.io.dirty.fetch_and_update((flush_epoch, node_id), update);
    }

    // NB: must never be called without having already added the empty leaf
    // operations to a normal flush epoch. This function acquires the lock
    // for the left sibling so that the empty leaf's hi key can be given
    // to the left sibling, but for this to happen atomically, the act of
    // moving left must "happen" in the same flush epoch. By "pushing" the
    // merge left potentially into a future flush epoch, any deletions that the
    // leaf had applied that may have been a part of a previous batch would also
    // be pushed into the future flush epoch, which would break the crash
    // atomicity of the batch if the updates were not flushed in the same epoch
    // as the rest of the batch. So, this is why we potentially separate the
    // flush of the left merge from the flush of the operations that caused
    // the leaf to empty in the first place.
    fn merge_leaf_into_left_sibling<'a>(
        &'a self,
        mut leaf_guard: LeafWriteGuard<'a, LEAF_FANOUT>,
    ) -> io::Result<()> {
        let mut predecessor_guard = self.predecessor_leaf_mut(&leaf_guard)?;

        assert!(predecessor_guard.epoch() >= leaf_guard.epoch());

        let merge_epoch = leaf_guard.epoch().max(predecessor_guard.epoch());

        let leaf = leaf_guard.leaf_write.as_mut().unwrap();
        let predecessor = predecessor_guard.leaf_write.as_mut().unwrap();

        assert!(leaf.data.is_empty());
        assert!(predecessor.deleted.is_none());
        assert_eq!(predecessor.hi.as_ref(), Some(&leaf.lo));

        if leaf_guard.node.id.0 == 0 {
            assert!(leaf_guard.low_key.is_empty());
        }

        log::trace!(
            "deleting empty node id {} with low key {:?} and high key {:?}",
            leaf_guard.node.id.0,
            leaf.lo,
            leaf.hi
        );

        predecessor.hi = leaf.hi.clone();
        predecessor.dirty_flush_epoch = Some(merge_epoch);

        leaf.deleted = Some(merge_epoch);

        self.index.remove(&leaf_guard.low_key).unwrap();
        self.io.node_id_index.remove(&leaf_guard.node.id.0).unwrap();

        // NB: these updates must "happen" atomically in the same flush epoch
        self.install_dirty(
            merge_epoch,
            leaf_guard.node.id,
            Dirty::MergedAndDeleted { node_id: leaf_guard.node.id },
        );

        self.install_dirty(
            merge_epoch,
            predecessor_guard.node.id,
            Dirty::NotYetSerialized {
                low_key: predecessor.lo.clone(),
                node: predecessor_guard.node.clone(),
            },
        );

        let (p_node_id, p_sz) =
            predecessor_guard.handle_cache_access_and_eviction_externally();
        let (s_node_id, s_sz) =
            leaf_guard.handle_cache_access_and_eviction_externally();

        self.mark_access_and_evict(p_node_id, p_sz)?;
        self.mark_access_and_evict(s_node_id, s_sz)?;

        Ok(())
    }

    fn predecessor_leaf_mut<'a>(
        &'a self,
        successor: &LeafWriteGuard<'a, LEAF_FANOUT>,
    ) -> io::Result<LeafWriteGuard<'a, LEAF_FANOUT>> {
        assert_ne!(successor.low_key, &*InlineArray::MIN);

        loop {
            let search_key = self
                .index
                .range::<InlineArray, _>(..&successor.low_key)
                .next_back()
                .unwrap()
                .0;

            let node = self.leaf_for_key_mut(&search_key)?;

            let leaf = node.leaf_write.as_ref().unwrap();
            let leaf_hi = leaf.hi.as_ref().unwrap();
            assert!(
                leaf_hi <= &successor.low_key,
                "somehow, predecessor high key of {:?} \
                is greater than successor low key of {:?}",
                leaf_hi,
                successor.low_key
            );
            if leaf_hi != &successor.low_key {
                continue;
            }
            return Ok(node);
        }
    }

    fn leaf_for_key_mut<'a>(
        &'a self,
        key: &[u8],
    ) -> io::Result<LeafWriteGuard<'a, LEAF_FANOUT>> {
        let (low_key, mut write, node) = self.page_in(key)?;

        let flush_epoch_guard = self.io.flush_epoch.check_in();

        let leaf = write.as_mut().unwrap();

        // NB: these invariants should be enforced in page_in
        assert!(leaf.deleted.is_none());
        assert!(&*leaf.lo <= key);
        if let Some(ref hi) = leaf.hi {
            assert!(
                &**hi > key,
                "while retrieving the leaf for key {:?} \
                we pulled a leaf with hi key of {:?}",
                key,
                hi
            );
        }

        if let Some(old_flush_epoch) = leaf.dirty_flush_epoch {
            if old_flush_epoch != flush_epoch_guard.epoch() {
                log::trace!(
                    "cooperatively flushing {:?} with dirty {:?} after checking into {:?}",
                    node.id,
                    old_flush_epoch,
                    flush_epoch_guard.epoch()
                );
                // cooperatively serialize and put into dirty
                let old_dirty_epoch = leaf.dirty_flush_epoch.take().unwrap();

                // be extra-explicit about serialized bytes
                let leaf_ref: &Leaf<LEAF_FANOUT> = &*leaf;

                let serialized =
                    leaf_ref.serialize(self.io.config.zstd_compression_level);

                log::trace!(
                    "D adding node {} to dirty {:?}",
                    node.id.0,
                    old_dirty_epoch
                );

                if node.id.0 == 0 {
                    assert!(leaf.lo.is_empty());
                }

                self.install_dirty(
                    old_dirty_epoch,
                    node.id,
                    Dirty::CooperativelySerialized {
                        low_key: leaf.lo.clone(),
                        mutation_count: leaf.mutation_count,
                        data: Arc::new(serialized),
                    },
                );

                assert_eq!(
                    old_flush_epoch.increment(),
                    flush_epoch_guard.epoch(),
                    "flush epochs somehow became unlinked"
                );
            }
        }

        if node.id.0 == 0 {
            assert!(low_key.is_empty());
        }
        Ok(LeafWriteGuard {
            flush_epoch_guard,
            leaf_write: ManuallyDrop::new(write),
            inner: self,
            low_key,
            node,
            external_cache_access_and_eviction: false,
        })
    }

    // NB: must not be called while holding a leaf lock - which also means
    // that no two LeafGuards can be held concurrently in the same scope due to
    // this being called in the destructor.
    fn mark_access_and_evict(
        &self,
        node_id: NodeId,
        size: usize,
    ) -> io::Result<()> {
        let mut ca = self.io.cache_advisor.borrow_mut();
        let to_evict = ca.accessed_reuse_buffer(node_id.0, size);
        for (node_to_evict, _rough_size) in to_evict {
            let node = if let Some(n) =
                self.io.node_id_index.get(&node_to_evict)
            {
                if n.id.0 != *node_to_evict {
                    log::warn!("during cache eviction, node to evict did not match current occupant for {:?}", node_to_evict);
                    continue;
                }
                n
            } else {
                log::warn!("during cache eviction, unable to find node to evict for {:?}", node_to_evict);
                continue;
            };

            let mut write = node.inner.write();
            if write.is_none() {
                // already paged out
                continue;
            }
            let leaf: &mut Leaf<LEAF_FANOUT> = write.as_mut().unwrap();

            if let Some(dirty_epoch) = leaf.dirty_flush_epoch {
                // We can't page out this leaf until it has been
                // flushed, because its changes are not yet durable.
                leaf.page_out_on_flush = Some(dirty_epoch);
            } else {
                *write = None;
            }
        }

        Ok(())
    }

    /// Retrieve a value from the `Tree` if it exists.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// db.insert(&[0], vec![0])?;
    /// assert_eq!(db.get(&[0]).unwrap(), Some(sled::InlineArray::from(vec![0])));
    /// assert!(db.get(&[1]).unwrap().is_none());
    /// # Ok(()) }
    /// ```
    pub fn get<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> io::Result<Option<InlineArray>> {
        self.check_error()?;

        let key_ref = key.as_ref();

        let leaf_guard = self.leaf_for_key(key_ref)?;

        let leaf = leaf_guard.leaf_read.as_ref().unwrap();

        if let Some(ref hi) = leaf.hi {
            assert!(&**hi > key_ref);
        }

        Ok(leaf.data.get(key_ref).cloned())
    }

    /// Insert a key to a new value, returning the last value if it
    /// was set.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// assert_eq!(db.insert(&[1, 2, 3], vec![0]).unwrap(), None);
    /// assert_eq!(db.insert(&[1, 2, 3], vec![1]).unwrap(), Some(sled::InlineArray::from(&[0])));
    /// # Ok(()) }
    /// ```
    #[doc(alias = "set")]
    #[doc(alias = "put")]
    pub fn insert<K, V>(
        &self,
        key: K,
        value: V,
    ) -> io::Result<Option<InlineArray>>
    where
        K: AsRef<[u8]>,
        V: Into<InlineArray>,
    {
        self.check_error()?;

        let key_ref = key.as_ref();

        let value_ivec = value.into();
        let mut leaf_guard = self.leaf_for_key_mut(key_ref)?;
        let new_epoch = leaf_guard.epoch();

        let leaf = leaf_guard.leaf_write.as_mut().unwrap();

        // TODO handle prefix encoding

        let ret = leaf.data.insert(key_ref.into(), value_ivec.clone());

        let old_size =
            ret.as_ref().map(|v| key_ref.len() + v.len()).unwrap_or(0);
        let new_size = key_ref.len() + value_ivec.len();

        if new_size > old_size {
            leaf.in_memory_size += new_size - old_size;
        } else {
            leaf.in_memory_size =
                leaf.in_memory_size.saturating_sub(old_size - new_size);
        }

        let split = leaf.split_if_full(new_epoch, &self.io.store);
        if split.is_some() || Some(value_ivec) != ret {
            leaf.mutation_count += 1;
            leaf.dirty_flush_epoch = Some(new_epoch);
            log::trace!(
                "F adding node {} to dirty {:?}",
                leaf_guard.node.id.0,
                new_epoch
            );
            if leaf_guard.node.id.0 == 0 {
                assert!(leaf_guard.low_key.is_empty());
            }

            self.install_dirty(
                new_epoch,
                leaf_guard.node.id,
                Dirty::NotYetSerialized {
                    node: leaf_guard.node.clone(),
                    low_key: leaf_guard.low_key.clone(),
                },
            );
        }
        if let Some((split_key, rhs_node)) = split {
            log::trace!(
                "G adding new from split {:?} to dirty {:?}",
                rhs_node.id,
                new_epoch
            );

            assert_ne!(rhs_node.id.0, 0);
            assert!(!split_key.is_empty());

            self.io.node_id_index.insert(rhs_node.id.0, rhs_node.clone());
            self.index.insert(split_key.clone(), rhs_node.clone());

            self.install_dirty(
                new_epoch,
                rhs_node.id,
                Dirty::NotYetSerialized { node: rhs_node, low_key: split_key },
            );
        }

        // this is for clarity, that leaf_guard is held while
        // inserting into dirty with its guarded epoch
        drop(leaf_guard);

        Ok(ret)
    }

    /// Delete a value, returning the old value if it existed.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// db.insert(&[1], vec![1]);
    /// assert_eq!(db.remove(&[1]).unwrap(), Some(sled::InlineArray::from(vec![1])));
    /// assert!(db.remove(&[1]).unwrap().is_none());
    /// # Ok(()) }
    /// ```
    #[doc(alias = "delete")]
    #[doc(alias = "del")]
    pub fn remove<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> io::Result<Option<InlineArray>> {
        self.check_error()?;

        let key_ref = key.as_ref();

        let mut leaf_guard = self.leaf_for_key_mut(key_ref)?;
        if leaf_guard.node.id.0 == 0 {
            // TODO will break with collections so this is just an early stage scaffolding bit.
            assert!(leaf_guard.low_key.is_empty());
        }

        let new_epoch = leaf_guard.epoch();

        let leaf = leaf_guard.leaf_write.as_mut().unwrap();

        // TODO handle prefix encoding

        let ret = leaf.data.remove(key_ref);

        if ret.is_some() {
            leaf.mutation_count += 1;

            leaf.dirty_flush_epoch = Some(new_epoch);

            log::trace!(
                "H adding node {} to dirty {:?}",
                leaf_guard.node.id.0,
                new_epoch
            );

            self.install_dirty(
                new_epoch,
                leaf_guard.node.id,
                Dirty::NotYetSerialized {
                    low_key: leaf_guard.low_key.clone(),
                    node: leaf_guard.node.clone(),
                },
            );

            if leaf.data.is_empty() && leaf_guard.low_key != InlineArray::MIN {
                self.merge_leaf_into_left_sibling(leaf_guard)?;
            }
        }

        Ok(ret)
    }
    /// Compare and swap. Capable of unique creation, conditional modification,
    /// or deletion. If old is `None`, this will only set the value if it
    /// doesn't exist yet. If new is `None`, will delete the value if old is
    /// correct. If both old and new are `Some`, will modify the value if
    /// old is correct.
    ///
    /// It returns `Ok(Ok(CompareAndSwapSuccess { new_value, previous_value }))` if operation finishes successfully.
    ///
    /// If it fails it returns:
    ///     - `Ok(Err(CompareAndSwapError{ current, proposed }))` if no IO
    ///       error was encountered but the operation
    ///       failed to specify the correct current value. `CompareAndSwapError` contains
    ///       current and proposed values.
    ///     - `Err(io::Error)` if there was a high-level IO problem that prevented
    ///       the operation from logically progressing. This is usually fatal and
    ///       will prevent future requests from functioning, and requires the
    ///       administrator to fix the system issue before restarting.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// // unique creation
    /// assert!(
    ///     db.compare_and_swap(&[1], None as Option<&[u8]>, Some(&[10])).unwrap().is_ok(),
    /// );
    ///
    /// // conditional modification
    /// assert!(
    ///     db.compare_and_swap(&[1], Some(&[10]), Some(&[20])).unwrap().is_ok(),
    /// );
    ///
    /// // failed conditional modification -- the current value is returned in
    /// // the error variant
    /// let operation = db.compare_and_swap(&[1], Some(&[30]), Some(&[40]));
    /// assert!(operation.is_ok()); // the operation succeeded
    /// let modification = operation.unwrap();
    /// assert!(modification.is_err());
    /// let actual_value = modification.unwrap_err();
    /// assert_eq!(actual_value.current.map(|ivec| ivec.to_vec()), Some(vec![20]));
    ///
    /// // conditional deletion
    /// assert!(
    ///     db.compare_and_swap(&[1], Some(&[20]), None as Option<&[u8]>).unwrap().is_ok(),
    /// );
    /// assert!(db.get(&[1]).unwrap().is_none());
    /// # Ok(()) }
    /// ```
    #[doc(alias = "cas")]
    #[doc(alias = "tas")]
    #[doc(alias = "test_and_swap")]
    #[doc(alias = "compare_and_set")]
    pub fn compare_and_swap<K, OV, NV>(
        &self,
        key: K,
        old: Option<OV>,
        new: Option<NV>,
    ) -> CompareAndSwapResult
    where
        K: AsRef<[u8]>,
        OV: AsRef<[u8]>,
        NV: Into<InlineArray>,
    {
        self.check_error()?;

        let key_ref = key.as_ref();

        let mut leaf_guard = self.leaf_for_key_mut(key_ref)?;
        let new_epoch = leaf_guard.epoch();

        let proposed: Option<InlineArray> = new.map(Into::into);

        let leaf = leaf_guard.leaf_write.as_mut().unwrap();

        let current = leaf.data.get(key_ref).cloned();

        let previous_matches = match (old, &current) {
            (None, None) => true,
            (Some(conditional), Some(current))
                if conditional.as_ref() == current.as_ref() =>
            {
                true
            }
            _ => false,
        };

        let ret = if previous_matches {
            if let Some(ref new_value) = proposed {
                leaf.data.insert(key_ref.into(), new_value.clone())
            } else {
                leaf.data.remove(key_ref)
            };

            Ok(CompareAndSwapSuccess {
                new_value: proposed,
                previous_value: current,
            })
        } else {
            Err(CompareAndSwapError { current, proposed })
        };

        let split = leaf.split_if_full(new_epoch, &self.io.store);
        let split_happened = split.is_some();
        if split_happened || ret.is_ok() {
            leaf.mutation_count += 1;

            leaf.dirty_flush_epoch = Some(new_epoch);
            log::trace!(
                "A adding node {} to dirty {:?}",
                leaf_guard.node.id.0,
                new_epoch
            );
            if leaf_guard.node.id.0 == 0 {
                assert!(leaf_guard.low_key.is_empty());
            }

            self.install_dirty(
                new_epoch,
                leaf_guard.node.id,
                Dirty::NotYetSerialized {
                    node: leaf_guard.node.clone(),
                    low_key: leaf_guard.low_key.clone(),
                },
            );
        }
        if let Some((split_key, rhs_node)) = split {
            log::trace!(
                "B adding new from split {:?} to dirty {:?}",
                rhs_node.id,
                new_epoch
            );
            self.install_dirty(
                new_epoch,
                rhs_node.id,
                Dirty::NotYetSerialized {
                    node: rhs_node.clone(),
                    low_key: split_key.clone(),
                },
            );
            self.io.node_id_index.insert(rhs_node.id.0, rhs_node.clone());
            self.index.insert(split_key, rhs_node);
        }

        if leaf.data.is_empty() && leaf_guard.low_key != InlineArray::MIN {
            assert!(!split_happened);
            self.merge_leaf_into_left_sibling(leaf_guard)?;
        }

        Ok(ret)
    }

    /// Fetch the value, apply a function to it and return the result.
    ///
    /// # Note
    ///
    /// This may call the function multiple times if the value has been
    /// changed from other threads in the meantime.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sled::{Config, InlineArray};
    ///
    /// let config = Config::tmp().unwrap();
    /// let db: sled::Db<1024> = config.open()?;
    ///
    /// fn u64_to_ivec(number: u64) -> InlineArray {
    ///     InlineArray::from(number.to_be_bytes().to_vec())
    /// }
    ///
    /// let zero = u64_to_ivec(0);
    /// let one = u64_to_ivec(1);
    /// let two = u64_to_ivec(2);
    /// let three = u64_to_ivec(3);
    ///
    /// fn increment(old: Option<&[u8]>) -> Option<Vec<u8>> {
    ///     let number = match old {
    ///         Some(bytes) => {
    ///             let array: [u8; 8] = bytes.try_into().unwrap();
    ///             let number = u64::from_be_bytes(array);
    ///             number + 1
    ///         }
    ///         None => 0,
    ///     };
    ///
    ///     Some(number.to_be_bytes().to_vec())
    /// }
    ///
    /// assert_eq!(db.update_and_fetch("counter", increment).unwrap(), Some(zero));
    /// assert_eq!(db.update_and_fetch("counter", increment).unwrap(), Some(one));
    /// assert_eq!(db.update_and_fetch("counter", increment).unwrap(), Some(two));
    /// assert_eq!(db.update_and_fetch("counter", increment).unwrap(), Some(three));
    /// # Ok(()) }
    /// ```
    pub fn update_and_fetch<K, V, F>(
        &self,
        key: K,
        mut f: F,
    ) -> io::Result<Option<InlineArray>>
    where
        K: AsRef<[u8]>,
        F: FnMut(Option<&[u8]>) -> Option<V>,
        V: Into<InlineArray>,
    {
        let key_ref = key.as_ref();
        let mut current = self.get(key_ref)?;

        loop {
            let tmp = current.as_ref().map(AsRef::as_ref);
            let next = f(tmp).map(Into::into);
            match self.compare_and_swap::<_, _, InlineArray>(
                key_ref,
                tmp,
                next.clone(),
            )? {
                Ok(_) => return Ok(next),
                Err(CompareAndSwapError { current: cur, .. }) => {
                    current = cur;
                }
            }
        }
    }

    /// Fetch the value, apply a function to it and return the previous value.
    ///
    /// # Note
    ///
    /// This may call the function multiple times if the value has been
    /// changed from other threads in the meantime.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sled::{Config, InlineArray};
    ///
    /// let config = Config::tmp().unwrap();
    /// let db: sled::Db<1024> = config.open()?;
    ///
    /// fn u64_to_ivec(number: u64) -> InlineArray {
    ///     InlineArray::from(number.to_be_bytes().to_vec())
    /// }
    ///
    /// let zero = u64_to_ivec(0);
    /// let one = u64_to_ivec(1);
    /// let two = u64_to_ivec(2);
    ///
    /// fn increment(old: Option<&[u8]>) -> Option<Vec<u8>> {
    ///     let number = match old {
    ///         Some(bytes) => {
    ///             let array: [u8; 8] = bytes.try_into().unwrap();
    ///             let number = u64::from_be_bytes(array);
    ///             number + 1
    ///         }
    ///         None => 0,
    ///     };
    ///
    ///     Some(number.to_be_bytes().to_vec())
    /// }
    ///
    /// assert_eq!(db.fetch_and_update("counter", increment).unwrap(), None);
    /// assert_eq!(db.fetch_and_update("counter", increment).unwrap(), Some(zero));
    /// assert_eq!(db.fetch_and_update("counter", increment).unwrap(), Some(one));
    /// assert_eq!(db.fetch_and_update("counter", increment).unwrap(), Some(two));
    /// # Ok(()) }
    /// ```
    pub fn fetch_and_update<K, V, F>(
        &self,
        key: K,
        mut f: F,
    ) -> io::Result<Option<InlineArray>>
    where
        K: AsRef<[u8]>,
        F: FnMut(Option<&[u8]>) -> Option<V>,
        V: Into<InlineArray>,
    {
        let key_ref = key.as_ref();
        let mut current = self.get(key_ref)?;

        loop {
            let tmp = current.as_ref().map(AsRef::as_ref);
            let next = f(tmp);
            match self.compare_and_swap(key_ref, tmp, next)? {
                Ok(_) => return Ok(current),
                Err(CompareAndSwapError { current: cur, .. }) => {
                    current = cur;
                }
            }
        }
    }

    pub fn iter(&self) -> Iter<LEAF_FANOUT> {
        Iter {
            prefetched: VecDeque::new(),
            prefetched_back: VecDeque::new(),
            next_fetch: Some(InlineArray::MIN),
            next_back_last_lo: None,
            next_calls: 0,
            next_back_calls: 0,
            inner: self,
            bounds: (Bound::Unbounded, Bound::Unbounded),
        }
    }

    pub fn range<K, R>(&self, range: R) -> Iter<LEAF_FANOUT>
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
    {
        let start: Bound<InlineArray> =
            map_bound(range.start_bound(), |b| InlineArray::from(b.as_ref()));
        let end: Bound<InlineArray> =
            map_bound(range.end_bound(), |b| InlineArray::from(b.as_ref()));

        let next_fetch = Some(match &start {
            Bound::Included(b) | Bound::Excluded(b) => b.clone(),
            Bound::Unbounded => InlineArray::MIN,
        });

        Iter {
            prefetched: VecDeque::new(),
            prefetched_back: VecDeque::new(),
            next_fetch,
            next_back_last_lo: None,
            next_calls: 0,
            next_back_calls: 0,
            inner: self,
            bounds: (start, end),
        }
    }

    /// Create a new batched update that is applied
    /// atomically. Readers will atomically see all updates
    /// at an atomic instant, and if the database crashes,
    /// either 0% or 100% of the full batch will be recovered,
    /// but never a partial batch. If a `flush` operation succeeds
    /// after this, it is guaranteed that 100% of the batch will be
    /// visible, unless later concurrent updates changed the values
    /// before the flush.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let _ = std::fs::remove_dir_all("batch_doctest");
    /// # let db: sled::Db<1024> = sled::open("batch_doctest")?;
    /// db.insert("key_0", "val_0")?;
    ///
    /// let mut batch = sled::Batch::default();
    /// batch.insert("key_a", "val_a");
    /// batch.insert("key_b", "val_b");
    /// batch.insert("key_c", "val_c");
    /// batch.remove("key_0");
    ///
    /// db.apply_batch(batch)?;
    /// // key_0 no longer exists, and key_a, key_b, and key_c
    /// // now do exist.
    /// # let _ = std::fs::remove_dir_all("batch_doctest");
    /// # Ok(()) }
    /// ```
    pub fn apply_batch(&self, batch: Batch) -> io::Result<()> {
        // NB: we rely on lexicographic lock acquisition
        // by iterating over the batch's BTreeMap to avoid
        // deadlocks during 2PL
        let mut acquired_locks: BTreeMap<
            InlineArray,
            (
                ArcRwLockWriteGuard<RawRwLock, Option<Box<Leaf<LEAF_FANOUT>>>>,
                Node<LEAF_FANOUT>,
            ),
        > = BTreeMap::new();

        // Phase 1: lock acquisition
        let mut last: Option<(
            InlineArray,
            ArcRwLockWriteGuard<RawRwLock, Option<Box<Leaf<LEAF_FANOUT>>>>,
            Node<LEAF_FANOUT>,
        )> = None;

        for key in batch.writes.keys() {
            if let Some((_lo, w, _id)) = &last {
                let leaf = w.as_ref().unwrap();
                assert!(&leaf.lo <= key);
                if let Some(hi) = &leaf.hi {
                    if hi <= key {
                        let (lo, w, n) = last.take().unwrap();
                        acquired_locks.insert(lo, (w, n));
                    }
                }
            }
            if last.is_none() {
                // TODO evaluate whether this is correct, as page_in performs
                // cache maintenance internally if it over/undershoots due to
                // concurrent modifications.
                last = Some(self.page_in(key)?);
            }
        }

        if let Some((lo, w, id)) = last.take() {
            acquired_locks.insert(lo, (w, id));
        }

        // NB: add the flush epoch at the end of the lock acquisition
        // process when all locks have been acquired, to avoid situations
        // where a leaf is already dirty with an epoch "from the future".
        let flush_epoch_guard = self.io.flush_epoch.check_in();
        let new_epoch = flush_epoch_guard.epoch();

        // Flush any leaves that are dirty from a previous flush epoch
        // before performing operations.
        for (write, node) in acquired_locks.values_mut() {
            let leaf = write.as_mut().unwrap();
            if let Some(old_flush_epoch) = leaf.dirty_flush_epoch {
                if old_flush_epoch != new_epoch {
                    assert_eq!(old_flush_epoch.increment(), new_epoch);

                    log::trace!(
                        "cooperatively flushing {:?} with dirty {:?} after checking into {:?}",
                        node.id,
                        old_flush_epoch,
                        new_epoch
                    );

                    // cooperatively serialize and put into dirty
                    let old_dirty_epoch =
                        leaf.dirty_flush_epoch.take().unwrap();

                    // be extra-explicit about serialized bytes
                    let leaf_ref: &Leaf<LEAF_FANOUT> = &*leaf;

                    let serialized = leaf_ref
                        .serialize(self.io.config.zstd_compression_level);

                    log::trace!(
                        "C adding node {} to dirty epoch {:?}",
                        node.id.0,
                        old_dirty_epoch
                    );

                    self.install_dirty(
                        old_dirty_epoch,
                        node.id,
                        Dirty::CooperativelySerialized {
                            mutation_count: leaf_ref.mutation_count,
                            low_key: leaf.lo.clone(),
                            data: Arc::new(serialized),
                        },
                    );

                    assert_eq!(
                        old_flush_epoch.increment(),
                        flush_epoch_guard.epoch(),
                        "flush epochs somehow became unlinked"
                    );
                }
            }
        }

        let mut splits: Vec<(InlineArray, Node<LEAF_FANOUT>)> = vec![];

        // Insert and split when full
        for (key, value_opt) in batch.writes {
            let range = ..=&key;
            let (_lo, (ref mut w, _id)) = acquired_locks
                .range_mut::<InlineArray, _>(range)
                .next_back()
                .unwrap();
            let leaf = w.as_mut().unwrap();

            assert!(leaf.lo <= key);
            if let Some(hi) = &leaf.hi {
                assert!(hi > &key);
            }

            if let Some(value) = value_opt {
                leaf.data.insert(key, value);

                if let Some((split_key, rhs_node)) =
                    leaf.split_if_full(new_epoch, &self.io.store)
                {
                    let write = rhs_node.inner.write_arc();
                    assert!(write.is_some());

                    splits.push((split_key.clone(), rhs_node.clone()));
                    acquired_locks.insert(split_key, (write, rhs_node));
                }
            } else {
                leaf.data.remove(&key);
            }
        }

        // Make splits globally visible
        for (split_key, rhs_node) in splits {
            self.io.node_id_index.insert(rhs_node.id.0, rhs_node.clone());
            self.index.insert(split_key, rhs_node);
        }

        // Add all written leaves to dirty and prepare to mark cache accesses
        let mut cache_accesses = Vec::with_capacity(acquired_locks.len());
        for (low_key, (write, node)) in &mut acquired_locks {
            let leaf = write.as_mut().unwrap();
            leaf.dirty_flush_epoch = Some(new_epoch);
            leaf.mutation_count += 1;
            cache_accesses.push((node.id, leaf.in_memory_size));
            self.install_dirty(
                new_epoch,
                node.id,
                Dirty::NotYetSerialized {
                    node: node.clone(),
                    low_key: low_key.clone(),
                },
            );
        }

        // Drop locks
        drop(acquired_locks);

        // Perform cache maintenance
        for (node_id, size) in cache_accesses {
            self.mark_access_and_evict(node_id, size)?;
        }

        Ok(())
    }

    /// Returns `true` if the `Tree` contains a value for
    /// the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// db.insert(&[0], vec![0])?;
    /// assert!(db.contains_key(&[0])?);
    /// assert!(!db.contains_key(&[1])?);
    /// # Ok(()) }
    /// ```
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> io::Result<bool> {
        self.get(key).map(|v| v.is_some())
    }

    /// Retrieve the key and value before the provided key,
    /// if one exists.
    ///
    /// # Note
    /// The order follows the Ord implementation for `Vec<u8>`:
    ///
    /// `[] < [0] < [255] < [255, 0] < [255, 255] ...`
    ///
    /// To retain the ordering of numerical types use big endian reprensentation
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sled::InlineArray;
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// for i in 0..10 {
    ///     db.insert(&[i], vec![i])
    ///         .expect("should write successfully");
    /// }
    ///
    /// assert!(db.get_lt(&[]).unwrap().is_none());
    /// assert!(db.get_lt(&[0]).unwrap().is_none());
    /// assert_eq!(
    ///     db.get_lt(&[1]).unwrap(),
    ///     Some((InlineArray::from(&[0]), InlineArray::from(&[0])))
    /// );
    /// assert_eq!(
    ///     db.get_lt(&[9]).unwrap(),
    ///     Some((InlineArray::from(&[8]), InlineArray::from(&[8])))
    /// );
    /// assert_eq!(
    ///     db.get_lt(&[10]).unwrap(),
    ///     Some((InlineArray::from(&[9]), InlineArray::from(&[9])))
    /// );
    /// assert_eq!(
    ///     db.get_lt(&[255]).unwrap(),
    ///     Some((InlineArray::from(&[9]), InlineArray::from(&[9])))
    /// );
    /// # Ok(()) }
    /// ```
    pub fn get_lt<K>(
        &self,
        key: K,
    ) -> io::Result<Option<(InlineArray, InlineArray)>>
    where
        K: AsRef<[u8]>,
    {
        self.range(..key).next_back().transpose()
    }

    /// Retrieve the next key and value from the `Tree` after the
    /// provided key.
    ///
    /// # Note
    /// The order follows the Ord implementation for `Vec<u8>`:
    ///
    /// `[] < [0] < [255] < [255, 0] < [255, 255] ...`
    ///
    /// To retain the ordering of numerical types use big endian reprensentation
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sled::InlineArray;
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// for i in 0..10 {
    ///     db.insert(&[i], vec![i])?;
    /// }
    ///
    /// assert_eq!(
    ///     db.get_gt(&[]).unwrap(),
    ///     Some((InlineArray::from(&[0]), InlineArray::from(&[0])))
    /// );
    /// assert_eq!(
    ///     db.get_gt(&[0]).unwrap(),
    ///     Some((InlineArray::from(&[1]), InlineArray::from(&[1])))
    /// );
    /// assert_eq!(
    ///     db.get_gt(&[1]).unwrap(),
    ///     Some((InlineArray::from(&[2]), InlineArray::from(&[2])))
    /// );
    /// assert_eq!(
    ///     db.get_gt(&[8]).unwrap(),
    ///     Some((InlineArray::from(&[9]), InlineArray::from(&[9])))
    /// );
    /// assert!(db.get_gt(&[9]).unwrap().is_none());
    ///
    /// db.insert(500u16.to_be_bytes(), vec![10]);
    /// assert_eq!(
    ///     db.get_gt(&499u16.to_be_bytes()).unwrap(),
    ///     Some((InlineArray::from(&500u16.to_be_bytes()), InlineArray::from(&[10])))
    /// );
    /// # Ok(()) }
    /// ```
    pub fn get_gt<K>(
        &self,
        key: K,
    ) -> io::Result<Option<(InlineArray, InlineArray)>>
    where
        K: AsRef<[u8]>,
    {
        self.range((ops::Bound::Excluded(key), ops::Bound::Unbounded))
            .next()
            .transpose()
    }

    /// Create an iterator over tuples of keys and values
    /// where all keys start with the given prefix.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// use sled::InlineArray;
    /// db.insert(&[0, 0, 0], vec![0, 0, 0])?;
    /// db.insert(&[0, 0, 1], vec![0, 0, 1])?;
    /// db.insert(&[0, 0, 2], vec![0, 0, 2])?;
    /// db.insert(&[0, 0, 3], vec![0, 0, 3])?;
    /// db.insert(&[0, 1, 0], vec![0, 1, 0])?;
    /// db.insert(&[0, 1, 1], vec![0, 1, 1])?;
    ///
    /// let prefix: &[u8] = &[0, 0];
    /// let mut r = db.scan_prefix(prefix);
    /// assert_eq!(
    ///     r.next().unwrap().unwrap(),
    ///     (InlineArray::from(&[0, 0, 0]), InlineArray::from(&[0, 0, 0]))
    /// );
    /// assert_eq!(
    ///     r.next().unwrap().unwrap(),
    ///     (InlineArray::from(&[0, 0, 1]), InlineArray::from(&[0, 0, 1]))
    /// );
    /// assert_eq!(
    ///     r.next().unwrap().unwrap(),
    ///     (InlineArray::from(&[0, 0, 2]), InlineArray::from(&[0, 0, 2]))
    /// );
    /// assert_eq!(
    ///     r.next().unwrap().unwrap(),
    ///     (InlineArray::from(&[0, 0, 3]), InlineArray::from(&[0, 0, 3]))
    /// );
    /// assert!(r.next().is_none());
    /// # Ok(()) }
    /// ```
    pub fn scan_prefix<'a, P>(&'a self, prefix: P) -> Iter<'a, LEAF_FANOUT>
    where
        P: AsRef<[u8]>,
    {
        let prefix_ref = prefix.as_ref();
        let mut upper = prefix_ref.to_vec();

        while let Some(last) = upper.pop() {
            if last < u8::MAX {
                upper.push(last + 1);
                return self.range(prefix_ref..&upper);
            }
        }

        self.range(prefix..)
    }

    /// Returns the first key and value in the `Tree`, or
    /// `None` if the `Tree` is empty.
    pub fn first(&self) -> io::Result<Option<(InlineArray, InlineArray)>> {
        self.iter().next().transpose()
    }

    /// Returns the last key and value in the `Tree`, or
    /// `None` if the `Tree` is empty.
    pub fn last(&self) -> io::Result<Option<(InlineArray, InlineArray)>> {
        self.iter().next_back().transpose()
    }

    /// Atomically removes the maximum item in the `Tree` instance.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// db.insert(&[0], vec![0])?;
    /// db.insert(&[1], vec![10])?;
    /// db.insert(&[2], vec![20])?;
    /// db.insert(&[3], vec![30])?;
    /// db.insert(&[4], vec![40])?;
    /// db.insert(&[5], vec![50])?;
    ///
    /// assert_eq!(&db.pop_last()?.unwrap().0, &[5]);
    /// assert_eq!(&db.pop_last()?.unwrap().0, &[4]);
    /// assert_eq!(&db.pop_last()?.unwrap().0, &[3]);
    /// assert_eq!(&db.pop_last()?.unwrap().0, &[2]);
    /// assert_eq!(&db.pop_last()?.unwrap().0, &[1]);
    /// assert_eq!(&db.pop_last()?.unwrap().0, &[0]);
    /// assert_eq!(db.pop_last()?, None);
    /// # Ok(()) }
    /// ```
    pub fn pop_last(&self) -> io::Result<Option<(InlineArray, InlineArray)>> {
        loop {
            if let Some(first_res) = self.iter().next_back() {
                let first = first_res?;
                if self
                    .compare_and_swap::<_, _, &[u8]>(
                        &first.0,
                        Some(&first.1),
                        None,
                    )?
                    .is_ok()
                {
                    log::trace!("pop_last removed item {:?}", first);
                    return Ok(Some(first));
                }
            // try again
            } else {
                log::trace!("pop_last removed nothing from empty tree");
                return Ok(None);
            }
        }
    }

    /// Pops the last kv pair in the provided range, or returns `Ok(None)` if nothing
    /// exists within that range.
    ///
    /// # Panics
    ///
    /// This will panic if the provided range's end_bound() == Bound::Excluded(K::MIN).
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    ///
    /// let data = vec![
    ///     (b"key 1", b"value 1"),
    ///     (b"key 2", b"value 2"),
    ///     (b"key 3", b"value 3")
    /// ];
    ///
    /// for (k, v) in data {
    ///     db.insert(k, v).unwrap();
    /// }
    ///
    /// let r1 = db.pop_last_in_range(b"key 1".as_ref()..=b"key 3").unwrap();
    /// assert_eq!(Some((b"key 3".into(), b"value 3".into())), r1);
    ///
    /// let r2 = db.pop_last_in_range(b"key 1".as_ref()..b"key 3").unwrap();
    /// assert_eq!(Some((b"key 2".into(), b"value 2".into())), r2);
    ///
    /// let r3 = db.pop_last_in_range(b"key 4".as_ref()..).unwrap();
    /// assert!(r3.is_none());
    ///
    /// let r4 = db.pop_last_in_range(b"key 2".as_ref()..=b"key 3").unwrap();
    /// assert!(r4.is_none());
    ///
    /// let r5 = db.pop_last_in_range(b"key 0".as_ref()..=b"key 3").unwrap();
    /// assert_eq!(Some((b"key 1".into(), b"value 1".into())), r5);
    ///
    /// let r6 = db.pop_last_in_range(b"key 0".as_ref()..=b"key 3").unwrap();
    /// assert!(r6.is_none());
    /// # Ok (()) }
    /// ```
    pub fn pop_last_in_range<K, R>(
        &self,
        range: R,
    ) -> io::Result<Option<(InlineArray, InlineArray)>>
    where
        K: AsRef<[u8]>,
        R: Clone + RangeBounds<K>,
    {
        loop {
            let mut r = self.range(range.clone());
            let (k, v) = if let Some(kv_res) = r.next_back() {
                kv_res?
            } else {
                return Ok(None);
            };
            if self
                .compare_and_swap(&k, Some(&v), None as Option<InlineArray>)?
                .is_ok()
            {
                return Ok(Some((k, v)));
            }
        }
    }

    /// Atomically removes the minimum item in the `Tree` instance.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// db.insert(&[0], vec![0])?;
    /// db.insert(&[1], vec![10])?;
    /// db.insert(&[2], vec![20])?;
    /// db.insert(&[3], vec![30])?;
    /// db.insert(&[4], vec![40])?;
    /// db.insert(&[5], vec![50])?;
    ///
    /// assert_eq!(&db.pop_first()?.unwrap().0, &[0]);
    /// assert_eq!(&db.pop_first()?.unwrap().0, &[1]);
    /// assert_eq!(&db.pop_first()?.unwrap().0, &[2]);
    /// assert_eq!(&db.pop_first()?.unwrap().0, &[3]);
    /// assert_eq!(&db.pop_first()?.unwrap().0, &[4]);
    /// assert_eq!(&db.pop_first()?.unwrap().0, &[5]);
    /// assert_eq!(db.pop_first()?, None);
    /// # Ok(()) }
    /// ```
    pub fn pop_first(&self) -> io::Result<Option<(InlineArray, InlineArray)>> {
        loop {
            if let Some(first_res) = self.iter().next() {
                let first = first_res?;
                if self
                    .compare_and_swap::<_, _, &[u8]>(
                        &first.0,
                        Some(&first.1),
                        None,
                    )?
                    .is_ok()
                {
                    log::trace!("pop_first removed item {:?}", first);
                    return Ok(Some(first));
                }
            // try again
            } else {
                log::trace!("pop_first removed nothing from empty tree");
                return Ok(None);
            }
        }
    }

    /// Pops the first kv pair in the provided range, or returns `Ok(None)` if nothing
    /// exists within that range.
    ///
    /// # Panics
    ///
    /// This will panic if the provided range's end_bound() == Bound::Excluded(K::MIN).
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    ///
    /// let data = vec![
    ///     (b"key 1", b"value 1"),
    ///     (b"key 2", b"value 2"),
    ///     (b"key 3", b"value 3")
    /// ];
    ///
    /// for (k, v) in data {
    ///     db.insert(k, v).unwrap();
    /// }
    ///
    /// let r1 = db.pop_first_in_range("key 1".as_ref()..="key 3").unwrap();
    /// assert_eq!(Some((b"key 1".into(), b"value 1".into())), r1);
    ///
    /// let r2 = db.pop_first_in_range("key 1".as_ref().."key 3").unwrap();
    /// assert_eq!(Some((b"key 2".into(), b"value 2".into())), r2);
    ///
    /// let r3_res: std::io::Result<Vec<_>> = db.range(b"key 4".as_ref()..).collect();
    /// let r3: Vec<_> = r3_res.unwrap();
    /// assert!(r3.is_empty());
    ///
    /// let r4 = db.pop_first_in_range("key 2".as_ref()..="key 3").unwrap();
    /// assert_eq!(Some((b"key 3".into(), b"value 3".into())), r4);
    /// # Ok (()) }
    /// ```
    pub fn pop_first_in_range<K, R>(
        &self,
        range: R,
    ) -> io::Result<Option<(InlineArray, InlineArray)>>
    where
        K: AsRef<[u8]>,
        R: Clone + RangeBounds<K>,
    {
        loop {
            let mut r = self.range(range.clone());
            let (k, v) = if let Some(kv_res) = r.next() {
                kv_res?
            } else {
                return Ok(None);
            };
            if self
                .compare_and_swap(&k, Some(&v), None as Option<InlineArray>)?
                .is_ok()
            {
                return Ok(Some((k, v)));
            }
        }
    }

    /// Returns the number of elements in this tree.
    ///
    /// Beware: performs a full O(n) scan under the hood.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = sled::Config::tmp().unwrap();
    /// # let db: sled::Db<1024> = config.open()?;
    /// db.insert(b"a", vec![0]);
    /// db.insert(b"b", vec![1]);
    /// assert_eq!(db.len(), 2);
    /// # Ok(()) }
    /// ```
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Returns `true` if the `Tree` contains no elements.
    ///
    /// This is O(1), as we only need to see if an iterator
    /// returns anything for the first call to `next()`.
    pub fn is_empty(&self) -> io::Result<bool> {
        if let Some(res) = self.iter().next() {
            res?;
            Ok(false)
        } else {
            Ok(true)
        }
    }

    /// Clears the `Tree`, removing all values.
    ///
    /// Note that this is not atomic.
    ///
    /// Beware: performs a full O(n) scan under the hood.
    pub fn clear(&self) -> io::Result<()> {
        for k in self.iter().keys() {
            let key = k?;
            let _old = self.remove(key)?;
        }
        Ok(())
    }

    /// Returns the CRC32 of all keys and values
    /// in this Tree.
    ///
    /// This is O(N) and locks the underlying tree
    /// for the duration of the entire scan.
    pub fn checksum(&self) -> io::Result<u32> {
        let mut hasher = crc32fast::Hasher::new();
        for kv_res in self.iter() {
            let (k, v) = kv_res?;
            hasher.update(&k);
            hasher.update(&v);
        }
        Ok(hasher.finalize())
    }
}

#[allow(unused)]
pub struct Iter<'a, const LEAF_FANOUT: usize> {
    inner: &'a Db<LEAF_FANOUT>,
    bounds: (Bound<InlineArray>, Bound<InlineArray>),
    next_calls: usize,
    next_back_calls: usize,
    next_fetch: Option<InlineArray>,
    next_back_last_lo: Option<InlineArray>,
    prefetched: VecDeque<(InlineArray, InlineArray)>,
    prefetched_back: VecDeque<(InlineArray, InlineArray)>,
}

impl<'a, const LEAF_FANOUT: usize> Iterator for Iter<'a, LEAF_FANOUT> {
    type Item = io::Result<(InlineArray, InlineArray)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_calls += 1;
        while self.prefetched.is_empty() {
            let search_key = if let Some(last) = &self.next_fetch {
                last.clone()
            } else {
                return None;
            };

            let node = match self.inner.leaf_for_key(&search_key) {
                Ok(n) => n,
                Err(e) => return Some(Err(e)),
            };

            let leaf = node.leaf_read.as_ref().unwrap();

            if let Some(leaf_hi) = &leaf.hi {
                if leaf_hi <= &search_key {
                    // concurrent merge, retry
                    log::trace!("undershot in interator, retrying search");
                    continue;
                }
            }

            if leaf.lo > search_key {
                // concurrent successor split, retry
                log::trace!("overshot in interator, retrying search");
                continue;
            }

            for (k, v) in leaf.data.iter() {
                if self.bounds.contains(k) && &search_key <= k {
                    self.prefetched.push_back((k.clone(), v.clone()));
                }
            }
            self.next_fetch = leaf.hi.clone();
        }

        self.prefetched.pop_front().map(Ok)
    }
}

impl<'a, const LEAF_FANOUT: usize> DoubleEndedIterator
    for Iter<'a, LEAF_FANOUT>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.next_back_calls += 1;
        while self.prefetched_back.is_empty() {
            let search_key: InlineArray = if let Some(last) =
                &self.next_back_last_lo
            {
                if !self.bounds.contains(last) || last == &InlineArray::MIN {
                    return None;
                }
                self.inner
                    .index
                    .range::<InlineArray, _>(..last)
                    .next_back()
                    .unwrap()
                    .0
            } else {
                match &self.bounds.1 {
                    Bound::Included(k) => k.clone(),
                    Bound::Excluded(k) if k == &InlineArray::MIN => {
                        InlineArray::MIN
                    }
                    Bound::Excluded(k) => self.inner.index.get_lt(k).unwrap().0,
                    Bound::Unbounded => self.inner.index.last().unwrap().0,
                }
            };

            let node = match self.inner.leaf_for_key(&search_key) {
                Ok(n) => n,
                Err(e) => return Some(Err(e)),
            };

            let leaf = node.leaf_read.as_ref().unwrap();

            if leaf.lo > search_key {
                // concurrent successor split, retry
                log::trace!("overshot in reverse interator, retrying search");
                continue;
            }

            // determine if we undershot our target due to concurrent modifications
            let undershot =
                match (&leaf.hi, &self.next_back_last_lo, &self.bounds.1) {
                    (Some(leaf_hi), Some(last_lo), _) => leaf_hi < last_lo,
                    (Some(_leaf_hi), None, Bound::Unbounded) => true,
                    (Some(leaf_hi), None, Bound::Included(bound_key)) => {
                        leaf_hi <= bound_key
                    }
                    (Some(leaf_hi), None, Bound::Excluded(bound_key)) => {
                        leaf_hi < bound_key
                    }
                    (None, _, _) => false,
                };

            if undershot {
                log::trace!(
                    "undershoot detected in reverse iterator with \
                    (leaf_hi, next_back_last_lo, self.bounds.1) being {:?}",
                    (&leaf.hi, &self.next_back_last_lo, &self.bounds.1)
                );
                continue;
            }

            for (k, v) in leaf.data.iter() {
                if self.bounds.contains(k) {
                    let beneath_last_lo =
                        if let Some(last_lo) = &self.next_back_last_lo {
                            k < last_lo
                        } else {
                            true
                        };
                    if beneath_last_lo {
                        self.prefetched_back.push_back((k.clone(), v.clone()));
                    }
                }
            }
            self.next_back_last_lo = Some(leaf.lo.clone());
        }

        self.prefetched_back.pop_back().map(Ok)
    }
}

impl<'a, const LEAF_FANOUT: usize> Iter<'a, LEAF_FANOUT> {
    pub fn keys(
        self,
    ) -> impl 'a + DoubleEndedIterator<Item = io::Result<InlineArray>> {
        self.into_iter().map(|kv_res| kv_res.map(|(k, _v)| k))
    }

    pub fn values(
        self,
    ) -> impl 'a + DoubleEndedIterator<Item = io::Result<InlineArray>> {
        self.into_iter().map(|kv_res| kv_res.map(|(_k, v)| v))
    }
}

impl<'a, const LEAF_FANOUT: usize> IntoIterator for &'a Db<LEAF_FANOUT> {
    type Item = io::Result<(InlineArray, InlineArray)>;
    type IntoIter = Iter<'a, LEAF_FANOUT>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A batch of updates that will
/// be applied atomically to the
/// Tree.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use sled::{Batch, open};
///
/// # let _ = std::fs::remove_dir_all("batch_db_2");
/// let db: sled::Db<1024> = open("batch_db_2")?;
/// db.insert("key_0", "val_0")?;
///
/// let mut batch = Batch::default();
/// batch.insert("key_a", "val_a");
/// batch.insert("key_b", "val_b");
/// batch.insert("key_c", "val_c");
/// batch.remove("key_0");
///
/// db.apply_batch(batch)?;
/// // key_0 no longer exists, and key_a, key_b, and key_c
/// // now do exist.
/// # let _ = std::fs::remove_dir_all("batch_db_2");
/// # Ok(()) }
/// ```
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Batch {
    pub(crate) writes:
        std::collections::BTreeMap<InlineArray, Option<InlineArray>>,
}

impl Batch {
    /// Set a key to a new value
    pub fn insert<K, V>(&mut self, key: K, value: V)
    where
        K: Into<InlineArray>,
        V: Into<InlineArray>,
    {
        self.writes.insert(key.into(), Some(value.into()));
    }

    /// Remove a key
    pub fn remove<K>(&mut self, key: K)
    where
        K: Into<InlineArray>,
    {
        self.writes.insert(key.into(), None);
    }

    /// Get a value if it is present in the `Batch`.
    /// `Some(None)` means it's present as a deletion.
    pub fn get<K: AsRef<[u8]>>(&self, k: K) -> Option<Option<&InlineArray>> {
        let inner = self.writes.get(k.as_ref())?;
        Some(inner.as_ref())
    }
}

fn initialize<const LEAF_FANOUT: usize>(
    index_data: &[(NodeId, CollectionId, InlineArray)],
    first_id_opt: Option<u64>,
) -> ConcurrentMap<
    InlineArray,
    Node<LEAF_FANOUT>,
    INDEX_FANOUT,
    EBR_LOCAL_GC_BUFFER_SIZE,
> {
    if index_data.is_empty() {
        let first_id = first_id_opt.unwrap();
        let first_leaf = Leaf {
            hi: None,
            lo: InlineArray::default(),
            // this does not need to be marked as dirty until it actually
            // receives inserted data
            dirty_flush_epoch: None,
            prefix_length: 0,
            data: StackMap::new(),
            in_memory_size: mem::size_of::<Leaf<LEAF_FANOUT>>(),
            mutation_count: 0,
            page_out_on_flush: None,
            deleted: None,
        };
        let first_node = Node {
            id: NodeId(first_id),
            inner: Arc::new(Some(Box::new(first_leaf)).into()),
        };
        return [(InlineArray::default(), first_node)].into_iter().collect();
    }

    let ret = ConcurrentMap::default();

    for (node_id, collection_id, low_key) in index_data {
        let node = Node { id: *node_id, inner: Arc::new(None.into()) };
        ret.insert(low_key.clone(), node);
    }

    ret
}
