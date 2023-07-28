// TODO for write batches, find new way of dropping LeafGuard types that avoids
//      calling mark_access_and_evict while any mutex is held for leaves. must
//      first drop all locks then do mark access on each.
// TODO document marble not to use too high pid otherwise recovery will lock
//      on essentially infinite recovery
// TODO marble maintenance w/ speculative write followed by CAS in pt
//      maybe with global writer lock that controls flushers too
// TODO re-enable paging out
// TODO write background flusher
// TODO max key and value sizes w/ corresponding heap

#![allow(unused)]

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::mem::{self, ManuallyDrop};
use std::num::NonZeroU64;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};

use cache_advisor::CacheAdvisor;
use concurrent_map::ConcurrentMap;
use ebr::Ebr;
use marble::Marble;
use parking_lot::{
    lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard},
    RawRwLock, RwLock,
};
use stack_map::StackMap;

use crate::*;

pub fn open_default<P: AsRef<Path>>(path: P) -> io::Result<Db> {
    Config { path: path.as_ref().into(), ..Default::default() }.open()
}

fn initialize<
    const INDEX_FANOUT: usize,
    const LEAF_FANOUT: usize,
    const EBR_LOCAL_GC_BUFFER_SIZE: usize,
>(
    index_data: &[(u64, InlineArray)],
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
        };
        let first_node = Node {
            id: NodeId(first_id),
            inner: Arc::new(Some(Box::new(first_leaf)).into()),
        };
        return [(InlineArray::default(), first_node)].into_iter().collect();
    }

    let ret = ConcurrentMap::default();

    for (id, low_key) in index_data {
        let node = Node { id: NodeId(*id), inner: Arc::new(None.into()) };
        ret.insert(low_key.clone(), node);
    }

    ret
}

#[derive(Debug, Clone)]
pub struct Config {
    pub path: PathBuf,
    /// Cache size in bytes. Default is 1gb.
    pub cache_size: usize,
    /// The percentage of the cache that is dedicated to the
    /// scan-resistant entry cache.
    pub entry_cache_percent: u8,
    /// Start a background thread that flushes data to disk
    /// every few milliseconds. Defaults to every 200ms.
    pub flush_every_ms: Option<usize>,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            path: "bloodstone.default".into(),
            flush_every_ms: Some(200),
            cache_size: 1024 * 1024 * 1024,
            entry_cache_percent: 20,
        }
    }
}

impl Config {
    pub fn open<
        const INDEX_FANOUT: usize,
        const LEAF_FANOUT: usize,
        const EBR_LOCAL_GC_BUFFER_SIZE: usize,
    >(
        &self,
    ) -> io::Result<Db<INDEX_FANOUT, LEAF_FANOUT, EBR_LOCAL_GC_BUFFER_SIZE>>
    {
        let (store, index_data) = marble::recover(&self.path)?;

        let max_id: u64 =
            index_data.iter().map(|(id, _low_key)| *id).max().unwrap_or(0);

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

        let node_id_to_low_key_index: ConcurrentMap<
            u64,
            InlineArray,
            INDEX_FANOUT,
            EBR_LOCAL_GC_BUFFER_SIZE,
        > = index.iter().map(|(low_key, node)| (node.id.0, low_key)).collect();

        let ret = Db {
            global_error: Default::default(),
            high_level_rc: Arc::new(()),
            store,
            cache_advisor: RefCell::new(CacheAdvisor::new(
                self.cache_size,
                self.entry_cache_percent,
            )),
            index,
            config: self.clone(),
            node_id_to_low_key_index,
            dirty: Default::default(),
            flush_epoch: Default::default(),
        };

        if let Some(flush_every_ms) = ret.config.flush_every_ms {
            let bloodstone = ret.clone();
        }
        Ok(ret)
    }
}

pub struct Iter<
    'a,
    const INDEX_FANOUT: usize,
    const LEAF_FANOUT: usize,
    const EBR_LOCAL_GC_BUFFER_SIZE: usize,
> {
    inner: &'a Db<INDEX_FANOUT, LEAF_FANOUT, EBR_LOCAL_GC_BUFFER_SIZE>,
    bounds: (Bound<InlineArray>, Bound<InlineArray>),
    last: Option<InlineArray>,
    last_back: Option<InlineArray>,
}

impl<
        'a,
        const INDEX_FANOUT: usize,
        const LEAF_FANOUT: usize,
        const EBR_LOCAL_GC_BUFFER_SIZE: usize,
    > Iterator
    for Iter<'a, INDEX_FANOUT, LEAF_FANOUT, EBR_LOCAL_GC_BUFFER_SIZE>
{
    type Item = io::Result<(InlineArray, InlineArray)>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl<
        'a,
        const INDEX_FANOUT: usize,
        const LEAF_FANOUT: usize,
        const EBR_LOCAL_GC_BUFFER_SIZE: usize,
    > DoubleEndedIterator
    for Iter<'a, INDEX_FANOUT, LEAF_FANOUT, EBR_LOCAL_GC_BUFFER_SIZE>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
struct NodeId(u64);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Leaf<const LEAF_FANOUT: usize> {
    lo: InlineArray,
    hi: Option<InlineArray>,
    prefix_length: usize,
    data: StackMap<InlineArray, InlineArray, LEAF_FANOUT>,
    #[serde(skip)]
    dirty_flush_epoch: Option<NonZeroU64>,
    in_memory_size: usize,
}

impl<const LEAF_FANOUT: usize> Leaf<LEAF_FANOUT> {
    fn serialize(&self) -> Vec<u8> {
        // bincode -> crc_frame -> zstd
        let mut ret = vec![];

        let mut zstd_enc = zstd::stream::Encoder::new(&mut ret, 3).unwrap();

        // let mut crc_frame_enc = crc_frame::Encoder::new(&mut zstd_enc);
        // bincode::serialize_into(&mut crc_frame_enc, self).unwrap();

        bincode::serialize_into(&mut zstd_enc, self).unwrap();

        zstd_enc.finish();

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

    fn split_if_full(
        &mut self,
        new_epoch: NonZeroU64,
        allocator: &Marble,
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
            let split_key = data.first().unwrap().0.clone();

            let rhs_id = allocator.allocate_object_id();

            log::trace!(
                "split leaf {:?} at split key: {:?} into new node {}",
                self.lo,
                split_key,
                rhs_id
            );

            let rhs = Leaf {
                dirty_flush_epoch: Some(new_epoch),
                hi: self.hi.clone(),
                lo: split_key.clone(),
                prefix_length: 0,
                in_memory_size: self.hi.as_ref().map(|h| h.len()).unwrap_or(0)
                    + split_key.len()
                    + data
                        .iter()
                        .map(|(k, v)| k.len() + v.len())
                        .sum::<usize>(),
                data,
            };

            self.hi = Some(split_key.clone());
            self.in_memory_size = self.lo.len()
                + split_key.len()
                + self
                    .data
                    .iter()
                    .map(|(k, v)| k.len() + v.len())
                    .sum::<usize>();

            let rhs_node = Node {
                id: NodeId(rhs_id),
                inner: Arc::new(Some(Box::new(rhs)).into()),
            };

            return Some((split_key, rhs_node));
        }

        None
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Node<const LEAF_FANOUT: usize> {
    // used for access in Marble
    id: NodeId,
    #[serde(skip)]
    inner: Arc<RwLock<Option<Box<Leaf<LEAF_FANOUT>>>>>,
}

pub struct Flusher {
    shutdown_trigger: mpsc::Receiver<()>,
    shutdown_confirm: mpsc::Sender<()>,
}

#[must_use]
struct LeafReadGuard<
    'a,
    const INDEX_FANOUT: usize = 64,
    const LEAF_FANOUT: usize = 1024,
    const EBR_LOCAL_GC_BUFFER_SIZE: usize = 128,
> {
    leaf_read: ManuallyDrop<
        ArcRwLockReadGuard<RawRwLock, Option<Box<Leaf<LEAF_FANOUT>>>>,
    >,
    low_key: InlineArray,
    inner: &'a Db<INDEX_FANOUT, LEAF_FANOUT, EBR_LOCAL_GC_BUFFER_SIZE>,
    node_id: NodeId,
}

impl<
        'a,
        const INDEX_FANOUT: usize,
        const LEAF_FANOUT: usize,
        const EBR_LOCAL_GC_BUFFER_SIZE: usize,
    > Drop
    for LeafReadGuard<'a, INDEX_FANOUT, LEAF_FANOUT, EBR_LOCAL_GC_BUFFER_SIZE>
{
    fn drop(&mut self) {
        let size = self.leaf_read.as_ref().unwrap().in_memory_size;
        // we must drop our mutex before calling mark_access_and_evict
        unsafe {
            ManuallyDrop::drop(&mut self.leaf_read);
        }
        if let Err(e) = self.inner.mark_access_and_evict(self.node_id, size) {
            self.inner.set_error(&e);
            log::error!("io error while paging out dirty data: {:?}", e);
        }
    }
}

struct LeafWriteGuard<
    'a,
    const INDEX_FANOUT: usize = 64,
    const LEAF_FANOUT: usize = 1024,
    const EBR_LOCAL_GC_BUFFER_SIZE: usize = 128,
> {
    leaf_write: ManuallyDrop<
        ArcRwLockWriteGuard<RawRwLock, Option<Box<Leaf<LEAF_FANOUT>>>>,
    >,
    flush_epoch_guard: FlushEpochGuard<'a>,
    low_key: InlineArray,
    inner: &'a Db<INDEX_FANOUT, LEAF_FANOUT, EBR_LOCAL_GC_BUFFER_SIZE>,
    node_id: NodeId,
}

impl<
        'a,
        const INDEX_FANOUT: usize,
        const LEAF_FANOUT: usize,
        const EBR_LOCAL_GC_BUFFER_SIZE: usize,
    > LeafWriteGuard<'a, INDEX_FANOUT, LEAF_FANOUT, EBR_LOCAL_GC_BUFFER_SIZE>
{
    fn epoch(&self) -> NonZeroU64 {
        self.flush_epoch_guard.epoch()
    }
}

impl<
        'a,
        const INDEX_FANOUT: usize,
        const LEAF_FANOUT: usize,
        const EBR_LOCAL_GC_BUFFER_SIZE: usize,
    > Drop
    for LeafWriteGuard<'a, INDEX_FANOUT, LEAF_FANOUT, EBR_LOCAL_GC_BUFFER_SIZE>
{
    fn drop(&mut self) {
        let size = self.leaf_write.as_ref().unwrap().in_memory_size;
        // we must drop our mutex before calling mark_access_and_evict
        unsafe {
            ManuallyDrop::drop(&mut self.leaf_write);
        }
        if let Err(e) = self.inner.mark_access_and_evict(self.node_id, size) {
            self.inner.set_error(&e);
            log::error!("io error while paging out dirty data: {:?}", e);
        }
    }
}

fn set_error(
    global_error: &AtomicPtr<(io::ErrorKind, String)>,
    error: &io::Error,
) {
    let kind = error.kind();
    let reason = error.to_string();

    let boxed = Box::new((kind, reason));
    let ptr = Box::into_raw(boxed);

    if global_error
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

/// sled 1.0
#[derive(Debug, Clone)]
pub struct Db<
    const INDEX_FANOUT: usize = 64,
    const LEAF_FANOUT: usize = 1024,
    const EBR_LOCAL_GC_BUFFER_SIZE: usize = 128,
> {
    global_error: Arc<AtomicPtr<(io::ErrorKind, String)>>,
    config: Config,
    high_level_rc: Arc<()>,
    index: ConcurrentMap<
        InlineArray,
        Node<LEAF_FANOUT>,
        INDEX_FANOUT,
        EBR_LOCAL_GC_BUFFER_SIZE,
    >,
    node_id_to_low_key_index:
        ConcurrentMap<u64, InlineArray, INDEX_FANOUT, EBR_LOCAL_GC_BUFFER_SIZE>,
    store: Marble,
    cache_advisor: RefCell<CacheAdvisor>,
    flush_epoch: FlushEpoch,
    // the value here is for serialized bytes
    dirty: ConcurrentMap<(NonZeroU64, InlineArray), Option<Arc<Vec<u8>>>>,
}

impl<
        const INDEX_FANOUT: usize,
        const LEAF_FANOUT: usize,
        const EBR_LOCAL_GC_BUFFER_SIZE: usize,
    > Drop for Db<INDEX_FANOUT, LEAF_FANOUT, EBR_LOCAL_GC_BUFFER_SIZE>
{
    fn drop(&mut self) {
        if Arc::strong_count(&self.high_level_rc) != 1 {
            return;
        }
        if let Err(e) = self.flush() {
            eprintln!("failed to flush Db on Drop: {e:?}");
        }
    }
}

impl<
        const INDEX_FANOUT: usize,
        const LEAF_FANOUT: usize,
        const EBR_LOCAL_GC_BUFFER_SIZE: usize,
    > Db<INDEX_FANOUT, LEAF_FANOUT, EBR_LOCAL_GC_BUFFER_SIZE>
{
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
        set_error(&self.global_error, error);
    }

    pub fn storage_stats(&self) -> marble::Stats {
        self.store.stats()
    }

    fn leaf_for_key<'a>(
        &'a self,
        key: &[u8],
    ) -> io::Result<
        LeafReadGuard<'a, INDEX_FANOUT, LEAF_FANOUT, EBR_LOCAL_GC_BUFFER_SIZE>,
    > {
        let (low_key, read, node_id) = loop {
            let (low_key, node) = self.index.get_lte(key).unwrap();
            let mut read = node.inner.read_arc();

            if read.is_none() {
                drop(read);
                let (low_key, write, node_id) = self.page_in(key)?;
                read = ArcRwLockWriteGuard::downgrade(write);
            }

            let leaf = read.as_ref().unwrap();

            assert!(&*leaf.lo <= key.as_ref());
            if let Some(ref hi) = leaf.hi {
                if &**hi < key.as_ref() {
                    log::trace!("key overshoot on leaf_for_key");
                    continue;
                }
            }
            break (low_key, read, node.id);
        };

        Ok(LeafReadGuard {
            leaf_read: ManuallyDrop::new(read),
            inner: self,
            low_key,
            node_id,
        })
    }

    fn page_in(
        &self,
        key: &[u8],
    ) -> io::Result<(
        InlineArray,
        ArcRwLockWriteGuard<RawRwLock, Option<Box<Leaf<LEAF_FANOUT>>>>,
        NodeId,
    )> {
        loop {
            let (low_key, node) = self.index.get_lte(key).unwrap();
            let mut write = node.inner.write_arc();
            if write.is_none() {
                let leaf_bytes = self.store.read(node.id.0)?.unwrap();
                let leaf: Box<Leaf<LEAF_FANOUT>> =
                    Leaf::deserialize(&leaf_bytes).unwrap();
                *write = Some(leaf);
            }
            let leaf = write.as_mut().unwrap();

            assert!(&*leaf.lo <= key.as_ref());
            if let Some(ref hi) = leaf.hi {
                if &**hi < key.as_ref() {
                    let size = leaf.in_memory_size;
                    drop(write);
                    log::trace!("key overshoot in leaf_for_key_mut_inner");
                    self.mark_access_and_evict(node.id, size);

                    continue;
                }
            }
            return Ok((low_key, write, node.id));
        }
    }

    fn leaf_for_key_mut<'a>(
        &'a self,
        key: &[u8],
    ) -> io::Result<
        LeafWriteGuard<'a, INDEX_FANOUT, LEAF_FANOUT, EBR_LOCAL_GC_BUFFER_SIZE>,
    > {
        let (low_key, mut write, node_id) = self.page_in(key)?;

        let flush_epoch_guard = self.flush_epoch.check_in();

        let leaf = write.as_mut().unwrap();
        assert!(&*leaf.lo <= key.as_ref());
        if let Some(ref hi) = leaf.hi {
            assert!(&**hi > key.as_ref());
        }

        if let Some(old_flush_epoch) = leaf.dirty_flush_epoch {
            if old_flush_epoch != flush_epoch_guard.epoch() {
                assert_eq!(
                    old_flush_epoch.get() + 1,
                    flush_epoch_guard.epoch().get()
                );

                log::info!(
                    "cooperatively flushing {:?} with dirty epoch {} after checking into epoch {}",
                    node_id,
                    old_flush_epoch.get(),
                    flush_epoch_guard.epoch().get()
                );
                // cooperatively serialize and put into dirty
                let dirty_epoch = leaf.dirty_flush_epoch.take().unwrap();

                // be extra-explicit about serialized bytes
                let leaf_ref: &Leaf<LEAF_FANOUT> = &*leaf;

                let serialized = leaf_ref.serialize();

                self.dirty.insert(
                    (dirty_epoch, leaf.lo.clone()),
                    Some(Arc::new(serialized)),
                );
            }
        }

        Ok(LeafWriteGuard {
            flush_epoch_guard,
            leaf_write: ManuallyDrop::new(write),
            inner: self,
            low_key,
            node_id,
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
        let mut ca = self.cache_advisor.borrow_mut();
        let to_evict = ca.accessed_reuse_buffer(node_id.0, size);
        for (node_to_evict, rough_size) in to_evict {
            let low_key =
                self.node_id_to_low_key_index.get(&node_to_evict).unwrap();
            let node = self.index.get(&low_key).unwrap();
            let mut write = node.inner.write();
            if write.is_none() {
                // already paged out
                continue;
            }
            let leaf: &mut Leaf<LEAF_FANOUT> = write.as_mut().unwrap();
            if let Some(dirty_flush_epoch) = leaf.dirty_flush_epoch {
                let serialized = leaf.serialize();

                self.dirty.insert(
                    (dirty_flush_epoch, leaf.lo.clone()),
                    Some(Arc::new(serialized)),
                );
            }
            *write = None;
        }

        Ok(())
    }

    pub fn get<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> io::Result<Option<InlineArray>> {
        self.check_error()?;
        let leaf_guard = self.leaf_for_key(key.as_ref())?;

        let leaf = leaf_guard.leaf_read.as_ref().unwrap();

        if let Some(ref hi) = leaf.hi {
            assert!(&**hi > key.as_ref());
        }

        Ok(leaf.data.get(key.as_ref()).cloned())
    }

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

        let value_ivec = value.into();
        let mut leaf_guard = self.leaf_for_key_mut(key.as_ref())?;
        let new_epoch = leaf_guard.flush_epoch_guard.epoch();

        let leaf = &mut leaf_guard.leaf_write.as_mut().unwrap();

        // TODO handle prefix encoding

        let ret = leaf.data.insert(key.as_ref().into(), value_ivec.clone());

        let split = leaf.split_if_full(new_epoch, &self.store);
        if split.is_some() || Some(value_ivec) != ret {
            leaf.dirty_flush_epoch = Some(new_epoch);
            self.dirty.insert((new_epoch, leaf_guard.low_key.clone()), None);
        }
        if let Some((split_key, rhs_node)) = split {
            self.dirty.insert((new_epoch, split_key.clone()), None);
            self.node_id_to_low_key_index
                .insert(rhs_node.id.0, split_key.clone());
            self.index.insert(split_key, rhs_node);
        }

        Ok(ret)
    }

    #[doc(alias = "delete")]
    #[doc(alias = "del")]
    pub fn remove<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> io::Result<Option<InlineArray>> {
        self.check_error()?;

        let mut leaf_guard = self.leaf_for_key_mut(key.as_ref())?;
        let new_epoch = leaf_guard.flush_epoch_guard.epoch();

        let leaf = &mut leaf_guard.leaf_write.as_mut().unwrap();

        // TODO handle prefix encoding

        let ret = leaf.data.remove(key.as_ref());

        if ret.is_some() {
            leaf.dirty_flush_epoch = Some(new_epoch);
            self.dirty.insert((new_epoch, leaf_guard.low_key.clone()), None);
        }

        Ok(ret)
    }

    pub fn flush(&self) -> io::Result<()> {
        let mut write_batch = vec![];

        let (
            previous_flush_complete_notifier,
            previous_vacant_notifier,
            forward_flush_notifier,
        ) = self.flush_epoch.roll_epoch_forward();

        previous_flush_complete_notifier.wait_for_complete();
        let flush_through_epoch: NonZeroU64 =
            previous_vacant_notifier.wait_for_complete();

        let flush_boundary = (
            NonZeroU64::new(flush_through_epoch.get() + 1).unwrap(),
            InlineArray::default(),
        );
        for ((epoch, low_key), _) in self.dirty.range(..flush_boundary) {
            if let Some(node) = self.index.get(&*low_key) {
                let mut lock = node.inner.write();

                assert_eq!(
                    epoch,
                    flush_through_epoch,
                    "{:?} is dirty for old epoch {}",
                    node.id,
                    epoch.get()
                );

                let serialized_value_opt = self
                    .dirty
                    .remove(&(epoch, low_key.clone()))
                    .expect("violation of flush responsibility");

                let leaf_bytes: Vec<u8> =
                    if let Some(serialized_value) = serialized_value_opt {
                        if Arc::strong_count(&serialized_value) == 1 {
                            Arc::into_inner(serialized_value).unwrap()
                        } else {
                            serialized_value.to_vec()
                        }
                    } else {
                        let leaf_ref: &mut Leaf<LEAF_FANOUT> =
                            lock.as_mut().unwrap();
                        let dirty_epoch =
                            leaf_ref.dirty_flush_epoch.take().unwrap();
                        assert_eq!(epoch, dirty_epoch);
                        // ugly but basically free
                        leaf_ref.serialize()
                    };

                drop(lock);
                // println!("node id {} is dirty", node.id.0);
                write_batch.push((
                    node.id.0,
                    Some((marble::InlineArray::from(&*low_key), leaf_bytes)),
                ));
            } else {
                continue;
            };
        }

        let written_count = write_batch.len();
        self.store.write_batch(write_batch)?;
        log::info!(
            "marking epoch {} as flushed - {} objects written",
            flush_through_epoch.get(),
            written_count
        );
        forward_flush_notifier.mark_complete();
        self.store.maintenance()?;
        Ok(())
    }

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

        let mut leaf_guard = self.leaf_for_key_mut(key.as_ref())?;
        let new_epoch = leaf_guard.epoch();

        let proposed: Option<InlineArray> = new.map(Into::into);

        let leaf = &mut leaf_guard.leaf_write.as_mut().unwrap();

        // TODO handle prefix encoding

        let current = leaf.data.get(key.as_ref()).cloned();

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
            let previous = if let Some(ref new_value) = proposed {
                leaf.data.insert(key.as_ref().into(), new_value.clone())
            } else {
                leaf.data.remove(key.as_ref())
            };

            Ok(CompareAndSwapSuccess {
                new_value: proposed,
                previous_value: current,
            })
        } else {
            Err(CompareAndSwapError { current, proposed })
        };

        let split = leaf.split_if_full(new_epoch, &self.store);
        if split.is_some() || ret.is_ok() {
            leaf.dirty_flush_epoch = Some(new_epoch);
            self.dirty.insert((new_epoch, leaf_guard.low_key.clone()), None);
        }
        if let Some((split_key, rhs_node)) = split {
            self.dirty.insert((new_epoch, split_key.clone()), None);
            self.node_id_to_low_key_index
                .insert(rhs_node.id.0, split_key.clone());
            self.index.insert(split_key, rhs_node);
        }

        Ok(ret)
    }

    #[doc(hidden)]
    pub fn iter(
        &self,
    ) -> Iter<INDEX_FANOUT, LEAF_FANOUT, EBR_LOCAL_GC_BUFFER_SIZE> {
        Iter {
            inner: self,
            bounds: (Bound::Unbounded, Bound::Unbounded),
            last: None,
            last_back: None,
        }
    }

    #[doc(hidden)]
    pub fn range<K, R>(
        &self,
        range: R,
    ) -> Iter<INDEX_FANOUT, LEAF_FANOUT, EBR_LOCAL_GC_BUFFER_SIZE>
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
    {
        let start: Bound<InlineArray> =
            map_bound(range.start_bound(), |b| InlineArray::from(b.as_ref()));
        let end: Bound<InlineArray> =
            map_bound(range.start_bound(), |b| InlineArray::from(b.as_ref()));

        Iter { inner: self, bounds: (start, end), last: None, last_back: None }
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
    /// # let db = bloodstone::open_default("batch_doctest")?;
    /// db.insert("key_0", "val_0")?;
    ///
    /// let mut batch = bloodstone::Batch::default();
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
                NodeId,
            ),
        > = BTreeMap::new();

        // Phase 1: lock acquisition
        let mut last: Option<(
            InlineArray,
            ArcRwLockWriteGuard<RawRwLock, Option<Box<Leaf<LEAF_FANOUT>>>>,
            NodeId,
        )> = None;

        for (key, _value) in &batch.writes {
            if let Some((lo, w, id)) = &last {
                let leaf = w.as_ref().unwrap();
                assert!(&leaf.lo <= key);
                if let Some(hi) = &leaf.hi {
                    if hi <= &key {
                        let (lo, w, id) = last.take().unwrap();
                        acquired_locks.insert(lo, (w, id));
                    }
                }
            }
            if last.is_none() {
                last = Some(self.page_in(&key)?);
            }
        }

        if let Some((lo, w, id)) = last.take() {
            acquired_locks.insert(lo, (w, id));
        }

        // NB: add the flush epoch at the end of the lock acquisition
        // process when all locks have been acquired, to avoid situations
        // where a leaf is already dirty with an epoch "from the future".
        let flush_epoch_guard = self.flush_epoch.check_in();
        let new_epoch = flush_epoch_guard.epoch();

        // Flush any leaves that are dirty from a previous flush epoch
        // before performing operations.
        for (_, (write, node_id)) in &mut acquired_locks {
            let leaf = write.as_mut().unwrap();
            if let Some(old_flush_epoch) = leaf.dirty_flush_epoch {
                if old_flush_epoch != new_epoch {
                    assert_eq!(old_flush_epoch.get() + 1, new_epoch.get());

                    log::info!(
                    "cooperatively flushing {:?} with dirty epoch {} after checking into epoch {}",
                    node_id,
                    old_flush_epoch.get(),
                    new_epoch.get()
                );
                    // cooperatively serialize and put into dirty
                    let dirty_epoch = leaf.dirty_flush_epoch.take().unwrap();

                    // be extra-explicit about serialized bytes
                    let leaf_ref: &Leaf<LEAF_FANOUT> = &*leaf;

                    let serialized = leaf_ref.serialize();

                    self.dirty.insert(
                        (dirty_epoch, leaf.lo.clone()),
                        Some(Arc::new(serialized)),
                    );
                }
            }
        }

        let mut splits: Vec<(InlineArray, Node<LEAF_FANOUT>)> = vec![];

        // Insert and split when full
        for (key, value_opt) in batch.writes {
            let range = (..=&key);
            let (lo, (ref mut w, id)) = acquired_locks
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
                    leaf.split_if_full(new_epoch, &self.store)
                {
                    let node_id = rhs_node.id;
                    let mut write = rhs_node.inner.write_arc();
                    assert!(write.is_some());

                    splits.push((split_key.clone(), rhs_node));
                    acquired_locks.insert(split_key, (write, node_id));
                }
            } else {
                leaf.data.remove(&key);
            }
        }

        // Make splits globally visible
        for (split_key, rhs_node) in splits {
            self.node_id_to_low_key_index
                .insert(rhs_node.id.0, split_key.clone());
            self.index.insert(split_key, rhs_node);
        }

        // Drop locks
        drop(acquired_locks);

        Ok(())
    }
}

fn map_bound<T, U, F: FnOnce(T) -> U>(bound: Bound<T>, f: F) -> Bound<U> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(x) => Bound::Included(f(x)),
        Bound::Excluded(x) => Bound::Excluded(f(x)),
    }
}
