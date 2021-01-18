#![allow(unsafe_code)]

// TODO we can skip the first offset because it's always 0

use std::{
    alloc::{alloc_zeroed, dealloc, Layout},
    cmp::Ordering::{self, Equal, Greater, Less},
    convert::{TryFrom, TryInto},
    fmt,
    mem::{align_of, size_of},
    num::{NonZeroU16, NonZeroU64},
    ops::{Bound, Deref, DerefMut},
    sync::Arc,
};

use crate::{varint, IVec, Link};

const ALIGNMENT: usize = align_of::<Header>();

macro_rules! tf {
    ($e:expr) => {
        usize::try_from($e).unwrap()
    };
    ($e:expr, $t:ty) => {
        <$t>::try_from($e).unwrap()
    };
}

// allocates space for a header struct at the beginning.
fn uninitialized_node(len: usize) -> Inner {
    let layout = Layout::from_size_align(len, ALIGNMENT).unwrap();

    unsafe {
        let ptr = alloc_zeroed(layout);
        Inner { ptr, len }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Header {
    // NB always lay out fields from largest to smallest
    // to properly pack the struct
    pub next: Option<NonZeroU64>,
    // could probably be Option<u16> w/ child index
    // rather than the pid
    pub merging_child: Option<NonZeroU64>,
    // could be replaced by a varint, w/ data buf offset stored instead
    lo_len: u64,
    // could be replaced by a varint, w/ data buf offset stored instead
    hi_len: u64,
    fixed_key_length: Option<NonZeroU16>,
    fixed_value_length: Option<NonZeroU16>,
    pub children: u16,
    pub prefix_len: u8,
    fixed_key_stride: u8,
    probation_ops_remaining: u8,
    // this can be 3 bits. 111 = 7, but we
    // will never need 7 bytes for storing offsets.
    // address spaces cap out at 2 ** 48 (256 ** 6)
    // so as long as we can represent the numbers 1-6,
    // we can reach the full linux address space currently
    // supported as of 2021.
    offset_bytes: u8,
    // can be 2 bits
    pub rewrite_generations: u8,
    // this can really be 2 bits, representing
    // 00: all updates have been at the end
    // 01: mixed updates
    // 10: all updates have been at the beginning
    activity_sketch: u8,
    // can be 1 bit
    pub merging: bool,
    // can be 1 bit
    pub is_index: bool,
}

fn apply_computed_distance(mut buf: &mut [u8], mut distance: usize) {
    while distance > 0 {
        let last = &mut buf[buf.len() - 1];
        let distance_byte = u8::try_from(distance % 256).unwrap();
        let carry = if 255 - distance_byte < *last { 1 } else { 0 };
        *last = last.wrapping_add(distance_byte);
        distance = (distance >> 8) + carry;
        if distance != 0 {
            let new_len = buf.len() - 1;
            buf = &mut buf[..new_len];
        }
    }
}

fn stride(a: &[u8], b: KeyRef<'_>) -> u8 {
    if a.len() != b.len() {
        return 0;
    }
    match b {
        KeyRef::Slice(b) => b[b.len() - 1].wrapping_sub(a[b.len() - 1]),
        KeyRef::Computed { base, distance: b_distance } => {
            let a_distance = linear_distance(base, a);
            u8::try_from(b_distance - a_distance).unwrap()
        }
    }
}

// NB: caller must ensure that base and search are equal lengths
fn linear_distance(base: &[u8], search: &[u8]) -> usize {
    fn f1(base: &[u8], search: &[u8]) -> usize {
        (search[search.len() - 1] - base[search.len() - 1]) as usize
    }
    fn f2(base: &[u8], search: &[u8]) -> usize {
        (u16::from_be_bytes(search.try_into().unwrap()) as usize)
            - (u16::from_be_bytes(base.try_into().unwrap()) as usize)
    }
    fn f3(base: &[u8], search: &[u8]) -> usize {
        (u32::from_be_bytes([0, search[0], search[1], search[2]]) as usize)
            - (u32::from_be_bytes([0, base[0], base[1], base[2]]) as usize)
    }
    fn f4(base: &[u8], search: &[u8]) -> usize {
        (u32::from_be_bytes(search.try_into().unwrap()) as usize)
            - (u32::from_be_bytes(base.try_into().unwrap()) as usize)
    }

    assert!(base <= search);
    assert!(!base.is_empty());
    assert_eq!(search.len(), base.len());

    let computed_gotos = [f1, f2, f3, f4];

    if base.len() > 4 {
        if &base[..base.len() - 4] != &search[..search.len() - 4] {
            std::usize::MAX
        } else {
            computed_gotos[3](
                &base[base.len() - 4..],
                &search[search.len() - 4..],
            )
        }
    } else {
        computed_gotos[base.len() - 1](base, search)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum KeyRef<'a> {
    // used when all keys on a node are linear
    // with a fixed stride length, allowing us to
    // avoid ever actually storing any of them
    Computed { base: &'a [u8], distance: usize },
    // used when keys are not linear, and we
    // store the actual prefix-encoded keys on the node
    Slice(&'a [u8]),
}

impl<'a> From<KeyRef<'a>> for IVec {
    fn from(kr: KeyRef<'a>) -> IVec {
        match kr {
            KeyRef::Computed { base, distance } => {
                let mut ivec: IVec = base.into();
                apply_computed_distance(&mut ivec, distance);
                ivec
            }
            KeyRef::Slice(s) => s.into(),
        }
    }
}

impl<'a> From<&KeyRef<'a>> for IVec {
    fn from(kr: &KeyRef<'a>) -> IVec {
        match kr {
            KeyRef::Computed { base, distance } => {
                let mut ivec: IVec = (*base).into();
                apply_computed_distance(&mut ivec, *distance);
                ivec
            }
            KeyRef::Slice(s) => (*s).into(),
        }
    }
}

impl<'a> KeyRef<'a> {
    fn unwrap_slice(&self) -> &[u8] {
        if let KeyRef::Slice(s) = self {
            s
        } else {
            panic!("called KeyRef::unwrap_slice on a KeyRef::Computed");
        }
    }

    fn write_into(&self, buf: &mut [u8]) {
        match self {
            KeyRef::Computed { base, distance } => {
                buf.copy_from_slice(base);
                apply_computed_distance(buf, *distance);
            }
            KeyRef::Slice(s) => buf.copy_from_slice(s),
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn trim_prefix_bytes(self, additional_prefix: usize) -> KeyRef<'a> {
        match self {
            KeyRef::Computed { base, distance } => {
                assert!(
                    (base.len() - additional_prefix) as f64
                        > (distance as f64).log(256.)
                );
                KeyRef::Computed { base: &base[additional_prefix..], distance }
            }
            KeyRef::Slice(s) => KeyRef::Slice(&s[additional_prefix..]),
        }
    }

    fn distance(&self, other: &KeyRef<'_>) -> usize {
        match (self, other) {
            (
                KeyRef::Computed { base: a, distance: da },
                KeyRef::Computed { base: b, distance: db },
            ) => linear_distance(a, b) + (db - da),
            (KeyRef::Computed { base: a, distance: da }, KeyRef::Slice(b)) => {
                linear_distance(a, b) - da
            }
            (KeyRef::Slice(a), KeyRef::Computed { base: b, distance: db }) => {
                linear_distance(a, b) + db
            }
            (KeyRef::Slice(a), KeyRef::Slice(b)) => linear_distance(a, b),
        }
    }

    fn len(&self) -> usize {
        match self {
            KeyRef::Computed { base, .. } => base.len(),
            KeyRef::Slice(s) => s.len(),
        }
    }
}

impl PartialEq<KeyRef<'_>> for KeyRef<'_> {
    fn eq(&self, other: &KeyRef<'_>) -> bool {
        if self.len() != other.len() {
            return false;
        }
        self.cmp(other) == Equal
    }
}

impl Eq for KeyRef<'_> {}

impl Ord for KeyRef<'_> {
    fn cmp(&self, other: &KeyRef<'_>) -> Ordering {
        let min_len = self.len().min(other.len());
        match (self, other) {
            (
                KeyRef::Computed { base: a, distance: da },
                KeyRef::Computed { base: b, distance: db },
            ) => {
                let shared_a = &a[..min_len];
                let compensated_da = da >> ((a.len() - min_len) * 8);
                let shared_b = &b[..min_len];
                let compensated_db = db >> ((b.len() - min_len) * 8);
                match a.cmp(b) {
                    Less => {
                        match compensated_da.cmp(
                            &(linear_distance(shared_a, shared_b)
                                + compensated_db),
                        ) {
                            Equal => self.len().cmp(&other.len()),
                            other => other,
                        }
                    }
                    Greater => {
                        match (linear_distance(shared_b, shared_a)
                            + compensated_da)
                            .cmp(&compensated_db)
                        {
                            Equal => self.len().cmp(&other.len()),
                            other => other,
                        }
                    }
                    Equal => da.cmp(db),
                }
            }
            (KeyRef::Computed { .. }, KeyRef::Slice(b)) => {
                // recurse to first case
                self.cmp(&KeyRef::Computed { base: b, distance: 0 })
            }
            (KeyRef::Slice(a), KeyRef::Computed { .. }) => {
                // recurse to first case
                KeyRef::Computed { base: a, distance: 0 }.cmp(&other)
            }
            (KeyRef::Slice(a), KeyRef::Slice(b)) => a.cmp(b),
        }
    }
}

impl PartialOrd<KeyRef<'_>> for KeyRef<'_> {
    fn partial_cmp(&self, other: &KeyRef<'_>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialOrd<[u8]> for KeyRef<'_> {
    fn partial_cmp(&self, other: &[u8]) -> Option<Ordering> {
        self.partial_cmp(&KeyRef::Slice(other))
    }
}

impl PartialEq<[u8]> for KeyRef<'_> {
    fn eq(&self, other: &[u8]) -> bool {
        self.eq(&KeyRef::Slice(other))
    }
}

pub struct Iter<'a> {
    overlay: std::slice::Iter<'a, (IVec, Option<IVec>)>,
    node: &'a Inner,
    node_position: usize,
    next_a: Option<(&'a [u8], Option<&'a IVec>)>,
    next_b: Option<(KeyRef<'a>, &'a [u8])>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (KeyRef<'a>, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.next_a.is_none() {
                log::trace!("src/node.rs:94");
                if let Some((k, v)) = self.overlay.next() {
                    log::trace!("next_a is now ({:?}, {:?})", k, v);
                    self.next_a = Some((k.as_ref(), v.as_ref()));
                }
            }
            if self.next_b.is_none()
                && self.node.children() > self.node_position
            {
                self.next_b = Some((
                    self.node.index_key(self.node_position),
                    self.node.index_value(self.node_position),
                ));
                log::trace!("next_b is now {:?}", self.next_b);
                self.node_position += 1;
            }
            match (self.next_a, self.next_b) {
                (None, _) => {
                    log::trace!("src/node.rs:112");
                    log::trace!("iterator returning {:?}", self.next_b);
                    return self.next_b.take();
                }
                (Some((_, None)), None) => {
                    log::trace!("src/node.rs:113");
                    self.next_a.take();
                }
                (Some((_, Some(_))), None) => {
                    log::trace!("src/node.rs:114");
                    log::trace!("iterator returning {:?}", self.next_a);
                    return self.next_a.take().map(|(k, v)| {
                        (KeyRef::Slice(&*k), v.unwrap().as_ref())
                    });
                }
                (Some((k_a, Some(_))), Some((k_b, _))) if k_b < *k_a => {
                    log::trace!("src/node.rs:120");
                    log::trace!("iterator returning {:?}", self.next_b);
                    return self.next_b.take();
                }
                (Some((k_a, Some(_))), Some((k_b, _))) if k_b > *k_a => {
                    log::trace!("iterator returning {:?}", self.next_a);
                    return self.next_a.take().map(|(k, v)| {
                        (KeyRef::Slice(&*k), v.unwrap().as_ref())
                    });
                }
                (Some((k_a, Some(_))), Some((k_b, _))) if k_b == *k_a => {
                    // prefer overlay, discard node value
                    self.next_b.take();
                    log::trace!("src/node.rs:133");
                    log::trace!("iterator returning {:?}", self.next_a);
                    return self.next_a.take().map(|(k, v)| {
                        (KeyRef::Slice(&*k), v.unwrap().as_ref())
                    });
                }
                (Some((k_a, None)), Some((k_b, _))) if k_b == *k_a => {
                    // skip tombstone and continue the loop
                    log::trace!("src/node.rs:141");
                    self.next_a.take();
                    self.next_b.take();
                }
                (Some((k_a, None)), Some((k_b, _))) if k_b < *k_a => {
                    log::trace!("src/node.rs:146");
                    // we do not clear a tombstone until we move past
                    // it in the underlying node
                    log::trace!("iterator returning {:?}", self.next_b);
                    return self.next_b.take();
                }
                (Some((k_a, None)), Some((k_b, _))) if k_b > *k_a => {
                    log::trace!("src/node.rs:151");
                    self.next_a.take();
                }
                _ => unreachable!(
                    "did not expect combination a: {:?} b: {:?}",
                    self.next_a, self.next_b
                ),
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Node {
    // the overlay accumulates new writes and tombstones
    // for deletions that have not yet been merged
    // into the inner backing node
    pub(crate) overlay: Vec<(IVec, Option<IVec>)>,
    inner: Arc<Inner>,
}

impl Clone for Node {
    fn clone(&self) -> Node {
        Node { inner: self.merge_overlay(), overlay: Default::default() }
    }
}

impl Deref for Node {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        &self.inner
    }
}

impl Node {
    fn iter(&self) -> Iter<'_> {
        Iter {
            overlay: self.overlay.iter(),
            node: &self.inner,
            node_position: 0,
            next_a: None,
            next_b: None,
        }
    }

    pub(crate) fn iter_index_pids(&self) -> impl '_ + Iterator<Item = u64> {
        log::trace!("iter_index_pids on node {:?}", self);
        self.iter().map(|(_, v)| u64::from_le_bytes(v.try_into().unwrap()))
    }

    pub(crate) unsafe fn from_raw(buf: &[u8]) -> Node {
        Node {
            overlay: Default::default(),
            inner: Arc::new(Inner::from_raw(buf)),
        }
    }

    pub(crate) fn new_root(child_pid: u64) -> Node {
        Node { overlay: vec![], inner: Arc::new(Inner::new_root(child_pid)) }
    }

    pub(crate) fn new_hoisted_root(left: u64, at: &[u8], right: u64) -> Node {
        Node {
            overlay: vec![],
            inner: Arc::new(Inner::new_hoisted_root(left, at, right)),
        }
    }

    pub(crate) fn new_empty_leaf() -> Node {
        Node { overlay: vec![], inner: Arc::new(Inner::new_empty_leaf()) }
    }

    pub(crate) fn apply(&self, link: &Link) -> Node {
        use self::Link::*;

        assert!(
            !self.inner.merging,
            "somehow a link was applied to a node after it was merged"
        );

        match *link {
            Set(ref k, ref v) => self.insert(k, v),
            Del(ref key) => self.remove(key),
            ParentMergeConfirm => {
                assert!(self.merging_child.is_some());
                let merged_child = self
                    .merging_child
                    .expect(
                        "we should have a specific \
                         child that was merged if this \
                         link appears here",
                    )
                    .get();
                let idx = self
                    .iter_index_pids()
                    .position(|pid| pid == merged_child)
                    .unwrap();
                let mut ret =
                    self.remove(&self.index_key(idx).into()).merge_overlay();
                Arc::get_mut(&mut ret).unwrap().merging_child = None;
                Node { inner: ret, overlay: Default::default() }
            }
            ParentMergeIntention(pid) => {
                assert!(
                    self.can_merge_child(pid),
                    "trying to merge {:?} into node {:?} which \
                     is not a valid merge target",
                    link,
                    self
                );
                let mut ret = self.merge_overlay();
                Arc::make_mut(&mut ret).merging_child =
                    Some(NonZeroU64::new(pid).unwrap());
                Node { inner: ret, overlay: vec![] }
            }
            ChildMergeCap => {
                let mut ret = self.merge_overlay();
                Arc::make_mut(&mut ret).merging = true;
                Node { inner: ret, overlay: vec![] }
            }
        }
    }

    fn insert(&self, key: &IVec, value: &IVec) -> Node {
        let search = self.overlay.binary_search_by_key(&key, |(k, _)| k);
        let overlay = match search {
            Ok(idx) => {
                let mut overlay = self.overlay.clone();
                overlay[idx].1 = Some(value.clone());
                overlay
            }
            Err(idx) => {
                let mut overlay = Vec::with_capacity(self.overlay.len() + 1);
                overlay.extend_from_slice(&self.overlay);
                overlay.insert(idx, (key.clone(), Some(value.clone())));
                overlay
            }
        };
        Node { overlay, inner: self.inner.clone() }
    }

    fn remove(&self, key: &IVec) -> Node {
        let mut overlay = self.overlay.clone();
        let search = overlay.binary_search_by_key(&key, |(k, _)| k);
        match search {
            Ok(idx) => overlay[idx].1 = None,
            Err(idx) => overlay.insert(idx, (key.clone(), None)),
        }
        let ret = Node { overlay, inner: self.inner.clone() };
        log::trace!(
            "applying removal of key {:?} results in node {:?}",
            key,
            ret
        );
        ret
    }

    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        self.overlay.binary_search_by_key(&key, |(k, _)| k).is_ok()
            || self.inner.contains_key(key)
    }

    // Push the overlay into the backing node.
    fn merge_overlay(&self) -> Arc<Inner> {
        if self.overlay.is_empty() {
            return self.inner.clone();
        };
        let mut items =
            Vec::with_capacity(self.inner.children() + self.overlay.len());

        for (k, v) in self.iter() {
            items.push((k, v))
        }

        log::trace!(
            "merging overlay items for node {:?} into {:?}",
            self,
            items
        );

        let mut ret = Inner::new(
            self.lo(),
            self.hi(),
            self.prefix_len,
            self.is_index,
            self.next,
            &items,
        );

        ret.merging = self.merging;
        ret.merging_child = self.merging_child;

        log::trace!("merged node {:?} into {:?}", self, ret);
        Arc::new(ret)
    }

    pub(crate) fn set_next(&mut self, next: Option<NonZeroU64>) {
        Arc::get_mut(&mut self.inner).unwrap().next = next;
    }

    pub(crate) fn increment_rewrite_generations(&mut self) {
        let rewrite_generations = self.rewrite_generations;
        Arc::make_mut(&mut self.inner).rewrite_generations =
            rewrite_generations.saturating_add(1);
    }

    pub(crate) fn receive_merge(&self, other: &Node) -> Node {
        log::trace!("receiving merge, left: {:?} right: {:?}", self, other);
        let left = self.merge_overlay();
        let right = other.merge_overlay();
        log::trace!(
            "overlays should now be merged: left: {:?} right: {:?}",
            left,
            right
        );

        let ret = Node {
            overlay: Default::default(),
            inner: Arc::new(left.receive_merge(&right)),
        };

        log::trace!("merge created node {:?}", ret);
        ret
    }
    pub(crate) fn split(&self) -> (Node, Node) {
        let (lhs_inner, rhs_inner) = self.merge_overlay().split();
        let lhs =
            Node { inner: Arc::new(lhs_inner), overlay: Default::default() };
        let rhs =
            Node { inner: Arc::new(rhs_inner), overlay: Default::default() };

        (lhs, rhs)
    }

    pub(crate) fn parent_split(&self, at: &[u8], to: u64) -> Option<Node> {
        let encoded_sep = &at[self.prefix_len as usize..];
        if self.contains_key(encoded_sep) {
            log::debug!(
                "parent_split skipped because \
                parent already contains child with key {:?} \
                at split point due to deep race",
                at
            );
            return None;
        }

        let mut overlay = self.overlay.clone();

        let value = Some(to.to_le_bytes().as_ref().into());

        let search = overlay.binary_search_by_key(&encoded_sep, |(k, _)| k);
        match search {
            Ok(idx) => overlay[idx].1 = value,
            Err(idx) => overlay.insert(idx, (encoded_sep.into(), value)),
        }

        let new_inner =
            Node { overlay, inner: self.inner.clone() }.merge_overlay();

        Some(Node { overlay: Default::default(), inner: new_inner })
    }

    pub(crate) fn node_kv_pair<'a>(
        &'a self,
        key: &'a [u8],
    ) -> (KeyRef<'_>, Option<&'a [u8]>) {
        let encoded_key = self.prefix_encode(key);
        if let Ok(idx) =
            self.overlay.binary_search_by_key(&encoded_key, |(k, _)| k)
        {
            let v = self.overlay[idx].1.as_ref();
            (KeyRef::Slice(encoded_key), v.map(AsRef::as_ref))
        } else {
            self.inner.node_kv_pair(key)
        }
    }

    pub(crate) fn successor(
        &self,
        bound: &Bound<IVec>,
    ) -> Option<(IVec, IVec)> {
        let (overlay, node_position) = match bound {
            Bound::Unbounded => (self.overlay.iter(), 0),
            Bound::Included(b) => {
                let overlay_search =
                    self.overlay.binary_search_by_key(&b, |(k, _)| k);
                let overlay = match overlay_search {
                    Ok(idx) => {
                        if let (k, Some(v)) = &self.overlay[idx] {
                            // short circuit return
                            return Some((k.clone(), v.clone()));
                        }
                        self.overlay[idx + 1..].iter()
                    }
                    Err(idx) => self.overlay[idx..].iter(),
                };

                let inner_search = self.find(b);
                let node_position = match inner_search {
                    Ok(idx) => {
                        return Some((
                            self.inner.index_key(idx).into(),
                            self.inner.index_value(idx).into(),
                        ))
                    }
                    Err(idx) => idx,
                };

                (overlay, node_position)
            }
            Bound::Excluded(b) => {
                let overlay_search =
                    self.overlay.binary_search_by_key(&b, |(k, _)| k);
                let overlay = match overlay_search {
                    Ok(idx) => self.overlay[idx + 1..].iter(),
                    Err(idx) => self.overlay[idx..].iter(),
                };

                let inner_search = self.find(b);
                let node_position = match inner_search {
                    Ok(idx) => {
                        // short circuit return
                        idx + 1
                    }
                    Err(idx) => idx,
                };

                (overlay, node_position)
            }
        };

        let in_bounds = |k: &KeyRef<'_>| match bound {
            Bound::Unbounded => true,
            Bound::Included(b) => *k >= b[self.prefix_len as usize..],
            Bound::Excluded(b) => *k > b[self.prefix_len as usize..],
        };

        let mut iter = Iter {
            overlay,
            node: &self.inner,
            node_position,
            next_a: None,
            next_b: None,
        };

        let ret: Option<(KeyRef<'_>, &[u8])> = iter.find(|(k, _)| in_bounds(k));

        ret.map(|(k, v)| (self.prefix_decode(k), v.into()))
    }

    pub(crate) fn predecessor(
        &self,
        bound: &Bound<IVec>,
    ) -> Option<(IVec, IVec)> {
        let in_bounds = |k: &KeyRef<'_>| match bound {
            Bound::Unbounded => true,
            Bound::Included(b) => *k <= b[self.prefix_len as usize..],
            Bound::Excluded(b) => *k < b[self.prefix_len as usize..],
        };

        let ret: Option<(KeyRef<'_>, &[u8])> = self
            .iter()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .find(|(k, _)| in_bounds(k));

        ret.map(|(k, v)| (self.prefix_decode(k), v.into()))
    }

    pub(crate) fn index_next_node(&self, key: &[u8]) -> (bool, u64) {
        log::debug!("index_next_node for key {:?} on node {:?}", key, self);
        assert!(key >= self.lo());
        if let Some(hi) = self.hi() {
            assert!(hi > key);
        }

        let encoded_key = self.prefix_encode(key);

        let idx = match self.find(encoded_key) {
            Ok(idx) => idx,
            Err(idx) => idx.max(1) - 1,
        };

        let is_leftmost = idx == 0;
        let pid_bytes = self.index_value(idx);
        let pid = u64::from_le_bytes(pid_bytes.try_into().unwrap());

        (is_leftmost, pid)
    }

    pub(crate) fn should_split(&self) -> bool {
        log::trace!("seeing if we should split node {:?}", self);
        let size_check = if cfg!(any(test, feature = "lock_free_delays")) {
            self.iter().take(4).count() > 4
        } else if self.is_index {
            self.len > 1024 && self.rss() > 1
        } else {
            /*
            let threshold = match self.rewrite_generations {
                0 => 24 * 1024,
                1 => {
                    64 * 1024
                }
                other => {
                    128 * 1024
                }
            };
            */
            let threshold = 4 * 1024 - crate::MAX_MSG_HEADER_LEN;
            self.len > threshold && self.iter().next().is_some()
        };

        let safety_checks = self.merging_child.is_none() && !self.merging;

        if size_check {
            log::trace!(
                "should_split: {} is index: {} children: {} size: {}",
                safety_checks && size_check,
                self.is_index,
                self.children,
                self.rss()
            );
        }

        safety_checks && size_check
    }

    pub(crate) fn should_merge(&self) -> bool {
        let size_check = if cfg!(any(test, feature = "lock_free_delays")) {
            self.iter().take(2).count() < 2
        /*
        } else if self.is_index {
            self.len < 4 * 1024
        */
        } else {
            /*
            let threshold = match self.rewrite_generations {
                0 => 10 * 1024,
                1 => 30 * 1024,
                other => {
                    64 * 1024
                }
            };
            */
            let threshold = 256 - crate::MAX_MSG_HEADER_LEN;
            self.len < threshold
        };

        let safety_checks = self.merging_child.is_none()
            && !self.merging
            && self.probation_ops_remaining == 0;

        safety_checks && size_check
    }
}

/// An immutable sorted string table
#[must_use]
pub struct Inner {
    ptr: *mut u8,
    pub len: usize,
}

impl PartialEq<Inner> for Inner {
    fn eq(&self, other: &Inner) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl Clone for Inner {
    fn clone(&self) -> Inner {
        unsafe { Inner::from_raw(self.as_ref()) }
    }
}

unsafe impl Sync for Inner {}
unsafe impl Send for Inner {}

impl Drop for Inner {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.len, ALIGNMENT).unwrap();
        unsafe {
            dealloc(self.ptr, layout);
        }
    }
}

impl AsRef<[u8]> for Inner {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl AsMut<[u8]> for Inner {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Deref for Inner {
    type Target = Header;

    fn deref(&self) -> &Header {
        self.header()
    }
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut ds = f.debug_struct("Inner");

        ds.field("header", self.header())
            .field("lo", &self.lo())
            .field("hi", &self.hi());

        if self.is_index {
            ds.field(
                "items",
                &self
                    .iter_keys()
                    .zip(self.iter_index_pids())
                    .collect::<Vec<_>>(),
            )
            .finish()
        } else {
            ds.field("items", &self.iter().collect::<Vec<_>>()).finish()
        }
    }
}

impl DerefMut for Inner {
    fn deref_mut(&mut self) -> &mut Header {
        self.header_mut()
    }
}

impl Inner {
    unsafe fn from_raw(buf: &[u8]) -> Inner {
        let mut ret = uninitialized_node(buf.len());
        ret.as_mut().copy_from_slice(buf);
        ret
    }

    fn new(
        lo: &[u8],
        hi: Option<&[u8]>,
        prefix_len: u8,
        is_index: bool,
        next: Option<NonZeroU64>,
        items: &[(KeyRef<'_>, &[u8])],
    ) -> Inner {
        assert!(items.len() <= std::u16::MAX as usize);

        // determine if we need to use varints and offset
        // indirection tables, or if everything is equal
        // size we can skip this. If all keys are linear
        // with a fixed stride, we can completely skip writing
        // them at all, as they can always be calculated by
        // adding the desired offset to the lo key.
        let mut key_lengths = Vec::with_capacity(items.len());
        let mut value_lengths = Vec::with_capacity(items.len());

        let mut initial_keys_equal_length = true;
        let mut initial_values_equal_length = true;
        let mut linear = items.len() > 1;
        let fixed_key_stride =
            if linear && lo[prefix_len as usize..].len() == items[1].0.len() {
                stride(&lo[prefix_len as usize..], items[1].0)
            } else {
                0
            };

        // determines if the item can be losslessly
        // constructed from the base by adding a fixed
        // stride to it.
        fn is_linear(a: &KeyRef<'_>, b: &KeyRef<'_>, stride: u8) -> bool {
            if a.len() != b.len() || a.len() > 4 {
                return false;
            }

            a.distance(b) == stride as usize
        }

        let mut prev: Option<&KeyRef<'_>> = None;
        for (k, v) in items {
            key_lengths.push(k.len() as u64);
            if let Some(first_sz) = key_lengths.first() {
                initial_keys_equal_length &= *first_sz == k.len() as u64;
                linear &= initial_keys_equal_length;
                if initial_keys_equal_length {
                    linear = linear
                        && if let Some(ref mut prev) = prev {
                            is_linear(prev, k, fixed_key_stride)
                        } else {
                            true
                        };

                    prev = Some(k);
                }
            }
            value_lengths.push(v.len() as u64);
            if let Some(first_sz) = value_lengths.first() {
                if is_index {
                    assert_eq!(*first_sz, size_of::<u64>() as u64);
                }
                initial_values_equal_length &= *first_sz == v.len() as u64;
            }
        }

        let (fixed_key_length, keys_equal_length) = if initial_keys_equal_length
        {
            if let Some(key_length) = key_lengths.first() {
                if *key_length > 0 && *key_length <= u64::from(std::u16::MAX) {
                    (
                        Some(
                            NonZeroU16::new(
                                u16::try_from(*key_length).unwrap(),
                            )
                            .unwrap(),
                        ),
                        true,
                    )
                } else {
                    (None, false)
                }
            } else {
                (None, false)
            }
        } else {
            (None, false)
        };

        linear &= fixed_key_length.is_some();
        let fixed_key_stride = if linear { fixed_key_stride } else { 0 };

        let (fixed_value_length, values_equal_length) =
            if initial_values_equal_length {
                if let Some(value_length) = value_lengths.first() {
                    if *value_length > 0
                        && *value_length <= u64::from(std::u16::MAX)
                    {
                        (
                            Some(
                                NonZeroU16::new(
                                    u16::try_from(*value_length).unwrap(),
                                )
                                .unwrap(),
                            ),
                            true,
                        )
                    } else {
                        (None, false)
                    }
                } else {
                    (None, false)
                }
            } else {
                (None, false)
            };

        let key_storage_size = if let Some(key_length) = fixed_key_length {
            if linear {
                // all keys can be directly computed from the node lo key
                // by adding a fixed stride length to the node lo key
                0
            } else {
                u64::from(key_length.get()) * (items.len() as u64)
            }
        } else {
            let mut sum = 0;
            for key_length in &key_lengths {
                sum += key_length;
                sum += varint::size(*key_length) as u64;
            }
            sum
        };

        // we max the value size with the size of a u64 because
        // when we retrieve offset sizes, we may actually read
        // over the end of the offset array and into the keys
        // and values data, and for nodes that only store tiny
        // items, it's possible that this would extend beyond the
        // allocation. This is why we always make the value buffer
        // 8 bytes or more, so any overlap from the offset array
        // does not extend beyond the allocation.
        let value_storage_size = if let Some(value_length) = fixed_value_length
        {
            u64::from(value_length.get()) * (items.len() as u64)
        } else {
            let mut sum = 0;
            for value_length in &value_lengths {
                sum += value_length;
                sum += varint::size(*value_length) as u64;
            }
            sum
        }
        .max(size_of::<u64>() as u64);

        let (offsets_storage_size, offset_bytes) = if keys_equal_length
            && values_equal_length
        {
            (0, 0)
        } else {
            let max_indexable_offset =
                if keys_equal_length { 0 } else { key_storage_size }
                    + if values_equal_length { 0 } else { value_storage_size };

            let bytes_per_offset: u8 = match max_indexable_offset {
                i if i < 256 => 1,
                i if i < (1 << 16) => 2,
                i if i < (1 << 24) => 3,
                i if i < (1 << 32) => 4,
                i if i < (1 << 40) => 5,
                i if i < (1 << 48) => 6,
                _ => unreachable!(),
            };

            (tf!(bytes_per_offset, u64) * items.len() as u64, bytes_per_offset)
        };

        let total_node_storage_size = size_of::<Header>() as u64
            + hi.map(|hi| hi.len() as u64).unwrap_or(0)
            + lo.len() as u64
            + key_storage_size
            + value_storage_size
            + offsets_storage_size;

        let mut ret = uninitialized_node(tf!(total_node_storage_size));

        *ret.header_mut() = Header {
            rewrite_generations: 0,
            activity_sketch: 0,
            probation_ops_remaining: 0,
            merging_child: None,
            merging: false,
            lo_len: lo.len() as u64,
            hi_len: hi.map(|hi| hi.len() as u64).unwrap_or(0),
            fixed_key_length,
            fixed_value_length,
            fixed_key_stride,
            offset_bytes,
            children: tf!(items.len(), u16),
            prefix_len,
            next,
            is_index,
        };

        ret.lo_mut().copy_from_slice(lo);

        if let Some(ref mut hi_buf) = ret.hi_mut() {
            hi_buf.copy_from_slice(hi.unwrap());
        }

        // we use either 0 or 1 offset tables.
        // - if keys and values are all equal lengths, no offset table is
        //   required
        // - if keys are equal length but values are not, we put an offset table
        //   at the beginning of the data buffer, then put each of the keys
        //   packed together, then varint-prefixed values which are addressed by
        //   the offset table
        // - if keys and values are both different lengths, we put an offset
        //   table at the beginning of the data buffer, then varint-prefixed
        //   keys followed inline with varint-prefixed values.
        //
        // So, there are 4 possible layouts:
        // 1. [fixed size keys] [fixed size values]
        //  - signified by fixed_key_length and fixed_value_length being Some
        // 2. [offsets] [fixed size keys] [variable values]
        //  - fixed_key_length: Some, fixed_value_length: None
        // 3. [offsets] [variable keys] [fixed-length values]
        //  - fixed_key_length: None, fixed_value_length: Some
        // 4. [offsets] [variable keys followed by variable values]
        //  - fixed_key_length: None, fixed_value_length: None
        let mut offset = 0_u64;
        for (idx, (k, v)) in items.iter().enumerate() {
            if !keys_equal_length || !values_equal_length {
                ret.set_offset(idx, tf!(offset));
            }
            if !keys_equal_length {
                offset += varint::size(k.len() as u64) as u64 + k.len() as u64;
            }
            if !values_equal_length {
                offset += varint::size(v.len() as u64) as u64 + v.len() as u64;
            }

            if !linear {
                // we completely skip writing any key data at all
                // when the keys are linear, as they can be
                // computed losslessly by multiplying the desired
                // index by the fixed stride length.
                let mut key_buf = ret.key_buf_for_offset_mut(idx);
                if !keys_equal_length {
                    let varint_bytes =
                        varint::serialize_into(k.len() as u64, key_buf);
                    key_buf = &mut key_buf[varint_bytes..];
                }
                k.write_into(&mut key_buf[..k.len()]);
            }

            let mut value_buf = ret.value_buf_for_offset_mut(idx);
            if !values_equal_length {
                let varint_bytes =
                    varint::serialize_into(v.len() as u64, value_buf);
                value_buf = &mut value_buf[varint_bytes..];
            }
            value_buf[..v.len()].copy_from_slice(v);
        }

        if ret.is_index {
            assert!(!ret.is_empty())
        }

        if ret.fixed_key_stride > 0 {
            assert!(
                ret.fixed_key_length.is_some(),
                "fixed_key_stride is {} but fixed_key_length \
                is None for generated node {:?}",
                ret.fixed_key_stride,
                ret
            );
        }

        testing_assert!(
            ret.is_sorted(),
            "created new node is not sorted: {:?}, had items passed in: {:?} linear: {}",
            ret,
            items,
            linear
        );

        ret
    }

    fn new_root(child_pid: u64) -> Inner {
        Inner::new(
            &[],
            None,
            0,
            true,
            None,
            &[(KeyRef::Slice(prefix::empty()), &child_pid.to_le_bytes())],
        )
    }

    fn new_hoisted_root(left: u64, at: &[u8], right: u64) -> Inner {
        Inner::new(
            &[],
            None,
            0,
            true,
            None,
            &[
                (KeyRef::Slice(prefix::empty()), &left.to_le_bytes()),
                (KeyRef::Slice(at), &right.to_le_bytes()),
            ],
        )
    }

    fn new_empty_leaf() -> Inner {
        Inner::new(&[], None, 0, false, None, &[])
    }

    // returns the OPEN ENDED buffer where a key may be placed
    fn key_buf_for_offset_mut(&mut self, index: usize) -> &mut [u8] {
        let offset_sz = self.children as usize * self.offset_bytes as usize;
        match (self.fixed_key_length, self.fixed_value_length) {
            (Some(k_sz), Some(_)) | (Some(k_sz), None) => {
                let keys_buf = &mut self.data_buf_mut()[offset_sz..];
                &mut keys_buf[index * tf!(k_sz.get())..]
            }
            (None, Some(_)) | (None, None) => {
                // find offset for key or combined kv offset
                let offset = self.offset(index);
                let keys_buf = &mut self.data_buf_mut()[offset_sz..];
                &mut keys_buf[offset..]
            }
        }
    }

    // returns the OPEN ENDED buffer where a value may be placed
    //
    // NB: it's important that this is only ever called after setting
    // the key and its varint length prefix, as this needs to be parsed
    // for case 4.
    fn value_buf_for_offset_mut(&mut self, index: usize) -> &mut [u8] {
        match (self.fixed_key_length, self.fixed_value_length) {
            (Some(_), Some(v_sz)) | (None, Some(v_sz)) => {
                let values_buf = self.values_buf_mut();
                &mut values_buf[index * tf!(v_sz.get())..]
            }
            (Some(_), None) => {
                // find combined kv offset
                let offset = self.offset(index);
                let values_buf = self.values_buf_mut();
                &mut values_buf[offset..]
            }
            (None, None) => {
                // find combined kv offset, skip key bytes
                let offset = self.offset(index);
                let values_buf = self.values_buf_mut();
                let slot_buf = &mut values_buf[offset..];
                let (val_len, varint_sz) =
                    varint::deserialize(slot_buf).unwrap();
                &mut slot_buf[tf!(val_len) + varint_sz..]
            }
        }
    }

    // returns the OPEN ENDED buffer where a value may be read
    //
    // NB: it's important that this is only ever called after setting
    // the key and its varint length prefix, as this needs to be parsed
    // for case 4.
    fn value_buf_for_offset(&self, index: usize) -> &[u8] {
        match (self.fixed_key_length, self.fixed_value_length) {
            (Some(_), Some(v_sz)) | (None, Some(v_sz)) => {
                let values_buf = self.values_buf();
                &values_buf[index * tf!(v_sz.get())..]
            }
            (Some(_), None) => {
                // find combined kv offset
                let offset = self.offset(index);
                let values_buf = self.values_buf();
                &values_buf[offset..]
            }
            (None, None) => {
                // find combined kv offset, skip key bytes
                let offset = self.offset(index);
                let values_buf = self.values_buf();
                let slot_buf = &values_buf[offset..];
                let (val_len, varint_sz) =
                    varint::deserialize(slot_buf).unwrap();
                &slot_buf[tf!(val_len) + varint_sz..]
            }
        }
    }

    #[inline]
    fn offset(&self, index: usize) -> usize {
        assert!(index < self.children as usize);
        assert!(self.offset_bytes > 0);
        let offsets_buf_start =
            tf!(self.lo_len) + tf!(self.hi_len) + size_of::<Header>();

        let start = offsets_buf_start + (index * self.offset_bytes as usize);

        let mask = std::usize::MAX
            >> (8
                * (tf!(size_of::<usize>(), u32)
                    - u32::from(self.offset_bytes)));

        let mut tmp = std::mem::MaybeUninit::<usize>::uninit();
        let len = size_of::<usize>();

        // we use unsafe code here because it cuts a significant number of
        // CPU cycles on a simple insertion workload compared to using the
        // more idiomatic approach of copying the correct number of bytes into
        // a buffer initialized with zeroes. the seemingly "less" unsafe
        // approach of using ptr::copy_nonoverlapping did not improve matters.
        // using a match statement on offest_bytes and performing simpler
        // casting for one or two bytes slowed things down due to increasing
        // code size. this approach is branch-free and cut CPU usage of this
        // function from 7-11% down to 0.5-2% in a monotonic insertion workload.
        #[allow(unsafe_code)]
        unsafe {
            let ptr: *const u8 = self.ptr.add(start);
            std::ptr::copy_nonoverlapping(
                ptr,
                tmp.as_mut_ptr() as *mut u8,
                len,
            );
            tmp.assume_init() & mask
        }
    }

    fn set_offset(&mut self, index: usize, offset: usize) {
        let offset_bytes = self.offset_bytes as usize;
        let buf = {
            let start = index * self.offset_bytes as usize;
            let end = start + offset_bytes;
            &mut self.data_buf_mut()[start..end]
        };
        let bytes = &offset.to_le_bytes()[..offset_bytes];
        buf.copy_from_slice(bytes);
    }

    fn values_buf_mut(&mut self) -> &mut [u8] {
        let offset_sz = self.children as usize * self.offset_bytes as usize;
        match (self.fixed_key_length, self.fixed_value_length) {
            (_, Some(fixed_value_length)) => {
                let total_value_size =
                    tf!(fixed_value_length.get()) * self.children as usize;
                let data_buf = self.data_buf_mut();
                let start = data_buf.len() - total_value_size;
                &mut data_buf[start..]
            }
            (Some(fixed_key_length), _) => {
                let start = offset_sz
                    + tf!(fixed_key_length.get()) * self.children as usize;
                &mut self.data_buf_mut()[start..]
            }
            (None, None) => &mut self.data_buf_mut()[offset_sz..],
        }
    }

    fn values_buf(&self) -> &[u8] {
        let offset_sz = self.children as usize * self.offset_bytes as usize;
        match (self.fixed_key_length, self.fixed_value_length) {
            (_, Some(fixed_value_length)) => {
                let total_value_size =
                    tf!(fixed_value_length.get()) * self.children as usize;
                let data_buf = self.data_buf();
                let start = data_buf.len() - total_value_size;
                &data_buf[start..]
            }
            (Some(fixed_key_length), _) => {
                let start = offset_sz
                    + tf!(fixed_key_length.get()) * self.children as usize;
                &self.data_buf()[start..]
            }
            (None, None) => &self.data_buf()[offset_sz..],
        }
    }

    #[inline]
    fn data_buf(&self) -> &[u8] {
        let start = tf!(self.lo_len) + tf!(self.hi_len) + size_of::<Header>();
        &self.as_ref()[start..]
    }

    fn data_buf_mut(&mut self) -> &mut [u8] {
        let start = tf!(self.lo_len) + tf!(self.hi_len) + size_of::<Header>();
        &mut self.as_mut()[start..]
    }

    fn weighted_split_point(&self) -> usize {
        let bits_set = self.activity_sketch.count_ones() as usize;

        if bits_set == 0 {
            // this shouldn't happen often, but it could happen
            // if we burn through our probation_ops_remaining
            // with just removals and no inserts, which don't tick
            // the activity sketch.
            return self.children() / 2;
        }

        let mut weighted_count = 0_usize;
        for bit in 0..8 {
            if (1 << bit) & self.activity_sketch != 0 {
                weighted_count += bit + 1;
            }
        }
        let average_bit = weighted_count / bits_set;
        (average_bit * self.children as usize / 8)
            .min(self.children() - 1)
            .max(1)
    }

    fn split(&self) -> (Inner, Inner) {
        assert!(self.children() >= 2);
        assert!(!self.merging);
        assert!(self.merging_child.is_none());

        let split_point = self.weighted_split_point();

        let left_max: IVec = self.index_key(split_point - 1).into();
        let right_min: IVec = self.index_key(split_point).into();

        assert_ne!(
            left_max, right_min,
            "split point: {} node: {:?}",
            split_point, self
        );

        // see if we can reduce the splitpoint length to reduce
        // the number of bytes that end up in index nodes
        let splitpoint_length = if self.is_index {
            right_min.len()
        } else {
            // we can only perform suffix truncation when
            // choosing the split points for leaf nodes.
            // split points bubble up into indexes, but
            // an important invariant is that for indexes
            // the first item always matches the lo key,
            // otherwise ranges would be permanently
            // inaccessible by falling into the gap
            // during a split.
            right_min
                .iter()
                .zip(left_max.iter())
                .take_while(|(a, b)| a == b)
                .count()
                + 1
        };

        let untruncated_split_key: IVec = self.index_key(split_point).into();

        let possibly_truncated_split_key =
            &untruncated_split_key[..splitpoint_length];

        let split_key =
            self.prefix_decode(KeyRef::Slice(possibly_truncated_split_key));

        if untruncated_split_key.len() != possibly_truncated_split_key.len() {
            log::trace!(
                "shaving off {} bytes for split key",
                untruncated_split_key.len()
                    - possibly_truncated_split_key.len()
            );
        }

        log::trace!(
            "splitting node with lo: {:?} split_key: {:?} hi: {:?} prefix_len {}",
            self.lo(),
            split_key,
            self.hi(),
            self.prefix_len
        );

        #[cfg(test)]
        use rand::Rng;

        // prefix encoded length can only grow or stay the same
        // during splits
        #[cfg(test)]
        let test_jitter_left = rand::thread_rng().gen_range(0, 16);

        #[cfg(not(test))]
        let test_jitter_left = std::u8::MAX as usize;

        let additional_left_prefix = self.lo()[self.prefix_len as usize..]
            .iter()
            .zip(split_key[self.prefix_len as usize..].iter())
            .take((std::u8::MAX - self.prefix_len) as usize)
            .take_while(|(a, b)| a == b)
            .count()
            .min(test_jitter_left);

        #[cfg(test)]
        let test_jitter_right = rand::thread_rng().gen_range(0, 16);

        #[cfg(not(test))]
        let test_jitter_right = std::u8::MAX as usize;

        let additional_right_prefix = if let Some(hi) = self.hi() {
            split_key[self.prefix_len as usize..]
                .iter()
                .zip(hi[self.prefix_len as usize..].iter())
                .take((std::u8::MAX - self.prefix_len) as usize)
                .take_while(|(a, b)| a == b)
                .count()
                .min(test_jitter_right)
        } else {
            0
        };

        log::trace!(
            "trying to add additional left prefix length {} to items {:?}",
            additional_left_prefix,
            self.iter().take(split_point).collect::<Vec<_>>()
        );
        let left_items: Vec<_> = self
            .iter()
            .take(split_point)
            .map(|(k, v)| (k.trim_prefix_bytes(additional_left_prefix), v))
            .collect();

        let right_items: Vec<_> = self
            .iter()
            .skip(split_point)
            .map(|(k, v)| (k.trim_prefix_bytes(additional_right_prefix), v))
            .collect();

        let mut left = Inner::new(
            self.lo(),
            Some(&split_key),
            self.prefix_len + tf!(additional_left_prefix, u8),
            self.is_index,
            self.next,
            &left_items,
        );

        left.rewrite_generations = self.rewrite_generations;

        let mut right = Inner::new(
            &split_key,
            self.hi(),
            self.prefix_len + tf!(additional_right_prefix, u8),
            self.is_index,
            self.next,
            &right_items,
        );

        right.rewrite_generations = self.rewrite_generations;
        right.next = self.next;
        right.probation_ops_remaining =
            tf!((self.children() / 2).min(std::u8::MAX as usize), u8);

        log::trace!(
            "splitting node {:?} into left: {:?} and right: {:?}",
            self,
            left,
            right
        );

        testing_assert!(
            left.is_sorted(),
            "split node left is not sorted: {:?}",
            left
        );
        testing_assert!(
            right.is_sorted(),
            "split node right is not sorted: {:?}",
            right
        );

        (left, right)
    }

    fn receive_merge(&self, other: &Inner) -> Inner {
        log::trace!(
            "merging node receiving merge left: {:?} right: {:?}",
            self,
            other
        );
        assert_eq!(self.hi(), Some(other.lo()));
        assert_eq!(self.is_index, other.is_index);
        assert!(!self.merging);
        assert!(self.merging_child.is_none());

        let extended_keys: Vec<_>;
        let items: Vec<_> = match self.prefix_len.cmp(&other.prefix_len) {
            Equal => {
                log::trace!(
                    "keeping equal prefix lengths of {}",
                    other.prefix_len
                );
                self.iter().chain(other.iter()).collect()
            }
            Greater => {
                extended_keys = self
                    .iter_keys()
                    .map(|k| {
                        prefix::reencode(
                            self.prefix(),
                            &IVec::from(k),
                            other.prefix_len as usize,
                        )
                    })
                    .collect();
                log::trace!("reencoding left items to have shorter prefix len of {}, new items: {:?}", other.prefix_len, extended_keys);
                let left_items = extended_keys
                    .iter()
                    .map(AsRef::as_ref)
                    .zip(self.iter_values());
                left_items
                    .map(|(k, v)| (KeyRef::Slice(k), v))
                    .chain(other.iter())
                    .collect()
            }
            Less => {
                // self.prefix_len < other.prefix_len
                extended_keys = other
                    .iter_keys()
                    .map(|k| {
                        prefix::reencode(
                            other.prefix(),
                            &IVec::from(k),
                            self.prefix_len as usize,
                        )
                    })
                    .collect();
                log::trace!("reencoding right items to have shorter prefix len of {}, new items: {:?}", self.prefix_len, extended_keys);
                let right_items = extended_keys
                    .iter()
                    .map(AsRef::as_ref)
                    .zip(other.iter_values())
                    .map(|(k, v)| (KeyRef::Slice(k), v));
                self.iter().chain(right_items).collect()
            }
        };

        let mut ret = Inner::new(
            self.lo(),
            other.hi(),
            self.prefix_len.min(other.prefix_len),
            self.is_index,
            other.next,
            &*items,
        );

        ret.rewrite_generations =
            self.rewrite_generations.min(other.rewrite_generations);

        testing_assert!(ret.is_sorted());

        ret
    }

    fn header(&self) -> &Header {
        assert_eq!(self.ptr as usize % 8, 0);
        unsafe { &*(self.ptr as *mut u64 as *mut Header) }
    }

    fn header_mut(&mut self) -> &mut Header {
        unsafe { &mut *(self.ptr as *mut Header) }
    }

    fn is_empty(&self) -> bool {
        self.children() == 0
    }

    pub(crate) fn rss(&self) -> u64 {
        self.len as u64
    }

    fn children(&self) -> usize {
        usize::from(self.children)
    }

    fn contains_key(&self, key: &[u8]) -> bool {
        self.find(key).is_ok()
    }

    fn find(&self, key: &[u8]) -> Result<usize, usize> {
        if self.fixed_key_stride > 0 {
            let compare_bytes =
                usize::try_from(self.lo_len - u64::from(self.prefix_len))
                    .unwrap()
                    .min(key.len());
            let distance: usize = linear_distance(
                &self.lo()[self.prefix_len as usize..],
                &key[..compare_bytes],
            );
            let offset = distance / self.fixed_key_stride as usize;
            if key.len() != compare_bytes
                || distance % self.fixed_key_stride as usize != 0
            {
                // search key does not evenly fit based on
                // our fixed stride length
                return Err(offset.min(self.children()));
            }
            assert_eq!(
                key.len(),
                usize::from(self.fixed_key_length.unwrap().get())
            );
            if offset >= self.children as usize {
                return Err(self.children());
            }
            return Ok(offset);
        }

        let mut size = self.children();
        if size == 0 || self.index_key(0).unwrap_slice() > key {
            return Err(0);
        }
        let mut base = 0_usize;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            // mid is always in [0, size), that means mid is >= 0 and < size.
            // mid >= 0: by definition
            // mid < size: mid = size / 2 + size / 4 + size / 8 ...
            let l = self.index_key(mid);
            let cmp = crate::fastcmp(l.unwrap_slice(), key);
            base = if cmp == Greater { base } else { mid };
            size -= half;
        }
        // base is always in [0, size) because base <= mid.
        let l = self.index_key(base);
        let cmp = crate::fastcmp(l.unwrap_slice(), key);

        if cmp == Equal {
            Ok(base)
        } else {
            Err(base + (cmp == Less) as usize)
        }
    }

    pub(crate) fn can_merge_child(&self, pid: u64) -> bool {
        self.merging_child.is_none()
            && !self.merging
            && self.iter_index_pids().any(|p| p == pid)
    }

    fn iter_keys(
        &self,
    ) -> impl Iterator<Item = KeyRef<'_>>
           + ExactSizeIterator
           + DoubleEndedIterator
           + Clone {
        (0..self.children()).map(move |idx| self.index_key(idx))
    }

    fn iter_index_pids(
        &self,
    ) -> impl '_
           + Iterator<Item = u64>
           + ExactSizeIterator
           + DoubleEndedIterator
           + Clone {
        assert!(self.is_index);
        self.iter_values().map(move |pid_bytes| {
            u64::from_le_bytes(pid_bytes.try_into().unwrap())
        })
    }

    fn iter_values(
        &self,
    ) -> impl Iterator<Item = &[u8]> + ExactSizeIterator + DoubleEndedIterator + Clone
    {
        (0..self.children()).map(move |idx| self.index_value(idx))
    }

    fn iter(&self) -> impl Iterator<Item = (KeyRef<'_>, &[u8])> {
        self.iter_keys().zip(self.iter_values())
    }

    pub(crate) fn lo(&self) -> &[u8] {
        let start = size_of::<Header>();
        let end = start + tf!(self.lo_len);
        &self.as_ref()[start..end]
    }

    fn lo_mut(&mut self) -> &mut [u8] {
        let start = size_of::<Header>();
        let end = start + tf!(self.lo_len);
        &mut self.as_mut()[start..end]
    }

    pub(crate) fn hi(&self) -> Option<&[u8]> {
        let start = tf!(self.lo_len) + size_of::<Header>();
        let end = start + tf!(self.hi_len);
        if start == end {
            None
        } else {
            Some(&self.as_ref()[start..end])
        }
    }

    fn hi_mut(&mut self) -> Option<&mut [u8]> {
        let start = tf!(self.lo_len) + size_of::<Header>();
        let end = start + tf!(self.hi_len);
        if start == end {
            None
        } else {
            Some(&mut self.as_mut()[start..end])
        }
    }

    fn index_key(&self, idx: usize) -> KeyRef<'_> {
        assert!(
            idx < self.children(),
            "index {} is not less than internal length of {}",
            idx,
            self.children()
        );

        if self.fixed_key_stride > 0 {
            return KeyRef::Computed {
                base: &self.lo()[self.prefix_len as usize..],
                distance: self.fixed_key_stride as usize * idx,
            };
        }

        let offset_sz = self.children as usize * self.offset_bytes as usize;
        let keys_buf = &self.data_buf()[offset_sz..];
        let key_buf = {
            match (self.fixed_key_length, self.fixed_value_length) {
                (Some(k_sz), Some(_)) | (Some(k_sz), None) => {
                    &keys_buf[idx * tf!(k_sz.get())..]
                }
                (None, Some(_)) | (None, None) => {
                    // find offset for key or combined kv offset
                    let offset = self.offset(idx);
                    &keys_buf[offset..]
                }
            }
        };

        let (start, end) = if let Some(fixed_key_length) = self.fixed_key_length
        {
            (0, tf!(fixed_key_length.get()))
        } else {
            let (key_len, varint_sz) = varint::deserialize(key_buf).unwrap();
            let start = varint_sz;
            let end = start + tf!(key_len);
            (start, end)
        };

        KeyRef::Slice(&key_buf[start..end])
    }

    fn index_value(&self, idx: usize) -> &[u8] {
        assert!(
            idx < self.children(),
            "index {} is not less than internal length of {}",
            idx,
            self.children()
        );

        let buf = self.value_buf_for_offset(idx);

        let (start, end) =
            if let Some(fixed_value_length) = self.fixed_value_length {
                (0, tf!(fixed_value_length.get()))
            } else {
                let (value_len, varint_sz) = varint::deserialize(buf).unwrap();
                let start = varint_sz;
                let end = start + tf!(value_len);
                (start, end)
            };

        &buf[start..end]
    }

    /// `node_kv_pair` returns either the existing (node/key, value, current offset) tuple or
    /// (node/key, none, future offset) where a node/key is node level encoded key.
    fn node_kv_pair<'a>(
        &'a self,
        key: &'a [u8],
    ) -> (KeyRef<'a>, Option<&[u8]>) {
        assert!(key >= self.lo());
        if let Some(hi) = self.hi() {
            assert!(key < hi);
        }

        let suffix = &key[self.prefix_len as usize..];

        let search = self.find(suffix);

        if let Ok(idx) = search {
            (self.index_key(idx), Some(self.index_value(idx)))
        } else {
            let encoded_key = KeyRef::Slice(&key[self.prefix_len as usize..]);
            let encoded_val = None;
            (encoded_key, encoded_val)
        }
    }

    pub(crate) fn contains_upper_bound(&self, bound: &Bound<IVec>) -> bool {
        if let Some(hi) = self.hi() {
            match bound {
                Bound::Excluded(bound) if hi >= &*bound => true,
                Bound::Included(bound) if hi > &*bound => true,
                _ => false,
            }
        } else {
            true
        }
    }

    pub(crate) fn contains_lower_bound(
        &self,
        bound: &Bound<IVec>,
        is_forward: bool,
    ) -> bool {
        let lo = self.lo();
        match bound {
            Bound::Excluded(bound)
                if lo < &*bound || (is_forward && *bound == lo) =>
            {
                true
            }
            Bound::Included(bound) if lo <= &*bound => true,
            Bound::Unbounded if !is_forward => self.hi().is_none(),
            _ => lo.is_empty(),
        }
    }

    fn prefix_decode(&self, key: KeyRef<'_>) -> IVec {
        match key {
            KeyRef::Slice(s) => prefix::decode(self.prefix(), s),
            KeyRef::Computed { base, distance } => {
                let mut ret = prefix::decode(self.prefix(), base);
                apply_computed_distance(&mut ret, distance);
                ret
            }
        }
    }

    pub(crate) fn prefix_encode<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        assert!(self.lo() <= key);
        if let Some(hi) = self.hi() {
            assert!(
                hi > key,
                "key being encoded {:?} >= self.hi {:?}",
                key,
                hi
            );
        }

        &key[self.prefix_len as usize..]
    }

    fn prefix(&self) -> &[u8] {
        &self.lo()[..self.prefix_len as usize]
    }

    #[cfg(feature = "testing")]
    fn is_sorted(&self) -> bool {
        if self.fixed_key_stride > 0 {
            return true;
        }
        if self.children() <= 1 {
            return true;
        }

        for i in 0..self.children() - 1 {
            if self.index_key(i) >= self.index_key(i + 1) {
                log::error!(
                    "key {:?} at index {} >= key {:?} at index {}",
                    self.index_key(i),
                    i,
                    self.index_key(i + 1),
                    i + 1
                );
                return false;
            }
        }

        true
    }
}

mod prefix {
    use crate::IVec;

    pub(super) fn empty() -> &'static [u8] {
        &[]
    }

    pub(super) fn reencode(
        old_prefix: &[u8],
        old_encoded_key: &[u8],
        new_prefix_length: usize,
    ) -> IVec {
        old_prefix
            .iter()
            .chain(old_encoded_key.iter())
            .skip(new_prefix_length)
            .copied()
            .collect()
    }

    pub(super) fn decode(old_prefix: &[u8], old_encoded_key: &[u8]) -> IVec {
        let mut decoded_key =
            Vec::with_capacity(old_prefix.len() + old_encoded_key.len());
        decoded_key.extend_from_slice(old_prefix);
        decoded_key.extend_from_slice(old_encoded_key);

        IVec::from(decoded_key)
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use quickcheck::{Arbitrary, Gen};

    use super::*;

    #[test]
    fn keyref_ord() {
        assert_eq!(
            KeyRef::Computed { base: &[], distance: 0 },
            KeyRef::Slice(&[])
        );
        assert_eq!(
            KeyRef::Computed { base: &[0], distance: 0 },
            KeyRef::Slice(&[0])
        );
        assert_eq!(
            KeyRef::Computed { base: &[0], distance: 1 },
            KeyRef::Slice(&[1])
        );
        assert_eq!(
            KeyRef::Slice(&[1]),
            KeyRef::Computed { base: &[0], distance: 1 },
        );
        assert_eq!(
            KeyRef::Slice(&[1, 0]),
            KeyRef::Computed { base: &[0, 255], distance: 1 },
        );
        assert_eq!(
            KeyRef::Computed { base: &[0, 255], distance: 1 },
            KeyRef::Slice(&[1, 0]),
        );
        assert!(KeyRef::Slice(&[1]) > KeyRef::Slice(&[0]));
        assert!(KeyRef::Slice(&[]) < KeyRef::Slice(&[0]));
        assert!(
            KeyRef::Computed { base: &[0, 255], distance: 2 }
                > KeyRef::Slice(&[1, 0]),
        );
        assert!(
            KeyRef::Slice(&[1, 0])
                < KeyRef::Computed { base: &[0, 255], distance: 2 }
        );
        assert!(
            KeyRef::Computed { base: &[0, 255], distance: 2 }
                < KeyRef::Slice(&[2, 0]),
        );
        assert!(
            KeyRef::Slice(&[2, 0])
                > KeyRef::Computed { base: &[0, 255], distance: 2 }
        );
    }

    #[test]
    fn compute_distance() {
        let table: &[(KeyRef<'_>, &[u8])] = &[
            (KeyRef::Computed { base: &[0], distance: 0 }, &[0]),
            (KeyRef::Computed { base: &[0], distance: 1 }, &[1]),
            (KeyRef::Computed { base: &[0, 255], distance: 1 }, &[1, 0]),
        ];

        for (key_ref, expected) in table {
            let ivec: IVec = key_ref.into();
            assert_eq!(&ivec, expected)
        }
    }

    #[test]
    fn insert_regression() {
        let node = Inner::new(
            &[0, 0, 0, 0, 0, 0, 162, 211],
            Some(&[0, 0, 0, 0, 0, 0, 163, 21]),
            6,
            false,
            Some(NonZeroU64::new(220).unwrap()),
            &[
                (KeyRef::Slice(&[162, 211, 0, 0]), &[]),
                (KeyRef::Slice(&[163, 15, 0, 0]), &[]),
            ],
        );

        Node { overlay: Default::default(), inner: Arc::new(node) }
            .insert(&vec![162, 211, 0, 0].into(), &vec![].into())
            .merge_overlay();
    }

    impl Arbitrary for Node {
        fn arbitrary<G: Gen>(g: &mut G) -> Node {
            Node {
                overlay: Default::default(),
                inner: Arc::new(Inner::arbitrary(g)),
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            let overlay = self.overlay.clone();
            Box::new(
                self.inner.shrink().map(move |ni| Node {
                    overlay: overlay.clone(),
                    inner: ni,
                }),
            )
        }
    }

    impl Arbitrary for Inner {
        fn arbitrary<G: Gen>(g: &mut G) -> Inner {
            use rand::Rng;

            let mut lo: Vec<u8> = Arbitrary::arbitrary(g);
            let mut hi: Option<Vec<u8>> = Some(Arbitrary::arbitrary(g));

            let children: BTreeMap<Vec<u8>, Vec<u8>> = Arbitrary::arbitrary(g);

            if let Some((min_k, _)) = children.iter().next() {
                if *min_k < lo {
                    lo = min_k.clone();
                }
            }

            if let Some((max_k, _)) = children.iter().next_back() {
                if Some(max_k) >= hi.as_ref() {
                    hi = None
                }
            }

            let hi: Option<&[u8]> =
                if let Some(ref hi) = hi { Some(hi) } else { None };

            let equal_length_keys =
                g.gen::<Option<usize>>().map(|kl| (kl % 32).max(1));

            let min_key_length = equal_length_keys.unwrap_or(0);

            let equal_length_values =
                g.gen::<Option<usize>>().map(|vl| (vl % 32).max(1));

            let min_value_length = equal_length_values.unwrap_or(0);

            let children_ref: Vec<(KeyRef<'_>, &[u8])> = children
                .iter()
                .filter(|(k, v)| {
                    k.len() >= min_key_length && v.len() >= min_value_length
                })
                .map(|(k, v)| {
                    (
                        if let Some(kl) = equal_length_keys {
                            KeyRef::Slice(&k[..kl])
                        } else {
                            KeyRef::Slice(k.as_ref())
                        },
                        if let Some(vl) = equal_length_values {
                            &v[..vl]
                        } else {
                            v.as_ref()
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect();

            let mut ret =
                Inner::new(&lo, hi.map(|h| &*h), 0, false, None, &children_ref);

            ret.activity_sketch = g.gen();

            if g.gen_bool(1. / 30.) {
                ret.probation_ops_remaining = g.gen();
            }

            if g.gen_bool(1. / 4.) {
                ret.rewrite_generations = g.gen();
            }

            ret
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            Box::new({
                let node = self.clone();
                let lo = node.lo();
                let shrink_lo = if lo.is_empty() {
                    None
                } else {
                    Some(Inner::new(
                        &lo[..lo.len() - 1],
                        node.hi(),
                        node.prefix_len,
                        node.is_index,
                        node.next,
                        &node.iter().collect::<Vec<_>>(),
                    ))
                };

                let shrink_hi = if let Some(hi) = node.hi() {
                    let new_hi = if !node.is_empty() {
                        let max_k = node.index_key(node.children() - 1);
                        if max_k >= hi[..hi.len() - 1] {
                            None
                        } else {
                            Some(&hi[..hi.len() - 1])
                        }
                    } else {
                        Some(&hi[..hi.len() - 1])
                    };

                    Some(Inner::new(
                        node.lo(),
                        new_hi,
                        node.prefix_len,
                        node.is_index,
                        node.next,
                        &node.iter().collect::<Vec<_>>(),
                    ))
                } else {
                    None
                };

                let item_removals = (0..node.children()).map({
                    let node = self.clone();
                    move |i| {
                        let key = node.index_key(i).into();

                        Node {
                            overlay: Default::default(),
                            inner: Arc::new(node.clone()),
                        }
                        .remove(&key)
                        .merge_overlay()
                        .deref()
                        .clone()
                    }
                });
                let item_reductions = (0..node.children()).flat_map({
                    let node = self.clone();
                    move |i| {
                        let (k, v) = (
                            IVec::from(node.index_key(i)),
                            node.index_value(i).to_vec(),
                        );
                        let k_shrink = k.shrink().flat_map({
                            let node2 = Node {
                                overlay: Default::default(),
                                inner: Arc::new(node.clone()),
                            }
                            .remove(&k.deref().into())
                            .merge_overlay()
                            .deref()
                            .clone();

                            let v = v.clone();
                            move |k| {
                                if node2.contains_key(&k) {
                                    None
                                } else {
                                    let new_node = Node {
                                        overlay: Default::default(),
                                        inner: Arc::new(node2.clone()),
                                    }
                                    .insert(
                                        &k.deref().into(),
                                        &v.deref().into(),
                                    )
                                    .merge_overlay()
                                    .deref()
                                    .clone();
                                    Some(new_node)
                                }
                            }
                        });
                        let v_shrink = v.shrink().map({
                            let node3 = node.clone();
                            move |v| {
                                Node {
                                    overlay: Default::default(),
                                    inner: Arc::new(node3.clone()),
                                }
                                .insert(&k.deref().into(), &v.into())
                                .merge_overlay()
                                .deref()
                                .clone()
                            }
                        });
                        k_shrink.chain(v_shrink)
                    }
                });

                shrink_lo
                    .into_iter()
                    .chain(shrink_hi)
                    .into_iter()
                    .chain(item_removals)
                    .chain(item_reductions)
            })
        }
    }

    fn prop_indexable(
        lo: Vec<u8>,
        hi: Vec<u8>,
        children: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> bool {
        let children_ref: Vec<(KeyRef<'_>, &[u8])> = children
            .iter()
            .map(|(k, v)| (KeyRef::Slice(k.as_ref()), v.as_ref()))
            .collect();
        let ir = Inner::new(&lo, Some(&hi), 0, false, None, &children_ref);

        assert_eq!(ir.children as usize, children_ref.len());

        for (idx, (k, v)) in children_ref.iter().enumerate() {
            assert_eq!(ir.index_key(idx), *k);
            let value = ir.index_value(idx);
            assert_eq!(
                value, *v,
                "expected value index {} to have value {:?} but instead it was {:?}",
                idx, *v, value,
            );
        }
        true
    }

    fn prop_insert_split_merge(
        node: Inner,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> bool {
        // the inserted key must have its bytes after the prefix len
        // be greater than the node's lo key after the prefix len
        let skip_key_ops = !node
            .contains_upper_bound(&Bound::Included((&*key).into()))
            || !node
                .contains_lower_bound(&Bound::Included((&*key).into()), true);

        let node2 = if !node.contains_key(&key) && !skip_key_ops {
            let applied = Node {
                overlay: Default::default(),
                inner: Arc::new(node.clone()),
            }
            .insert(&key.deref().into(), &value.into())
            .merge_overlay()
            .deref()
            .clone();
            let applied_items: Vec<_> = applied.iter().collect();
            let clone = applied.clone();
            let cloned_items: Vec<_> = clone.iter().collect();
            assert_eq!(applied_items, cloned_items);
            applied
        } else {
            node.clone()
        };

        if node2.children() > 2 {
            let (left, right) = node2.split();
            let node3 = left.receive_merge(&right);
            assert_eq!(
                node3.iter().collect::<Vec<_>>(),
                node2.iter().collect::<Vec<_>>()
            );
        }

        if !node.contains_key(&key) && !skip_key_ops {
            let node4 = Node {
                overlay: Default::default(),
                inner: Arc::new(node.clone()),
            }
            .remove(&key.deref().into())
            .merge_overlay()
            .deref()
            .clone();

            assert_eq!(
                node.iter().collect::<Vec<_>>(),
                node4.iter().collect::<Vec<_>>(),
                "we expected that removing item at key {:?} would return the node to its original pre-insertion state",
                key
            );
        }

        true
    }

    quickcheck::quickcheck! {
        #[cfg_attr(miri, ignore)]
        fn indexable(lo: Vec<u8>, hi: Vec<u8>, children: BTreeMap<Vec<u8>, Vec<u8>>) -> bool {
            prop_indexable(lo, hi, children.into_iter().collect())
        }

        #[cfg_attr(miri, ignore)]
        fn insert_split_merge(node: Inner, key: Vec<u8>, value: Vec<u8>) -> bool {
            prop_insert_split_merge(node, key, value)
        }

    }

    #[test]
    fn node_bug_00() {
        // postmortem: offsets were not being stored, and the slot buf was not
        // being considered correctly while writing or reading values in
        // shared slots.
        assert!(prop_indexable(
            vec![],
            vec![],
            vec![(vec![], vec![]), (vec![1], vec![1]),]
        ));
    }

    #[test]
    fn node_bug_01() {
        // postmortem: hi and lo keys were not properly being accounted in the
        // inital allocation
        assert!(prop_indexable(vec![], vec![0], vec![],));
    }

    #[test]
    fn node_bug_02() {
        // postmortem: the test code had some issues with handling invalid keys for nodes
        let node = Inner::new(
            &[47, 97][..],
            None,
            0,
            false,
            None,
            &[(KeyRef::Slice(&[47, 97]), &[]), (KeyRef::Slice(&[99]), &[])],
        );

        assert!(prop_insert_split_merge(node, vec![], vec![]));
    }

    #[test]
    fn node_bug_03() {
        // postmortem: linear key lengths were being improperly determined
        assert!(prop_indexable(
            vec![],
            vec![],
            vec![(vec![], vec![]), (vec![0], vec![]),]
        ));
    }
}
