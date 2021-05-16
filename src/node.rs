#![allow(unsafe_code)]

// TODO we can skip the first offset because it's always 0

use std::{
    alloc::{alloc_zeroed, dealloc, Layout},
    cell::UnsafeCell,
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
        let cell_ptr = fatten(ptr, len);
        Inner { ptr: cell_ptr }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Header {
    // NB always lay out fields from largest to smallest to properly pack the struct
    pub next: Option<NonZeroU64>,
    pub merging_child: Option<NonZeroU64>,
    lo_len: u64,
    hi_len: u64,
    pub children: u32,
    fixed_key_length: Option<NonZeroU16>,
    // we use this form to squish it all into
    // 16 bytes, but really we do allow
    // for Some(0) by shifting everything
    // down by one on access.
    fixed_value_length: Option<NonZeroU16>,
    // if all keys on a node are equidistant,
    // we can avoid writing any data for them
    // at all.
    fixed_key_stride: Option<NonZeroU16>,
    pub prefix_len: u8,
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
    version: u8,
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

// TODO change to u64 or u128 output
// This function has several responsibilities:
// * `find` will call this when looking for the
//   proper child pid on an index, with slice
//   lengths that may or may not match
// * `KeyRef::Ord` and `KeyRef::distance` call
//   this while performing node iteration,
//   again with possibly mismatching slice
//   lengths. Merging nodes together, or
//   merging overlays into inner nodes
//   will rely on this functionality, and
//   it's possible for the lengths to vary.
//
// This is not a general-purpose function. It
// is not possible to determine distances when
// the distance is not representable using the
// return type of this function.
//
// This is different from simply treating
// the byte slice as a zero-padded big-endian
// integer because length exists as a variable
// dimension that must be numerically represented
// in a way that preserves lexicographic ordering.
fn shared_distance(base: &[u8], search: &[u8]) -> usize {
    const fn f1(base: &[u8], search: &[u8]) -> usize {
        (search[search.len() - 1] - base[search.len() - 1]) as usize
    }
    fn f2(base: &[u8], search: &[u8]) -> usize {
        (u16::from_be_bytes(search.try_into().unwrap()) as usize)
            - (u16::from_be_bytes(base.try_into().unwrap()) as usize)
    }
    const fn f3(base: &[u8], search: &[u8]) -> usize {
        (u32::from_be_bytes([0, search[0], search[1], search[2]]) as usize)
            - (u32::from_be_bytes([0, base[0], base[1], base[2]]) as usize)
    }
    fn f4(base: &[u8], search: &[u8]) -> usize {
        (u32::from_be_bytes(search.try_into().unwrap()) as usize)
            - (u32::from_be_bytes(base.try_into().unwrap()) as usize)
    }
    testing_assert!(
        base <= search,
        "expected base {:?} to be <= search {:?}",
        base,
        search
    );
    testing_assert!(
        base.len() == search.len(),
        "base len: {} search len: {}",
        base.len(),
        search.len()
    );
    testing_assert!(!base.is_empty());
    testing_assert!(base.len() <= 4);

    let computed_gotos = [f1, f2, f3, f4];
    computed_gotos[search.len() - 1](base, search)
}

#[derive(Debug, Clone, Copy)]
enum KeyRef<'a> {
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
        (&kr).into()
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
                let buf_len = buf.len();
                buf[buf_len - base.len()..].copy_from_slice(base);
                apply_computed_distance(buf, *distance);
            }
            KeyRef::Slice(s) => buf.copy_from_slice(s),
        }
    }

    fn shared_distance(&self, other: &KeyRef<'_>) -> usize {
        match (self, other) {
            (
                KeyRef::Computed { base: a, distance: da },
                KeyRef::Computed { base: b, distance: db },
            ) => {
                assert!(a.len() <= 4);
                assert!(b.len() <= 4);
                let s_len = a.len().min(b.len());
                let s_a = &a[..s_len];
                let s_b = &b[..s_len];
                let s_da = shift_distance(a, *da, a.len() - s_len);
                let s_db = shift_distance(b, *db, b.len() - s_len);
                match a.cmp(b) {
                    Less => shared_distance(s_a, s_b) + s_db - s_da,
                    Greater => (s_db - s_da) - shared_distance(s_b, s_a),
                    Equal => db - da,
                }
            }
            (KeyRef::Computed { .. }, KeyRef::Slice(b)) => {
                // recurse to first case
                self.shared_distance(&KeyRef::Computed { base: b, distance: 0 })
            }
            (KeyRef::Slice(a), KeyRef::Computed { .. }) => {
                // recurse to first case
                KeyRef::Computed { base: a, distance: 0 }.shared_distance(other)
            }
            (KeyRef::Slice(a), KeyRef::Slice(b)) => {
                // recurse to first case
                KeyRef::Computed { base: a, distance: 0 }
                    .shared_distance(&KeyRef::Computed { base: b, distance: 0 })
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        match self {
            KeyRef::Computed { base, distance } => {
                let mut slack = 0_usize;
                for c in base.iter() {
                    slack += 255 - *c as usize;
                    slack <<= 8;
                }
                slack >>= 8;
                base.len() + if *distance > slack { 1 } else { 0 }
            }
            KeyRef::Slice(s) => s.len(),
        }
    }
}

// this function "corrects" a distance calculated
// for shared prefix lengths by accounting for
// dangling bytes that were omitted from the
// shared calculation. We only need to subtract
// distance when the base is shorter than the
// search key, because in the other case,
// the result is still usable
fn unshift_distance(
    mut shared_distance: usize,
    base: &[u8],
    search: &[u8],
) -> usize {
    if base.len() > search.len() {
        for byte in &base[search.len()..] {
            shared_distance <<= 8;
            shared_distance -= *byte as usize;
        }
    }

    shared_distance
}

fn shift_distance(
    mut buf: &[u8],
    mut distance: usize,
    mut shift: usize,
) -> usize {
    while shift > 0 {
        let last = buf[buf.len() - 1];
        let distance_byte = u8::try_from(distance % 256).unwrap();
        let carry = if 255 - distance_byte < last { 1 } else { 0 };
        distance = (distance >> 8) + carry;
        buf = &buf[..buf.len() - 1];
        shift -= 1;
    }
    distance
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
        // TODO this needs to avoid linear_distance
        // entirely when the lengths between `a` and
        // `b` are more than the number of elements
        // that we can actually represent numerical
        // distances using
        match (self, other) {
            (
                KeyRef::Computed { base: a, distance: da },
                KeyRef::Computed { base: b, distance: db },
            ) => {
                let s_len = a.len().min(b.len());
                let s_a = &a[..s_len];
                let s_b = &b[..s_len];
                let s_da = shift_distance(a, *da, a.len() - s_len);
                let s_db = shift_distance(b, *db, b.len() - s_len);

                let shared_cmp = match s_a.cmp(s_b) {
                    Less => s_da.cmp(&(shared_distance(s_a, s_b) + s_db)),
                    Greater => (shared_distance(s_b, s_a) + s_da).cmp(&s_db),
                    Equal => s_da.cmp(&s_db),
                };

                match shared_cmp {
                    Equal => a.len().cmp(&b.len()),
                    other => other,
                }
            }
            (KeyRef::Computed { .. }, KeyRef::Slice(b)) => {
                // recurse to first case
                self.cmp(&KeyRef::Computed { base: b, distance: 0 })
            }
            (KeyRef::Slice(a), KeyRef::Computed { .. }) => {
                // recurse to first case
                KeyRef::Computed { base: a, distance: 0 }.cmp(other)
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

struct Iter<'a> {
    overlay: std::iter::Skip<im::ordmap::Iter<'a, IVec, Option<IVec>>>,
    node: &'a Inner,
    node_position: usize,
    node_back_position: usize,
    next_a: Option<(&'a [u8], Option<&'a IVec>)>,
    next_b: Option<(KeyRef<'a>, &'a [u8])>,
    next_back_a: Option<(&'a [u8], Option<&'a IVec>)>,
    next_back_b: Option<(KeyRef<'a>, &'a [u8])>,
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
                (Some((k_a, v_a_opt)), Some((k_b, _))) => {
                    let cmp = KeyRef::Slice(k_a).cmp(&k_b);
                    match (cmp, v_a_opt) {
                        (Equal, Some(_)) => {
                            // prefer overlay, discard node value
                            self.next_b.take();
                            log::trace!("src/node.rs:133");
                            log::trace!("iterator returning {:?}", self.next_a);
                            return self.next_a.take().map(|(k, v)| {
                                (KeyRef::Slice(&*k), v.unwrap().as_ref())
                            });
                        }
                        (Equal, None) => {
                            // skip tombstone and continue the loop
                            log::trace!("src/node.rs:141");
                            self.next_a.take();
                            self.next_b.take();
                        }
                        (Less, Some(_)) => {
                            log::trace!("iterator returning {:?}", self.next_a);
                            return self.next_a.take().map(|(k, v)| {
                                (KeyRef::Slice(&*k), v.unwrap().as_ref())
                            });
                        }
                        (Less, None) => {
                            log::trace!("src/node.rs:151");
                            self.next_a.take();
                        }
                        (Greater, Some(_)) => {
                            log::trace!("src/node.rs:120");
                            log::trace!("iterator returning {:?}", self.next_b);
                            return self.next_b.take();
                        }
                        (Greater, None) => {
                            log::trace!("src/node.rs:146");
                            // we do not clear a tombstone until we move past
                            // it in the underlying node
                            log::trace!("iterator returning {:?}", self.next_b);
                            return self.next_b.take();
                        }
                    }
                }
            }
        }
    }
}

impl<'a> DoubleEndedIterator for Iter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            if self.next_back_a.is_none() {
                log::trace!("src/node.rs:458");
                if let Some((k, v)) = self.overlay.next_back() {
                    log::trace!("next_back_a is now ({:?}, {:?})", k, v);
                    self.next_back_a = Some((k.as_ref(), v.as_ref()));
                }
            }
            if self.next_back_b.is_none() && self.node_back_position > 0 {
                self.node_back_position -= 1;
                self.next_back_b = Some((
                    self.node.index_key(self.node_back_position),
                    self.node.index_value(self.node_back_position),
                ));
                log::trace!("next_back_b is now {:?}", self.next_back_b);
            }
            match (self.next_back_a, self.next_back_b) {
                (None, _) => {
                    log::trace!("src/node.rs:474");
                    log::trace!("iterator returning {:?}", self.next_back_b);
                    return self.next_back_b.take();
                }
                (Some((_, None)), None) => {
                    log::trace!("src/node.rs:480");
                    self.next_back_a.take();
                }
                (Some((k_a, None)), Some((k_b, _))) if k_b == *k_a => {
                    // skip tombstone and continue the loop
                    log::trace!("src/node.rs:491");
                    self.next_back_a.take();
                    self.next_back_b.take();
                }
                (Some((k_a, None)), Some((k_b, _))) if k_b > *k_a => {
                    log::trace!("src/node.rs:496");
                    // we do not clear a tombstone until we move past
                    // it in the underlying node
                    log::trace!("iterator returning {:?}", self.next_back_b);
                    return self.next_back_b.take();
                }
                (Some((k_a, None)), Some((k_b, _))) if k_b < *k_a => {
                    log::trace!("src/node.rs:503");
                    self.next_back_a.take();
                }
                (Some((_, Some(_))), None) => {
                    log::trace!("src/node.rs:483");
                    log::trace!("iterator returning {:?}", self.next_back_a);
                    return self.next_back_a.take().map(|(k, v)| {
                        (KeyRef::Slice(&*k), v.unwrap().as_ref())
                    });
                }
                (Some((k_a, Some(_))), Some((k_b, _))) if k_b > *k_a => {
                    log::trace!("src/node.rs:508");
                    log::trace!("iterator returning {:?}", self.next_back_b);
                    return self.next_back_b.take();
                }
                (Some((k_a, Some(_))), Some((k_b, _))) if k_b < *k_a => {
                    log::trace!("iterator returning {:?}", self.next_back_a);
                    return self.next_back_a.take().map(|(k, v)| {
                        (KeyRef::Slice(&*k), v.unwrap().as_ref())
                    });
                }
                (Some((k_a, Some(_))), Some((k_b, _))) if k_b == *k_a => {
                    // prefer overlay, discard node value
                    self.next_back_b.take();
                    log::trace!("src/node.rs:520");
                    log::trace!("iterator returning {:?}", self.next_back_a);
                    return self.next_back_a.take().map(|(k, v)| {
                        (KeyRef::Slice(&*k), v.unwrap().as_ref())
                    });
                }
                _ => unreachable!(
                    "did not expect combination a: {:?} b: {:?}",
                    self.next_back_a, self.next_back_b
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
    pub(crate) overlay: im::OrdMap<IVec, Option<IVec>>,
    pub(crate) inner: Arc<Inner>,
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
            overlay: self.overlay.iter().skip(0),
            node: &self.inner,
            node_position: 0,
            next_a: None,
            next_b: None,
            node_back_position: self.children(),
            next_back_a: None,
            next_back_b: None,
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
        Node {
            overlay: Default::default(),
            inner: Arc::new(Inner::new_root(child_pid)),
        }
    }

    pub(crate) fn new_hoisted_root(left: u64, at: &[u8], right: u64) -> Node {
        Node {
            overlay: Default::default(),
            inner: Arc::new(Inner::new_hoisted_root(left, at, right)),
        }
    }

    pub(crate) fn new_empty_leaf() -> Node {
        Node {
            overlay: Default::default(),
            inner: Arc::new(Inner::new_empty_leaf()),
        }
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
                Node { inner: ret, overlay: Default::default() }
            }
            ChildMergeCap => {
                let mut ret = self.merge_overlay();
                Arc::make_mut(&mut ret).merging = true;
                Node { inner: ret, overlay: Default::default() }
            }
        }
    }

    fn insert(&self, key: &IVec, value: &IVec) -> Node {
        let overlay = self.overlay.update(key.clone(), Some(value.clone()));
        Node { overlay, inner: self.inner.clone() }
    }

    fn remove(&self, key: &IVec) -> Node {
        let overlay = self.overlay.update(key.clone(), None);
        let ret = Node { overlay, inner: self.inner.clone() };
        log::trace!(
            "applying removal of key {:?} results in node {:?}",
            key,
            ret
        );
        ret
    }

    fn contains_key(&self, key: &[u8]) -> bool {
        if key < self.lo()
            || if let Some(hi) = self.hi() { key >= hi } else { false }
        {
            return false;
        }
        if let Some(fixed_key_length) = self.fixed_key_length {
            if usize::from(fixed_key_length.get()) != key.len() {
                return false;
            }
        }
        self.overlay.contains_key(key) || self.inner.contains_key(key)
    }

    // Push the overlay into the backing node.
    fn merge_overlay(&self) -> Arc<Inner> {
        if self.overlay.is_empty() {
            return self.inner.clone();
        };

        // if this is a node that has a fixed key stride
        // and empty values, we may be able to skip
        // the normal merge process by performing a
        // header-only update
        let can_seamlessly_absorb = self.fixed_key_stride.is_some()
            && self.fixed_value_length() == Some(0)
            && {
                let mut prev = self.inner.index_key(self.inner.children() - 1);
                let stride: u16 = self.fixed_key_stride.unwrap().get();
                let mut length_and_stride_matches = true;
                for (k, v) in &self.overlay {
                    length_and_stride_matches &=
                        v.is_some() && v.as_ref().unwrap().is_empty();
                    length_and_stride_matches &= KeyRef::Slice(&*k) > prev
                        && is_linear(&prev, &KeyRef::Slice(&*k), stride);

                    prev = KeyRef::Slice(&*k);

                    if !length_and_stride_matches {
                        break;
                    }
                }
                length_and_stride_matches
            };

        if can_seamlessly_absorb {
            let mut ret = self.inner.deref().clone();
            ret.children = self
                .inner
                .children
                .checked_add(u32::try_from(self.overlay.len()).unwrap())
                .unwrap();
            return Arc::new(ret);
        }

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

        #[cfg(feature = "testing")]
        {
            let orig_ivec_pairs: Vec<_> = self
                .iter()
                .map(|(k, v)| (self.prefix_decode(k), IVec::from(v)))
                .collect();

            let new_ivec_pairs: Vec<_> = ret
                .iter()
                .map(|(k, v)| (ret.prefix_decode(k), IVec::from(v)))
                .collect();

            assert_eq!(orig_ivec_pairs, new_ivec_pairs);
        }

        ret.merging = self.merging;
        ret.merging_child = self.merging_child;
        ret.probation_ops_remaining =
            self.probation_ops_remaining.saturating_sub(
                u8::try_from(self.overlay.len().min(u8::MAX as usize)).unwrap(),
            );

        log::trace!("merged node {:?} into {:?}", self, ret);
        Arc::new(ret)
    }

    pub(crate) fn set_next(&mut self, next: Option<NonZeroU64>) {
        Arc::get_mut(&mut self.inner).unwrap().next = next;
    }

    pub(crate) fn increment_rewrite_generations(&mut self) {
        let rewrite_generations = self.rewrite_generations;

        // don't bump rewrite_generations unless we've cooled
        // down after the last split.
        if self.activity_sketch == 0 {
            Arc::make_mut(&mut self.inner).rewrite_generations =
                rewrite_generations.saturating_add(1);
        }
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

        #[cfg(feature = "testing")]
        {
            let orig_ivec_pairs: Vec<_> = self
                .iter()
                .map(|(k, v)| (self.prefix_decode(k), IVec::from(v)))
                .chain(
                    other
                        .iter()
                        .map(|(k, v)| (other.prefix_decode(k), IVec::from(v))),
                )
                .collect();

            let new_ivec_pairs: Vec<_> = ret
                .iter()
                .map(|(k, v)| (ret.prefix_decode(k), IVec::from(v)))
                .collect();

            assert_eq!(orig_ivec_pairs, new_ivec_pairs);
        }

        log::trace!("merge created node {:?}", ret);
        ret
    }

    pub(crate) fn split(&self) -> (Node, Node) {
        let (lhs_inner, rhs_inner) = self.merge_overlay().split();
        let lhs =
            Node { inner: Arc::new(lhs_inner), overlay: Default::default() };
        let rhs =
            Node { inner: Arc::new(rhs_inner), overlay: Default::default() };

        #[cfg(feature = "testing")]
        {
            let orig_ivec_pairs: Vec<_> = self
                .iter()
                .map(|(k, v)| (self.prefix_decode(k), IVec::from(v)))
                .collect();

            let new_ivec_pairs: Vec<_> = lhs
                .iter()
                .map(|(k, v)| (lhs.prefix_decode(k), IVec::from(v)))
                .chain(
                    rhs.iter()
                        .map(|(k, v)| (rhs.prefix_decode(k), IVec::from(v))),
                )
                .collect();

            assert_eq!(
                orig_ivec_pairs, new_ivec_pairs,
                "splitting node {:?} failed",
                self
            );
        }

        (lhs, rhs)
    }

    pub(crate) fn parent_split(&self, at: &[u8], to: u64) -> Option<Node> {
        let encoded_sep = &at[self.prefix_len as usize..];
        if self.contains_key(encoded_sep) {
            log::debug!(
                "parent_split skipped because \
                parent node already contains child with key {:?} \
                pid {} \
                at split point due to deep race. parent node: {:?}",
                at,
                to,
                self
            );
            return None;
        }

        if at < self.lo()
            || if let Some(hi) = self.hi() { hi <= at } else { false }
        {
            log::debug!(
                "tried to add split child at {:?} to parent index node {:?}",
                at,
                self
            );
            return None;
        }

        let value = Some(to.to_le_bytes().as_ref().into());
        let overlay = self.overlay.update(encoded_sep.into(), value);

        let new_inner =
            Node { overlay, inner: self.inner.clone() }.merge_overlay();

        Some(Node { overlay: Default::default(), inner: new_inner })
    }

    /// `node_kv_pair` returns either the existing (node/key, value, current offset) tuple or
    /// (node/key, none, future offset) where a node/key is node level encoded key.
    pub(crate) fn node_kv_pair<'a>(
        &'a self,
        key: &'a [u8],
    ) -> (IVec, Option<&[u8]>) {
        let encoded_key = self.prefix_encode(key);
        if let Some(v) = self.overlay.get(encoded_key) {
            (encoded_key.into(), v.as_ref().map(AsRef::as_ref))
        } else {
            // look for the key in our compacted inner node
            let search = self.find(encoded_key);

            if let Ok(idx) = search {
                (self.index_key(idx).into(), Some(self.index_value(idx)))
            } else {
                (encoded_key.into(), None)
            }
        }
    }

    pub(crate) fn successor(
        &self,
        bound: &Bound<IVec>,
    ) -> Option<(IVec, IVec)> {
        let (overlay, node_position) = match bound {
            Bound::Unbounded => (self.overlay.iter().skip(0), 0),
            Bound::Included(b) => {
                if let Some(Some(v)) = self.overlay.get(b) {
                    // short circuit return
                    return Some((b.clone(), v.clone()));
                }
                let overlay_search = self.overlay.range(b.clone()..).skip(0);

                let inner_search = if &**b < self.lo() {
                    Err(0)
                } else {
                    self.find(self.prefix_encode(b))
                };
                let node_position = match inner_search {
                    Ok(idx) => {
                        return Some((
                            self.prefix_decode(self.inner.index_key(idx)),
                            self.inner.index_value(idx).into(),
                        ))
                    }
                    Err(idx) => idx,
                };

                (overlay_search, node_position)
            }
            Bound::Excluded(b) => {
                let overlay_search = if self.overlay.contains_key(b) {
                    self.overlay.range(b.clone()..).skip(1)
                } else {
                    self.overlay.range(b.clone()..).skip(0)
                };

                let inner_search = if &**b < self.lo() {
                    Err(0)
                } else {
                    self.find(self.prefix_encode(b))
                };
                let node_position = match inner_search {
                    Ok(idx) => idx + 1,
                    Err(idx) => idx,
                };

                (overlay_search, node_position)
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
            node_back_position: self.children(),
            next_back_a: None,
            next_back_b: None,
        };

        let ret: Option<(KeyRef<'_>, &[u8])> = iter.find(|(k, _)| in_bounds(k));

        ret.map(|(k, v)| (self.prefix_decode(k), v.into()))
    }

    pub(crate) fn predecessor(
        &self,
        bound: &Bound<IVec>,
    ) -> Option<(IVec, IVec)> {
        let (overlay, node_back_position) = match bound {
            Bound::Unbounded => (self.overlay.iter().skip(0), self.children()),
            Bound::Included(b) => {
                let overlay = self.overlay.range(..=b.clone()).skip(0);

                let inner_search = if &**b < self.lo() {
                    Err(0)
                } else {
                    self.find(self.prefix_encode(b))
                };
                let node_back_position = match inner_search {
                    Ok(idx) => {
                        return Some((
                            self.prefix_decode(self.inner.index_key(idx)),
                            self.inner.index_value(idx).into(),
                        ))
                    }
                    Err(idx) => idx,
                };

                (overlay, node_back_position)
            }
            Bound::Excluded(b) => {
                let overlay = self.overlay.range(..b.clone()).skip(0);

                let above_hi =
                    if let Some(hi) = self.hi() { &**b >= hi } else { false };

                let inner_search = if above_hi {
                    Err(self.children())
                } else {
                    self.find(self.prefix_encode(b))
                };
                #[allow(clippy::match_same_arms)]
                let node_back_position = match inner_search {
                    Ok(idx) => idx,
                    Err(idx) => idx,
                };

                (overlay, node_back_position)
            }
        };

        let iter = Iter {
            overlay,
            node: &self.inner,
            node_position: 0,
            node_back_position,
            next_a: None,
            next_b: None,
            next_back_a: None,
            next_back_b: None,
        };

        let in_bounds = |k: &KeyRef<'_>| match bound {
            Bound::Unbounded => true,
            Bound::Included(b) => *k <= b[self.prefix_len as usize..],
            Bound::Excluded(b) => *k < b[self.prefix_len as usize..],
        };

        let ret: Option<(KeyRef<'_>, &[u8])> =
            iter.rev().find(|(k, _)| in_bounds(k));

        ret.map(|(k, v)| (self.prefix_decode(k), v.into()))
    }

    pub(crate) fn index_next_node(&self, key: &[u8]) -> (bool, u64) {
        log::trace!("index_next_node for key {:?} on node {:?}", key, self);
        assert!(self.overlay.is_empty());
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

        log::trace!("index_next_node for key {:?} returning pid {} after seaching node {:?}", key, pid, self);
        (is_leftmost, pid)
    }

    pub(crate) fn should_split(&self) -> bool {
        log::trace!("seeing if we should split node {:?}", self);
        let size_checks = if cfg!(any(test, feature = "lock_free_delays")) {
            self.iter().take(6).count() > 5
        } else {
            let size_threshold = 1024 - crate::MAX_MSG_HEADER_LEN;
            let child_threshold = 56 * 1024;

            self.len() > size_threshold || self.children > child_threshold
        };

        let safety_checks = self.merging_child.is_none()
            && !self.merging
            && self.iter().take(2).count() == 2
            && self.probation_ops_remaining == 0;

        if size_checks {
            log::trace!(
                "should_split: {} is index: {} children: {} size: {}",
                safety_checks && size_checks,
                self.is_index,
                self.children,
                self.rss()
            );
        }

        safety_checks && size_checks
    }

    pub(crate) fn should_merge(&self) -> bool {
        let size_check = if cfg!(any(test, feature = "lock_free_delays")) {
            self.iter().take(2).count() < 2
        } else {
            let size_threshold = 256 - crate::MAX_MSG_HEADER_LEN;
            self.len() < size_threshold
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
    ptr: *mut UnsafeCell<[u8]>,
}

/// <https://users.rust-lang.org/t/construct-fat-pointer-to-struct/29198/9>
#[allow(trivial_casts)]
fn fatten(data: *mut u8, len: usize) -> *mut UnsafeCell<[u8]> {
    // Requirements of slice::from_raw_parts.
    assert!(!data.is_null());
    assert!(isize::try_from(len).is_ok());

    let slice = unsafe { core::slice::from_raw_parts(data as *const (), len) };
    slice as *const [()] as *mut _
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
        let layout = Layout::from_size_align(self.len(), ALIGNMENT).unwrap();
        unsafe {
            dealloc(self.ptr(), layout);
        }
    }
}

impl AsRef<[u8]> for Inner {
    fn as_ref(&self) -> &[u8] {
        self.buf()
    }
}

impl AsMut<[u8]> for Inner {
    fn as_mut(&mut self) -> &mut [u8] {
        self.buf_mut()
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

        if self.fixed_value_length() == Some(0)
            && self.fixed_key_stride.is_some()
        {
            ds.field("fixed node, all keys and values omitted", &()).finish()
        } else if self.is_index {
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

// determines if the item can be losslessly
// constructed from the base by adding a fixed
// stride to it.
fn is_linear(a: &KeyRef<'_>, b: &KeyRef<'_>, stride: u16) -> bool {
    let a_len = a.len();
    if a_len != b.len() || a_len > 4 {
        return false;
    }

    a.shared_distance(b) == stride as usize
}

impl Inner {
    #[inline]
    fn ptr(&self) -> *mut u8 {
        unsafe { (*self.ptr).get() as *mut u8 }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.buf().len()
    }

    #[inline]
    fn buf(&self) -> &[u8] {
        unsafe { &*(*self.ptr).get() }
    }

    #[inline]
    fn buf_mut(&mut self) -> &mut [u8] {
        unsafe { &mut *(*self.ptr).get() }
    }

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
        assert!(items.len() <= u32::MAX as usize);

        // determine if we need to use varints and offset
        // indirection tables, or if everything is equal
        // size we can skip this. If all keys are linear
        // with a fixed stride, we can completely skip writing
        // them at all, as they can always be calculated by
        // adding the desired offset to the lo key.

        // we compare the lo key to the second item because
        // it is assumed that the first key matches the lo key
        // in the case of a fixed stride
        let mut fixed_key_stride: Option<u16> = if items.len() > 1
            && lo[prefix_len as usize..].len() == items[1].0.len()
            && items[1].0.len() <= 4
        {
            assert!(
                items[1].0 > lo[prefix_len as usize..],
                "somehow, the second key on this node is not greater \
                than the node low key (adjusted for prefix): \
                lo: {:?} items: {:?}",
                lo,
                items
            );
            u16::try_from(
                KeyRef::Slice(&lo[prefix_len as usize..])
                    .shared_distance(&items[1].0),
            )
            .ok()
        } else {
            None
        };

        let mut fixed_key_length = match items {
            [(kr, _), ..] if !kr.is_empty() => Some(kr.len()),
            _ => None,
        };

        let mut fixed_value_length = items.first().map(|(_, v)| v.len());

        let mut dynamic_key_storage_size = 0;
        let mut dynamic_value_storage_size = 0;

        let mut prev: Option<&KeyRef<'_>> = None;

        // the first pass over items determines the various
        // sizes required to represent keys and values, and
        // whether keys, values, or both share the same sizes
        // or possibly whether the keys increment at a fixed
        // rate so that they can be completely skipped
        for (k, v) in items {
            dynamic_key_storage_size += k.len() + varint::size(k.len() as u64);
            dynamic_value_storage_size +=
                v.len() + varint::size(v.len() as u64);

            if fixed_key_length.is_some() {
                if let Some(last) = prev {
                    // see if the lengths all match for the offset table
                    // omission optimization
                    if last.len() == k.len() {
                        // see if the keys are equidistant for the
                        // key omission optimization
                        if let Some(stride) = fixed_key_stride {
                            if !is_linear(last, k, stride) {
                                fixed_key_stride = None;
                            }
                        }
                    } else {
                        fixed_key_length = None;
                        fixed_key_stride = None;
                    }
                }

                prev = Some(k);
            }

            if let Some(fvl) = fixed_value_length {
                if v.len() != fvl {
                    fixed_value_length = None;
                }
            }
        }
        let fixed_key_length = fixed_key_length
            .and_then(|fkl| u16::try_from(fkl).ok())
            .and_then(NonZeroU16::new);

        let fixed_key_stride =
            fixed_key_stride.map(|stride| NonZeroU16::new(stride).unwrap());

        let fixed_value_length = fixed_value_length
            .and_then(|fvl| {
                if fvl < u16::MAX as usize {
                    // we add 1 to the fvl to
                    // represent Some(0) in
                    // less space.
                    u16::try_from(fvl).ok()
                } else {
                    None
                }
            })
            .and_then(|fvl| NonZeroU16::new(fvl + 1));

        let key_storage_size = if let Some(key_length) = fixed_key_length {
            assert_ne!(key_length.get(), 0);
            if let Some(stride) = fixed_key_stride {
                // all keys can be directly computed from the node lo key
                // by adding a fixed stride length to the node lo key
                assert!(stride.get() > 0);
                0
            } else {
                key_length.get() as usize * items.len()
            }
        } else {
            dynamic_key_storage_size
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
            (value_length.get() - 1) as usize * items.len()
        } else {
            dynamic_value_storage_size
        }
        .max(size_of::<u64>());

        let (offsets_storage_size, offset_bytes) = if fixed_key_length.is_some()
            && fixed_value_length.is_some()
        {
            (0, 0)
        } else {
            let max_indexable_offset =
                if fixed_key_length.is_some() { 0 } else { key_storage_size }
                    + if fixed_value_length.is_some() {
                        0
                    } else {
                        value_storage_size
                    };

            let bytes_per_offset: u8 = match max_indexable_offset {
                i if i < 256 => 1,
                i if i < (1 << 16) => 2,
                i if i < (1 << 24) => 3,
                i if i < (1 << 32) => 4,
                i if i < (1 << 40) => 5,
                i if i < (1 << 48) => 6,
                _ => unreachable!(),
            };

            (bytes_per_offset as usize * items.len(), bytes_per_offset)
        };

        let total_node_storage_size = size_of::<Header>()
            + hi.map(|hi| hi.len()).unwrap_or(0)
            + lo.len()
            + key_storage_size
            + value_storage_size
            + offsets_storage_size;

        let mut ret = uninitialized_node(tf!(total_node_storage_size));

        if offset_bytes == 0 {
            assert!(fixed_key_length.is_some());
            assert!(fixed_value_length.is_some());
        }

        let header = ret.header_mut();
        header.fixed_key_length = fixed_key_length;
        header.fixed_value_length = fixed_value_length;
        header.lo_len = lo.len() as u64;
        header.hi_len = hi.map(|hi| hi.len() as u64).unwrap_or(0);
        header.fixed_key_stride = fixed_key_stride;
        header.offset_bytes = offset_bytes;
        header.children = tf!(items.len(), u32);
        header.prefix_len = prefix_len;
        header.version = 1;
        header.next = next;
        header.is_index = is_index;

        /*
        header.merging_child = None;
        header.merging = false;
        header.probation_ops_remaining = 0;
        header.activity_sketch = 0;
        header.rewrite_generations = 0;
         * TODO use UnsafeCell to allow this to soundly work
        *ret.header_mut() = Header {
            rewrite_generations: 0,
            activity_sketch: 0,
            probation_ops_remaining: 0,
            merging_child: None,
            merging: false,
            fixed_key_length,
            fixed_value_length,
            lo_len: lo.len() as u64,
            hi_len: hi.map(|hi| hi.len() as u64).unwrap_or(0),
            fixed_key_stride,
            offset_bytes,
            children: tf!(items.len(), u32),
            prefix_len,
            version: 1,
            next,
            is_index,
        };
        */

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
            if fixed_key_length.is_none() || fixed_value_length.is_none() {
                ret.set_offset(idx, tf!(offset));
            }
            if fixed_key_length.is_none() {
                assert!(fixed_key_stride.is_none());
                offset += varint::size(k.len() as u64) as u64 + k.len() as u64;
            }
            if fixed_value_length.is_none() {
                offset += varint::size(v.len() as u64) as u64 + v.len() as u64;
            }

            if let Some(stride) = fixed_key_stride {
                assert!(stride.get() > 0);
            } else {
                // we completely skip writing any key data at all
                // when the keys are linear, as they can be
                // computed losslessly by multiplying the desired
                // index by the fixed stride length.
                let mut key_buf = ret.key_buf_for_offset_mut(idx);
                if fixed_key_length.is_none() {
                    let varint_bytes =
                        varint::serialize_into(k.len() as u64, key_buf);
                    key_buf = &mut key_buf[varint_bytes..];
                }
                k.write_into(&mut key_buf[..k.len()]);
            }

            let mut value_buf = ret.value_buf_for_offset_mut(idx);
            if fixed_value_length.is_none() {
                let varint_bytes =
                    varint::serialize_into(v.len() as u64, value_buf);
                value_buf = &mut value_buf[varint_bytes..];
            }
            value_buf[..v.len()].copy_from_slice(v);
        }

        if ret.is_index {
            assert!(!ret.is_empty())
        }

        if let Some(stride) = ret.fixed_key_stride {
            assert!(
                ret.fixed_key_length.is_some(),
                "fixed_key_stride is {} but fixed_key_length \
                is None for generated node {:?}",
                stride,
                ret
            );
        }

        testing_assert!(
            ret.is_sorted(),
            "created new node is not sorted: {:?}, had items passed in: {:?} fixed stride: {:?}",
            ret,
            items,
            fixed_key_stride
        );

        #[cfg(feature = "testing")]
        {
            for i in 0..items.len() {
                if fixed_key_length.is_none() || fixed_value_length.is_none() {
                    assert!(
                        ret.offset(i) < total_node_storage_size,
                        "offset {} is {} which is larger than \
                    total node storage size of {} for node \
                    with header {:#?}",
                        i,
                        ret.offset(i),
                        total_node_storage_size,
                        ret.header()
                    );
                }
            }
        }

        log::trace!("created new node {:?}", ret);

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

    fn fixed_value_length(&self) -> Option<usize> {
        self.fixed_value_length.map(|fvl| usize::from(fvl.get()) - 1)
    }

    // returns the OPEN ENDED buffer where a key may be placed
    fn key_buf_for_offset_mut(&mut self, index: usize) -> &mut [u8] {
        assert!(self.fixed_key_stride.is_none());
        let offset_sz = self.children as usize * self.offset_bytes as usize;
        if let Some(k_sz) = self.fixed_key_length {
            let keys_buf = &mut self.data_buf_mut()[offset_sz..];
            &mut keys_buf[index * tf!(k_sz.get())..]
        } else {
            // find offset for key or combined kv offset
            let offset = self.offset(index);
            let keys_buf = &mut self.data_buf_mut()[offset_sz..];
            &mut keys_buf[offset..]
        }
    }

    // returns the OPEN ENDED buffer where a value may be placed
    //
    // NB: it's important that this is only ever called after setting
    // the key and its varint length prefix, as this needs to be parsed
    // for case 4.
    fn value_buf_for_offset_mut(&mut self, index: usize) -> &mut [u8] {
        let stride = self.fixed_key_stride;
        match (self.fixed_key_length, self.fixed_value_length()) {
            (_, Some(0)) => &mut [],
            (Some(_), Some(v_sz)) | (None, Some(v_sz)) => {
                let values_buf = self.values_buf_mut();
                &mut values_buf[index * tf!(v_sz)..]
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
                let (key_len, key_varint_sz) = if stride.is_some() {
                    (0, 0)
                } else {
                    varint::deserialize(slot_buf).unwrap()
                };
                &mut slot_buf[tf!(key_len) + key_varint_sz..]
            }
        }
    }

    // returns the OPEN ENDED buffer where a value may be read
    //
    // NB: it's important that this is only ever called after setting
    // the key and its varint length prefix, as this needs to be parsed
    // for case 4.
    fn value_buf_for_offset(&self, index: usize) -> &[u8] {
        let stride = self.fixed_key_stride;
        match (self.fixed_key_length, self.fixed_value_length()) {
            (_, Some(0)) => &[],
            (Some(_), Some(v_sz)) | (None, Some(v_sz)) => {
                let values_buf = self.values_buf();
                &values_buf[index * v_sz..]
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
                let (key_len, key_varint_sz) = if stride.is_some() {
                    (0, 0)
                } else {
                    varint::deserialize(slot_buf).unwrap()
                };
                &slot_buf[tf!(key_len) + key_varint_sz..]
            }
        }
    }

    #[inline]
    fn offset(&self, index: usize) -> usize {
        assert!(index < self.children as usize);
        assert!(
            self.offset_bytes > 0,
            "offset invariant failed on {:#?}",
            self.header()
        );
        let offsets_buf_start =
            tf!(self.lo_len) + tf!(self.hi_len) + size_of::<Header>();

        let start = offsets_buf_start + (index * self.offset_bytes as usize);

        let mask = usize::MAX
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
            let ptr: *const u8 = self.ptr().add(start);
            std::ptr::copy_nonoverlapping(
                ptr,
                tmp.as_mut_ptr() as *mut u8,
                len,
            );
            *tmp.as_mut_ptr() &= mask;
            tmp.assume_init()
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
        match (self.fixed_key_length, self.fixed_value_length()) {
            (_, Some(0)) => &mut [],
            (_, Some(fixed_value_length)) => {
                let total_value_size = fixed_value_length * self.children();
                let data_buf = self.data_buf_mut();
                let start = data_buf.len() - total_value_size;
                &mut data_buf[start..]
            }
            (Some(fixed_key_length), _) => {
                let start = if self.fixed_key_stride.is_some() {
                    offset_sz
                } else {
                    offset_sz + tf!(fixed_key_length.get()) * self.children()
                };
                &mut self.data_buf_mut()[start..]
            }
            (None, None) => &mut self.data_buf_mut()[offset_sz..],
        }
    }

    fn values_buf(&self) -> &[u8] {
        let offset_sz = self.children as usize * self.offset_bytes as usize;
        match (self.fixed_key_length, self.fixed_value_length()) {
            (_, Some(0)) => &[],
            (_, Some(fixed_value_length)) => {
                let total_value_size = fixed_value_length * self.children();
                let data_buf = self.data_buf();
                let start = data_buf.len() - total_value_size;
                &data_buf[start..]
            }
            (Some(fixed_key_length), _) => {
                let start = if self.fixed_key_stride.is_some() {
                    offset_sz
                } else {
                    offset_sz
                        + tf!(fixed_key_length.get()) * self.children as usize
                };
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
        let test_jitter_left = u8::MAX as usize;

        let additional_left_prefix = self.lo()[self.prefix_len as usize..]
            .iter()
            .zip(split_key[self.prefix_len as usize..].iter())
            .take((u8::MAX - self.prefix_len) as usize)
            .take_while(|(a, b)| a == b)
            .count()
            .min(test_jitter_left);

        #[cfg(test)]
        let test_jitter_right = rand::thread_rng().gen_range(0, 16);

        #[cfg(not(test))]
        let test_jitter_right = u8::MAX as usize;

        let additional_right_prefix = if let Some(hi) = self.hi() {
            split_key[self.prefix_len as usize..]
                .iter()
                .zip(hi[self.prefix_len as usize..].iter())
                .take((u8::MAX - self.prefix_len) as usize)
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
            .map(|(k, v)| (IVec::from(k), v))
            .collect();

        let left_items: Vec<_> = left_items
            .iter()
            .map(|(k, v)| (KeyRef::Slice(&k[additional_left_prefix..]), *v))
            .collect();

        // we need to convert these to ivecs first
        // because if we shave off bytes of the
        // KeyRef base then it may corrupt their
        // semantic meanings, applying distances
        // that overflow into different values
        // than what the KeyRef was originally
        // created to represent.
        let right_ivecs: Vec<_> = self
            .iter()
            .skip(split_point)
            .map(|(k, v)| (IVec::from(k), v))
            .collect();

        let right_items: Vec<_> = right_ivecs
            .iter()
            .map(|(k, v)| (KeyRef::Slice(&k[additional_right_prefix..]), *v))
            .collect();

        let mut left = Inner::new(
            self.lo(),
            Some(&split_key),
            self.prefix_len + tf!(additional_left_prefix, u8),
            self.is_index,
            self.next,
            &left_items,
        );

        left.rewrite_generations =
            if split_point == 1 { 0 } else { self.rewrite_generations };
        left.probation_ops_remaining =
            tf!((self.children() / 2).min(u8::MAX as usize), u8);

        let mut right = Inner::new(
            &split_key,
            self.hi(),
            self.prefix_len + tf!(additional_right_prefix, u8),
            self.is_index,
            self.next,
            &right_items,
        );

        right.rewrite_generations = if split_point == self.children() - 1 {
            0
        } else {
            self.rewrite_generations
        };
        right.probation_ops_remaining = left.probation_ops_remaining;

        right.next = self.next;

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
        assert!(other.merging_child.is_none());

        // we can shortcut the normal merge process
        // when the right sibling can be merged into
        // our node without considering keys or values
        let can_seamlessly_absorb = self.fixed_key_stride.is_some()
            && self.fixed_key_stride == other.fixed_key_stride
            && self.fixed_value_length() == Some(0)
            && other.fixed_value_length() == Some(0)
            && self.fixed_key_length == other.fixed_key_length
            && self.lo().len() == other.lo().len()
            && self.hi().map(|h| h.len()) == other.hi().map(|h| h.len());

        if can_seamlessly_absorb {
            let mut ret = self.clone();
            if let Some(other_hi) = other.hi() {
                ret.hi_mut().unwrap().copy_from_slice(other_hi);
            }
            ret.children = self.children.checked_add(other.children).unwrap();
            ret.next = other.next;
            ret.rewrite_generations =
                self.rewrite_generations.max(other.rewrite_generations);
            return ret;
        }

        let prefix_len = if let Some(right_hi) = other.hi() {
            #[cfg(test)]
            use rand::Rng;

            // prefix encoded length can only grow or stay the same
            // during splits
            #[cfg(test)]
            let test_jitter = rand::thread_rng().gen_range(0, 16);

            #[cfg(not(test))]
            let test_jitter = u8::MAX as usize;

            self.lo()
                .iter()
                .zip(right_hi)
                .take(u8::MAX as usize)
                .take_while(|(a, b)| a == b)
                .count()
                .min(test_jitter)
        } else {
            0
        };

        let extended_left: Vec<_>;
        let extended_right: Vec<_>;
        let items: Vec<_> = if self.prefix_len as usize == prefix_len
            && other.prefix_len as usize == prefix_len
        {
            self.iter().chain(other.iter()).collect()
        } else {
            extended_left = self
                .iter_keys()
                .map(|k| {
                    prefix::reencode(self.prefix(), &IVec::from(k), prefix_len)
                })
                .collect();

            let left_iter = extended_left
                .iter()
                .map(|k| KeyRef::Slice(k.as_ref()))
                .zip(self.iter_values());

            extended_right = other
                .iter_keys()
                .map(|k| {
                    prefix::reencode(other.prefix(), &IVec::from(k), prefix_len)
                })
                .collect();

            let right_iter = extended_right
                .iter()
                .map(|k| KeyRef::Slice(k.as_ref()))
                .zip(other.iter_values());

            left_iter.chain(right_iter).collect()
        };

        let other_rewrite_generations = other.rewrite_generations;
        let other_next = other.next;

        let mut ret = Inner::new(
            self.lo(),
            other.hi(),
            u8::try_from(prefix_len).unwrap(),
            self.is_index,
            other_next,
            &*items,
        );

        ret.rewrite_generations =
            self.rewrite_generations.max(other_rewrite_generations);

        testing_assert!(ret.is_sorted());

        ret
    }

    fn header(&self) -> &Header {
        assert_eq!(self.ptr() as usize % 8, 0);
        unsafe { &*(self.ptr as *mut u64 as *mut Header) }
    }

    fn header_mut(&mut self) -> &mut Header {
        unsafe { &mut *(self.ptr as *mut Header) }
    }

    fn is_empty(&self) -> bool {
        self.children() == 0
    }

    pub(crate) fn rss(&self) -> u64 {
        self.len() as u64
    }

    fn children(&self) -> usize {
        self.children as usize
    }

    fn contains_key(&self, key: &[u8]) -> bool {
        if key < self.lo()
            || if let Some(hi) = self.hi() { key >= hi } else { false }
        {
            return false;
        }
        if let Some(fixed_key_length) = self.fixed_key_length {
            if usize::from(fixed_key_length.get()) != key.len() {
                return false;
            }
        }
        self.find(key).is_ok()
    }

    fn find(&self, key: &[u8]) -> Result<usize, usize> {
        if let Some(stride) = self.fixed_key_stride {
            // NB this branch must be able to handle
            // keys that are shorter or longer than
            // our fixed key length!
            let base = &self.lo()[self.prefix_len as usize..];

            let s_len = key.len().min(base.len());

            let shared_distance: usize =
                shared_distance(&base[..s_len], &key[..s_len]);

            let mut distance = unshift_distance(shared_distance, base, key);

            if distance % stride.get() as usize == 0 && key.len() < base.len() {
                // searching for [9] resulted in going to [9, 0],
                // this must have 1 subtracted in that case
                distance = distance.checked_sub(1).unwrap();
            }

            let offset = distance / stride.get() as usize;

            if base.len() != key.len()
                || distance % stride.get() as usize != 0
                || offset >= self.children as usize
            {
                // search key does not evenly fit based on
                // our fixed stride length
                log::trace!("failed to find, search: {:?} lo: {:?} \
                    prefix_len: {} distance: {} stride: {} offset: {} children: {}, node: {:?}",
                    key, self.lo(), self.prefix_len, distance,
                    stride.get(), offset, self.children, self
                );
                return Err((offset + 1).min(self.children()));
            }

            log::trace!("found offset in Node::find {}", offset);
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

        if let Some(stride) = self.fixed_key_stride {
            return KeyRef::Computed {
                base: &self.lo()[self.prefix_len as usize..],
                distance: stride.get() as usize * idx,
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

        if let Some(0) = self.fixed_value_length() {
            return &[];
        }

        let buf = self.value_buf_for_offset(idx);

        let (start, end) =
            if let Some(fixed_value_length) = self.fixed_value_length() {
                (0, fixed_value_length)
            } else {
                let (value_len, varint_sz) = varint::deserialize(buf).unwrap();
                let start = varint_sz;
                let end = start + tf!(value_len);
                (start, end)
            };

        &buf[start..end]
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
        if self.fixed_key_stride.is_some() {
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

    pub(super) const fn empty() -> &'static [u8] {
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
    fn keyref_ord_equal_length() {
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
    fn keyref_ord_varied_length() {
        assert!(
            KeyRef::Computed { base: &[0, 200], distance: 201 }
                > KeyRef::Slice(&[1])
        );
        assert!(
            KeyRef::Slice(&[1])
                < KeyRef::Computed { base: &[0, 200], distance: 201 }
        );
        assert!(
            KeyRef::Computed { base: &[2, 0], distance: 0 }
                > KeyRef::Computed { base: &[2], distance: 0 }
        );
        assert!(
            KeyRef::Computed { base: &[2], distance: 0 }
                < KeyRef::Computed { base: &[2, 0], distance: 0 }
        );
        assert!(
            KeyRef::Computed { base: &[0, 2], distance: 0 }
                < KeyRef::Computed { base: &[2], distance: 0 }
        );
        assert!(
            KeyRef::Computed { base: &[2], distance: 0 }
                > KeyRef::Computed { base: &[0, 2], distance: 0 }
        );
        assert!(
            KeyRef::Computed { base: &[2], distance: 0 }
                != KeyRef::Computed { base: &[0, 2], distance: 0 }
        );
        assert!(
            KeyRef::Computed { base: &[2], distance: 0 }
                != KeyRef::Computed { base: &[2, 0], distance: 0 }
        );
        assert!(
            KeyRef::Computed { base: &[1, 0], distance: 0 }
                != KeyRef::Computed { base: &[255], distance: 1 }
        );
        assert!(
            KeyRef::Computed { base: &[0, 0], distance: 0 }
                != KeyRef::Computed { base: &[255], distance: 1 }
        );
    }

    #[test]
    fn compute_distances() {
        let table: &[(&[u8], &[u8], usize)] =
            &[(&[0], &[0], 0), (&[0], &[1], 1), (&[0, 255], &[1, 0], 1)];

        for (a, b, expected) in table {
            assert_eq!(shared_distance(a, b), *expected);
        }
    }

    #[test]
    fn apply_computed_distances() {
        let table: &[(KeyRef<'_>, &[u8])] = &[
            (KeyRef::Computed { base: &[0], distance: 0 }, &[0]),
            (KeyRef::Computed { base: &[0], distance: 1 }, &[1]),
            (KeyRef::Computed { base: &[0, 255], distance: 1 }, &[1, 0]),
            (KeyRef::Computed { base: &[2, 253], distance: 8 }, &[3, 5]),
        ];

        for (key_ref, expected) in table {
            let ivec: IVec = key_ref.into();
            assert_eq!(&ivec, expected)
        }

        let key_ref = KeyRef::Computed { base: &[2, 253], distance: 8 };
        let mut buf = &mut [0, 0][..];
        key_ref.write_into(&mut buf);
        assert_eq!(buf, &[3, 5]);
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
            .filter_map(|(k, v)| {
                if k < &lo {
                    None
                } else {
                    Some((KeyRef::Slice(k.as_ref()), v.as_ref()))
                }
            })
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();

        let ir = Inner::new(
            &lo,
            if hi <= lo { None } else { Some(&hi) },
            0,
            false,
            None,
            &children_ref,
        );

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

    #[test]
    fn node_bug_04() {
        let node = Inner::new(
            &[0, 2, 253],
            Some(&[0, 3, 33]),
            1,
            true,
            None,
            &[
                (
                    KeyRef::Computed { base: &[2, 253], distance: 0 },
                    &620_u64.to_le_bytes(),
                ),
                (
                    KeyRef::Computed { base: &[2, 253], distance: 2 },
                    &665_u64.to_le_bytes(),
                ),
                (
                    KeyRef::Computed { base: &[2, 253], distance: 4 },
                    &683_u64.to_le_bytes(),
                ),
                (
                    KeyRef::Computed { base: &[2, 253], distance: 6 },
                    &713_u64.to_le_bytes(),
                ),
            ],
        );

        Node { inner: Arc::new(node), overlay: Default::default() }.split();
    }

    #[test]
    fn node_bug_05() {
        // postmortem: `prop_indexable` did not account for the requirement
        // of feeding sorted items that are >= the lo key to the Node::new method.
        assert!(prop_indexable(
            vec![1],
            vec![],
            vec![(vec![], vec![]), (vec![0], vec![])],
        ))
    }
}
