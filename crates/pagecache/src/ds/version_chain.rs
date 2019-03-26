use super::*;

#[derive(Default)]
pub(crate) struct VersionChain<T> {
    head: Atomic<VersionSegment<T>>,
}

impl<T> VersionChain<T> {
    pub(crate) fn install() {}
    pub(crate) fn bump_rts() {}
    pub(crate) fn abort() {}
    pub(crate) fn commit() {}
}

struct VersionSegment<T> {
    versions: [Atomic<Version<T>>; 32],
    next: Atomic<VersionSegment<T>>,
}

#[derive(Default)]
struct Version<T> {
    inner: T,
    wts: u64,
    max_rts: AtomicUsize,
}
