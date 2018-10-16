use std::cmp::PartialEq;
use std::convert::AsRef;
use std::fmt::{self, Debug};
use std::ops::Deref;

use super::*;

/// A reference to a heap location that is
/// guaranteed to be valid for as long as this
/// value exists.
pub struct PinnedValue(*const u8, usize, Guard);

impl Deref for PinnedValue {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.0, self.1) }
    }
}

impl PartialEq for PinnedValue {
    fn eq(&self, other: &PinnedValue) -> bool {
        use std::ops::Deref;
        self.deref() == other.deref()
    }
}

impl PartialEq<[u8]> for PinnedValue {
    fn eq(&self, other: &[u8]) -> bool {
        self.deref() == other
    }
}

impl<'a> PartialEq<&'a [u8]> for PinnedValue {
    fn eq(&self, other: &&[u8]) -> bool {
        self.deref() == *other
    }
}

impl PartialEq<Value> for PinnedValue {
    fn eq(&self, other: &Value) -> bool {
        use std::ops::Deref;
        self.deref() == &**other
    }
}

impl AsRef<[u8]> for PinnedValue {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl Debug for PinnedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PinnedValue( {:?}, Guard )", self.deref())
    }
}

impl PinnedValue {
    pub(crate) fn new(v: &[u8], guard: Guard) -> PinnedValue {
        let ptr = v.as_ptr();
        PinnedValue(ptr, v.len(), guard)
    }
}
