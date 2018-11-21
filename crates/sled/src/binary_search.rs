use std::cmp::Ordering::{self, Greater, Less};

// Adapted from the standard library's binary_search_by
#[inline]
pub(crate) fn binary_search_gt<'a, T, F>(
    slice: &'a [T],
    mut f: F,
) -> Option<usize>
where
    F: FnMut(&'a T) -> Ordering,
{
    let s = slice;
    let mut size = s.len();
    if size == 0 {
        return None;
    }
    let mut base = size - 1;
    while size > 1 {
        let half = size / 2;
        let mid = base - half;
        // mid is always in [0, size), that means mid is >= 0 and < size.
        // mid >= 0: by definition
        // mid < size: mid = size / 2 + size / 4 + size / 8 ...
        let cmp = f(unsafe { s.get_unchecked(mid) });
        base = if cmp == Greater { mid } else { base };
        size -= half;
    }
    // base is always in [0, size) because base <= mid.
    let cmp = f(unsafe { s.get_unchecked(base) });
    if cmp == Greater {
        Some(base)
    } else {
        None
    }
}

// Adapted from the standard library's binary_search_by
#[inline]
pub(crate) fn binary_search_lt<'a, T, F>(
    slice: &'a [T],
    mut f: F,
) -> Option<usize>
where
    F: FnMut(&'a T) -> Ordering,
{
    let s = slice;
    let mut size = s.len();
    if size == 0 {
        return None;
    }
    let mut base = 0usize;
    while size > 1 {
        let half = size / 2;
        let mid = base + half;
        // mid is always in [0, size), that means mid is >= 0 and < size.
        // mid >= 0: by definition
        // mid < size: mid = size / 2 + size / 4 + size / 8 ...
        let cmp = f(unsafe { s.get_unchecked(mid) });
        base = if cmp == Less { mid } else { base };
        size -= half;
    }
    // base is always in [0, size) because base <= mid.
    let cmp = f(unsafe { s.get_unchecked(base) });
    if cmp == Less {
        Some(base)
    } else {
        None
    }
}

#[inline]
pub(crate) fn binary_search_lub<'a, T, F>(
    s: &'a [T],
    f: F,
) -> Option<usize>
where
    F: FnMut(&'a T) -> ::std::cmp::Ordering,
{
    match s.binary_search_by(f) {
        Ok(i) => Some(i),
        Err(i) if i == 0 => None,
        Err(i) => Some(i - 1),
    }
}

#[test]
fn test_binary_search_lub() {
    let s = &*vec![4, 5, 5, 6, 9];
    assert_eq!(binary_search_lub(s, |e| e.cmp(&3)), None);
    assert_eq!(binary_search_lub(s, |e| e.cmp(&4)), Some(0));
    assert_eq!(binary_search_lub(s, |e| e.cmp(&5)), Some(2));
    assert_eq!(binary_search_lub(s, |e| e.cmp(&6)), Some(3));
    assert_eq!(binary_search_lub(s, |e| e.cmp(&7)), Some(3));
    assert_eq!(binary_search_lub(s, |e| e.cmp(&8)), Some(3));
    assert_eq!(binary_search_lub(s, |e| e.cmp(&9)), Some(4));
    assert_eq!(binary_search_lub(s, |e| e.cmp(&10)), Some(4));

    let s: &[u8] = &*vec![];
    assert_eq!(binary_search_lub(s, |e| e.cmp(&8)), None);
}

#[test]
fn test_binary_search_gt() {
    let s = &*vec![4, 5, 5, 6, 9];
    assert_eq!(binary_search_gt(s, |e| e.cmp(&3)), Some(0));
    assert_eq!(binary_search_gt(s, |e| e.cmp(&4)), Some(1));
    assert_eq!(binary_search_gt(s, |e| e.cmp(&5)), Some(3));
    assert_eq!(binary_search_gt(s, |e| e.cmp(&6)), Some(4));
    assert_eq!(binary_search_gt(s, |e| e.cmp(&7)), Some(4));
    assert_eq!(binary_search_gt(s, |e| e.cmp(&8)), Some(4));
    assert_eq!(binary_search_gt(s, |e| e.cmp(&9)), None);
    assert_eq!(binary_search_gt(s, |e| e.cmp(&10)), None);

    let s: &[u8] = &*vec![];
    assert_eq!(binary_search_gt(s, |e| e.cmp(&8)), None);
}

#[test]
fn test_binary_search_lt() {
    let s = &*vec![4, 5, 5, 6, 9];
    assert_eq!(binary_search_lt(s, |e| e.cmp(&3)), None);
    assert_eq!(binary_search_lt(s, |e| e.cmp(&4)), None);
    assert_eq!(binary_search_lt(s, |e| e.cmp(&5)), Some(0));
    assert_eq!(binary_search_lt(s, |e| e.cmp(&6)), Some(2));
    assert_eq!(binary_search_lt(s, |e| e.cmp(&7)), Some(3));
    assert_eq!(binary_search_lt(s, |e| e.cmp(&8)), Some(3));
    assert_eq!(binary_search_lt(s, |e| e.cmp(&9)), Some(3));
    assert_eq!(binary_search_lt(s, |e| e.cmp(&10)), Some(4));

    let s: &[u8] = &*vec![];
    assert_eq!(binary_search_lt(s, |e| e.cmp(&8)), None);
}
