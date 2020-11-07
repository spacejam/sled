use std::cmp::Ordering::{Equal, Greater, Less};

use crate::{fastcmp, IVec};

pub(crate) fn binary_search_lub(key: &[u8], s: &[IVec]) -> Option<usize> {
    match binary_search(key, s) {
        Ok(i) => Some(i),
        Err(i) if i == 0 => None,
        Err(i) => Some(i - 1),
    }
}

pub fn binary_search(key: &[u8], s: &[IVec]) -> Result<usize, usize> {
    let mut size = s.len();
    if size == 0 || *key < *s[0] {
        return Err(0);
    }
    let mut base = 0_usize;
    while size > 1 {
        let half = size / 2;
        let mid = base + half;
        // mid is always in [0, size), that means mid is >= 0 and < size.
        // mid >= 0: by definition
        // mid < size: mid = size / 2 + size / 4 + size / 8 ...
        #[allow(unsafe_code)]
        let l = unsafe { s.get_unchecked(mid).as_ref() };
        let cmp = fastcmp(l, key);
        base = if cmp == Greater { base } else { mid };
        size -= half;
    }
    // base is always in [0, size) because base <= mid.
    #[allow(unsafe_code)]
    let l = unsafe { s.get_unchecked(base).as_ref() };
    let cmp = fastcmp(l, key);
    if cmp == Equal {
        Ok(base)
    } else {
        Err(base + (cmp == Less) as usize)
    }
}

#[test]
fn test_binary_search_lub() {
    let s = vec![
        vec![4].into(),
        vec![5].into(),
        vec![5].into(),
        vec![6].into(),
        vec![9].into(),
    ];
    assert_eq!(binary_search_lub(&[3], &*s), None);
    assert_eq!(binary_search_lub(&[4], &*s), Some(0));
    assert_eq!(binary_search_lub(&[5], &*s), Some(2));
    assert_eq!(binary_search_lub(&[6], &*s), Some(3));
    assert_eq!(binary_search_lub(&[7], &*s), Some(3));
    assert_eq!(binary_search_lub(&[8], &*s), Some(3));
    assert_eq!(binary_search_lub(&[9], &*s), Some(4));
    assert_eq!(binary_search_lub(&[10], &*s), Some(4));

    let mut s = s;
    s.clear();
    assert_eq!(binary_search_lub(&[8], &*s), None);
}
