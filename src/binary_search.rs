pub(crate) fn interpolation_search_lub<'a, T>(
    key: &[u8],
    s: &'a [(IVec, T)],
) -> Option<usize> {
    match interpolation_search(key, s) {
        Ok(i) => Some(i),
        Err(i) if i == 0 => None,
        Err(i) => Some(i - 1),
    }
}

use crate::IVec;
use std::cmp::Ordering::*;

pub fn interpolation_search<'a, T>(
    key: &[u8],
    s: &'a [(IVec, T)],
) -> Result<usize, usize> {
    let mut size = s.len();
    if size == 0 || key < &*s[0].0 {
        return Err(0);
    }
    let mut base = 0usize;
    let mut iteration = 0;
    while size > 1 {
        let half = if iteration < 2 {
            interpolate(key, &s[base].0, &s[size - 1].0, size - 1)
        } else {
            // we switch to binary search after 2 iterations
            // to avoid worst case interp behavior
            size / 2
        };
        let mid = base + half;
        // mid is always in [0, size), that means mid is >= 0 and < size.
        // mid >= 0: by definition
        // mid < size: mid = size / 2 + size / 4 + size / 8 ...
        #[allow(unsafe_code)]
        let cmp = unsafe { s.get_unchecked(mid).0.as_ref().cmp(key) };
        base = if cmp == Greater { base } else { mid };
        size -= half;
        iteration += 1;
    }
    // base is always in [0, size) because base <= mid.
    #[allow(unsafe_code)]
    let cmp = unsafe { s.get_unchecked(base).0.as_ref().cmp(key) };
    if cmp == Equal {
        Ok(base)
    } else {
        Err(base + (cmp == Less) as usize)
    }
}

fn interpolate(search: &[u8], min: &[u8], max: &[u8], size: usize) -> usize {
    assert!(search >= min);
    const SZ: usize = std::mem::size_of::<usize>();

    let common_len =
        std::cmp::min(search.len(), std::cmp::min(min.len(), max.len()));
    let common_base: usize = common_len.saturating_sub(SZ);
    let width = common_len - common_base;
    let common_range = common_base..common_len;

    let mut searches = [0_u8; SZ];
    searches[..width].copy_from_slice(&search[common_range.clone()]);

    let mut mins = [0_u8; SZ];
    mins[..width].copy_from_slice(&min[common_range.clone()]);

    let mut maxes = [0_u8; SZ];
    maxes[..width].copy_from_slice(&max[common_range]);

    let search_usize = usize::from_be_bytes(searches);
    let min_usize = usize::from_be_bytes(mins);
    let max_usize = usize::from_be_bytes(maxes);

    let range = max_usize - min_usize;
    let offset = search_usize - min_usize;

    let prop = range as f64 / size as f64;

    // float protection
    std::cmp::min(size, (prop * offset as f64) as usize)
}

#[test]
fn test_interpolation_search_lub() {
    let s = vec![
        (vec![4].into(), ()),
        (vec![5].into(), ()),
        (vec![5].into(), ()),
        (vec![6].into(), ()),
        (vec![9].into(), ()),
    ];
    assert_eq!(interpolation_search_lub(&[3], &*s), None);
    assert_eq!(interpolation_search_lub(&[4], &*s), Some(0));
    assert_eq!(interpolation_search_lub(&[5], &*s), Some(2));
    assert_eq!(interpolation_search_lub(&[6], &*s), Some(3));
    assert_eq!(interpolation_search_lub(&[7], &*s), Some(3));
    assert_eq!(interpolation_search_lub(&[8], &*s), Some(3));
    assert_eq!(interpolation_search_lub(&[9], &*s), Some(4));
    assert_eq!(interpolation_search_lub(&[10], &*s), Some(4));

    let mut s = s;
    s.clear();
    assert_eq!(interpolation_search_lub(&[8], &*s), None);
}
