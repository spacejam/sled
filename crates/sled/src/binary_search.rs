#[inline]
pub(crate) fn binary_search_lub<'a, T, F>(s: &'a [T], f: F) -> Option<usize>
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
