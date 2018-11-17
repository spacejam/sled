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

#[inline]
pub(crate) fn binary_search_pred<'a, T, F>(
    s: &'a [T],
    f: F,
) -> Option<usize>
where
    F: FnMut(&'a T) -> ::std::cmp::Ordering,
{
    match s.binary_search_by(f) {
        Ok(i) | Err(i) if i == 0 => None,
        Ok(i) | Err(i) => Some(i - 1),
    }
}

#[test]
fn test_binary_search_lub() {
    let s = &*vec![4, 5, 6, 9];
    assert_eq!(binary_search_lub(s, |e| e.cmp(&1)), None);
    assert_eq!(binary_search_lub(s, |e| e.cmp(&4)), Some(0));
    assert_eq!(binary_search_lub(s, |e| e.cmp(&5)), Some(1));
    assert_eq!(binary_search_lub(s, |e| e.cmp(&7)), Some(2));
    assert_eq!(binary_search_lub(s, |e| e.cmp(&10)), Some(3));

    let s: &[u8] = &*vec![];
    assert_eq!(binary_search_lub(s, |e| e.cmp(&8)), None);
}

#[test]
fn test_binary_search_pred() {
    let s = &*vec![4, 5, 6, 9];
    assert_eq!(binary_search_pred(s, |e| e.cmp(&1)), None);
    assert_eq!(binary_search_pred(s, |e| e.cmp(&4)), None);
    assert_eq!(binary_search_pred(s, |e| e.cmp(&5)), Some(0));
    assert_eq!(binary_search_pred(s, |e| e.cmp(&7)), Some(2));
    assert_eq!(binary_search_pred(s, |e| e.cmp(&10)), Some(3));

    let s: &[u8] = &*vec![];
    assert_eq!(binary_search_pred(s, |e| e.cmp(&8)), None);
}
