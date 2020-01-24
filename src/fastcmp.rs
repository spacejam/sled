use std::cmp::Ordering;

#[cfg(any(unix, windows))]
#[allow(unsafe_code)]
pub(crate) fn fastcmp(l: &[u8], r: &[u8]) -> Ordering {
    let len = std::cmp::min(l.len(), r.len());
    let cmp = unsafe { libc::memcmp(l.as_ptr() as _, r.as_ptr() as _, len) };
    match cmp {
        a if a > 0 => Ordering::Greater,
        a if a < 0 => Ordering::Less,
        _ => l.len().cmp(&r.len()),
    }
}

#[cfg(not(any(unix, windows)))]
#[allow(unsafe_code)]
pub(crate) fn fastcmp(l: &[u8], r: &[u8]) -> Ordering {
    l.cmp(r)
}

#[cfg(test)]
mod qc {
    use super::fastcmp;

    fn prop_cmp_matches(l: &[u8], r: &[u8]) -> bool {
        assert_eq!(fastcmp(l, r), l.cmp(r));
        assert_eq!(fastcmp(r, l), r.cmp(l));
        assert_eq!(fastcmp(l, l), l.cmp(l));
        assert_eq!(fastcmp(r, r), r.cmp(r));
        true
    }

    #[test]
    fn test_fastcmp() {
        let cases: [&[u8]; 8] = [
            &[],
            &[0],
            &[1],
            &[1],
            &[255],
            &[1, 2, 3],
            &[1, 2, 3, 0],
            &[1, 2, 3, 55],
        ];
        for pair in cases.windows(2) {
            prop_cmp_matches(pair[0], pair[1]);
        }
    }

    quickcheck::quickcheck! {
        #[cfg_attr(miri, ignore)]
        fn qc_fastcmp(l: Vec<u8>, r: Vec<u8>) -> bool {
            prop_cmp_matches(&l, &r)
        }
    }
}
