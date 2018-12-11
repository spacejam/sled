use std::cmp::Ordering;

pub(crate) fn prefix_encode(prefix: &[u8], buf: &[u8]) -> Vec<u8> {
    assert!(
        prefix <= buf,
        "prefix {:?} must be lexicographically <= to the encoded buf {:?}",
        prefix,
        buf
    );
    let limit = std::cmp::min(std::u8::MAX as usize, buf.len());
    let mut prefix_len = 0usize;
    for (i, c) in prefix.iter().take(limit).enumerate() {
        if buf[i] == *c {
            prefix_len += 1;
        } else {
            break;
        }
    }

    let encoded_len = 1 + buf.len() - prefix_len;
    let mut ret = Vec::with_capacity(encoded_len);
    unsafe {
        ret.set_len(encoded_len);
    }
    ret[1..].copy_from_slice(&buf[prefix_len..]);
    ret[0] = prefix_len as u8;
    ret
}

pub(crate) fn prefix_decode(prefix: &[u8], buf: &[u8]) -> Vec<u8> {
    assert!(!buf.is_empty());
    let prefix_len = buf[0] as usize;
    let mut ret = Vec::with_capacity(prefix_len + buf.len() - 1);
    unsafe {
        ret.set_len(prefix_len + buf.len() - 1);
    }
    ret[0..prefix_len].copy_from_slice(&prefix[0..prefix_len]);
    ret[prefix_len..].copy_from_slice(&buf[1..]);
    ret
}

// NB: the correctness of this function depends on
// the invariant that the prefix is ALWAYS lexicographically
// Less than or Equal to the keys that have been encoded
// using it. Otherwise this comparison would make no sense.
pub(crate) fn prefix_cmp(a: &[u8], b: &[u8]) -> Ordering {
    if a.is_empty() && b.is_empty() {
        return Ordering::Equal;
    } else if a.is_empty() && !b.is_empty() {
        return Ordering::Less;
    } else if !a.is_empty() && b.is_empty() {
        return Ordering::Greater;
    }

    if a[0] > b[0] {
        Ordering::Less
    } else if a[0] < b[0] {
        Ordering::Greater
    } else {
        a[1..].cmp(&b[1..])
    }
}

/// Compare `a` and `b`, assuming that `a` is prefix encoded and `b` is not.
pub(crate) fn prefix_cmp_encoded(
    a: &[u8],
    mut b: &[u8],
    mut prefix: &[u8],
) -> Ordering {
    assert!(!a.is_empty() && a[0] as usize <= prefix.len());

    let mut a_prefix_len = a[0];
    let a_suffix = &a[1..];

    while a_prefix_len > 0 {
        if b.is_empty() || prefix[0] > b[0] {
            return Ordering::Greater;
        } else if prefix[0] < b[0] {
            return Ordering::Less;
        }

        a_prefix_len -= 1;
        b = &b[1..];
        prefix = &prefix[1..];
    }

    a_suffix.cmp(b)
}

#[test]
fn test_prefix() {
    let prefix = b"cat";
    assert_eq!(
        prefix_encode(prefix, prefix),
        vec![prefix.len() as u8]
    );
    assert_eq!(prefix_encode(prefix, b"catt"), vec![3, b't']);
    assert_eq!(prefix_encode(prefix, b"cb"), vec![1, b'b']);
    assert_eq!(prefix_encode(prefix, b"caz"), vec![2, b'z']);
    assert_eq!(
        prefix_encode(prefix, b"cvar"),
        vec![1, b'v', b'a', b'r']
    );
    assert_eq!(
        prefix_encode(prefix, b"zig"),
        vec![0, b'z', b'i', b'g']
    );

    let prefix = b"";
    assert_eq!(
        prefix_encode(prefix, prefix),
        vec![prefix.len() as u8]
    );
    assert_eq!(prefix_encode(prefix, b"ca"), vec![0, b'c', b'a']);
    assert_eq!(
        prefix_encode(prefix, b"cat"),
        vec![0, b'c', b'a', b't']
    );
    assert_eq!(
        prefix_encode(prefix, b"cab"),
        vec![0, b'c', b'a', b'b']
    );
    assert_eq!(
        prefix_encode(prefix, b"cvar"),
        vec![0, b'c', b'v', b'a', b'r']
    );
    assert_eq!(
        prefix_encode(prefix, b"zig"),
        vec![0, b'z', b'i', b'g']
    );

    let rtt =
        vec![b"" as &[u8], b"\x00cat", b"\x00", b"oyunwytounw\x00"];
    for item in rtt {
        assert_eq!(
            prefix_decode(prefix, &*prefix_encode(prefix, item)),
            item.to_vec()
        );
    }
}

#[test]
fn test_prefix_cmp() {
    assert_eq!(prefix_cmp(&[], &[]), Ordering::Equal);
    assert_eq!(prefix_cmp(&[0], &[]), Ordering::Greater);
    assert_eq!(prefix_cmp(&[], &[0]), Ordering::Less);

    assert_eq!(prefix_cmp(&[3], &[4]), Ordering::Greater);
    assert_eq!(prefix_cmp(&[4, 3], &[3, 4]), Ordering::Less);

    assert_eq!(prefix_cmp(&[1], &[1]), Ordering::Equal);
    assert_eq!(prefix_cmp(&[1, 1], &[1, 1]), Ordering::Equal);
    assert_eq!(prefix_cmp(&[1, 3], &[1, 1]), Ordering::Greater);
    assert_eq!(prefix_cmp(&[1, 1], &[1, 3]), Ordering::Less);
}

#[test]
fn test_prefix_cmp_encoded() {
    fn assert_pce(
        a: &[u8],
        b: &[u8],
        prefix: &[u8],
        expected: Ordering,
    ) {
        assert_eq!(
            prefix_cmp_encoded(a, &prefix_decode(prefix, b), prefix),
            expected
        );
    }

    let prefix = &vec![3, 3, 3, 3];
    assert_pce(&[0], &[0], prefix, Ordering::Equal);
    assert_pce(&[1], &[0], prefix, Ordering::Greater);
    assert_pce(&[0], &[1], prefix, Ordering::Less);

    assert_pce(&[3], &[2], prefix, Ordering::Greater);
    assert_pce(&[4, 3], &[3, 4], prefix, Ordering::Less);

    assert_pce(&[1], &[1], prefix, Ordering::Equal);
    assert_pce(&[1, 3, 3, 1], &[3, 1], prefix, Ordering::Equal);
    assert_pce(&[1, 3], &[1, 1], prefix, Ordering::Greater);
    assert_pce(&[1, 1], &[3, 3], prefix, Ordering::Less);
}
