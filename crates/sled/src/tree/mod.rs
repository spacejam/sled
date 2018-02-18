use super::*;

mod bound;
mod data;
mod frag;
mod node;
mod iter;
mod materializer;
mod tree;

pub use self::bound::Bound;
pub use self::frag::{ChildSplit, Frag, ParentSplit};
pub use self::data::Data;
pub use self::node::Node;
pub use self::iter::Iter;
pub use self::materializer::BLinkMaterializer;
pub use self::tree::Tree;

fn prefix_encode(prefix: &[u8], buf: &[u8]) -> Vec<u8> {
    let limit = std::cmp::min(std::u8::MAX as usize, buf.len());
    let mut prefix_len = 0usize;
    for (i, c) in prefix.iter().take(limit).enumerate() {
        if buf[i] == *c {
            prefix_len += 1;
        } else {
            break;
        }
    }
    let mut ret = Vec::with_capacity(1 + buf.len() - prefix_len);
    unsafe {
        ret.set_len(1 + buf.len() - prefix_len);
    }
    ret[1..].copy_from_slice(&buf[prefix_len..]);
    ret[0] = prefix_len as u8;
    ret
}

fn prefix_decode(prefix: &[u8], buf: &[u8]) -> Vec<u8> {
    assert!(buf.len() >= 1);
    let prefix_len = buf[0] as usize;
    let mut ret = Vec::with_capacity(prefix_len + buf.len() - 1);
    unsafe {
        ret.set_len(prefix_len + buf.len() - 1);
    }
    ret[0..prefix_len].copy_from_slice(&prefix[0..prefix_len]);
    ret[prefix_len..].copy_from_slice(&buf[1..]);
    ret
}

#[test]
fn test_prefix() {
    let prefix = b"cat";
    assert_eq!(prefix_encode(prefix, prefix), vec![prefix.len() as u8]);
    assert_eq!(prefix_encode(prefix, b"ca"), vec![2]);
    assert_eq!(prefix_encode(prefix, b"cab"), vec![2, b'b']);
    assert_eq!(prefix_encode(prefix, b"cvar"), vec![1, b'v', b'a', b'r']);
    assert_eq!(prefix_encode(prefix, b"zig"), vec![0, b'z', b'i', b'g']);

    let rtt = vec![b"" as &[u8], b"\x00cat", b"\x00", b"oyunwytounw\x00"];
    for item in rtt {
        assert_eq!(
            prefix_decode(prefix, &*prefix_encode(prefix, item)),
            item.to_vec()
        );
    }
}
