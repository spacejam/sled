use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem;
use std::ptr;
use std::sync::atomic::{
    AtomicUsize, Ordering::SeqCst, ATOMIC_USIZE_INIT,
};

const N_LOCKS: usize = 1 << 16;

struct HashLord {
    ts: AtomicUsize,
    locks: [AtomicUsize; N_LOCKS],
}

impl HashLord {
    fn new(from_ts: usize) -> HashLord {
        let mut locks: [AtomicUsize; N_LOCKS];
        unsafe {
            locks = mem::uninitialized();
            for elem in &mut locks[..] {
                ptr::write(elem, ATOMIC_USIZE_INIT);
            }
        }
        HashLord {
            ts: AtomicUsize::new(from_ts),
            locks: locks,
        }
    }

    fn with_ts<B, F>(&mut self, k: NodeID, mut f: F) -> Option<B>
    where
        F: FnMut(&mut Node) -> B,
    {
        self.nodes.get_mut(&k).map(|mut node| {
            node.meta.bump_mtime();
            f(&mut node)
        })
    }
}

fn hash<T: Hash>(t: &T) -> u16 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish() as u16
}

#[test]
fn f1() {}
