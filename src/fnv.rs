// extracted from the fnv crate for minor, mostly
// compile-time optimizations. fnv and sled are
// both licensed as Apache 2.0 OR MIT.
#[allow(missing_copy_implementations)]
pub struct Hasher(u64);

impl Default for Hasher {
    #[inline]
    fn default() -> Hasher {
        Hasher(0xcbf29ce484222325)
    }
}

impl std::hash::Hasher for Hasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    #[allow(clippy::cast_lossless)]
    fn write(&mut self, bytes: &[u8]) {
        let Hasher(mut hash) = *self;

        for byte in bytes.iter() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }

        *self = Hasher(hash);
    }
}
