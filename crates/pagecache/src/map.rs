use {
    fxhash::{FxHasher, FxHasher32, FxHasher64},
    hashbrown::{HashMap, HashSet},
    std::hash::BuildHasherDefault,
};

/// A fast map that is not resistant to collision attacks. Works
/// on one byte at a time.
pub type FastMap1<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

/// A fast map that is not resistant to collision attacks. Works
/// on 4 bytes at a time.
pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;

/// A fast map that is not resistant to collision attacks. Works
/// on 8 bytes at a time.
pub type FastMap8<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher64>>;

/// A fast set that is not resistant to collision attacks. Works
/// on one byte at a time.
pub type FastSet1<V> = HashSet<V, BuildHasherDefault<FxHasher>>;

/// A fast set that is not resistant to collision attacks. Works
/// on 4 bytes at a time.
pub type FastSet4<V> = HashSet<V, BuildHasherDefault<FxHasher32>>;

/// A fast set that is not resistant to collision attacks. Works
/// on 8 bytes at a time.
pub type FastSet8<V> = HashSet<V, BuildHasherDefault<FxHasher64>>;
