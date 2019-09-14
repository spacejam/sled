use {
    fxhash::FxHasher64,
    std::{
        collections::{HashMap, HashSet},
        hash::BuildHasherDefault,
    },
};

/// A fast map that is not resistant to collision attacks. Works
/// on 8 bytes at a time.
pub type FastMap8<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher64>>;

/// A fast set that is not resistant to collision attacks. Works
/// on 8 bytes at a time.
pub type FastSet8<V> = HashSet<V, BuildHasherDefault<FxHasher64>>;
