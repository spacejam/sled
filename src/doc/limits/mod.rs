//! This page documents some limitations that sled imposes on users.
//!
//! * The underlying pagecache can currently store 2^36 pages. Leaf nodes in the
//!   `Tree` tend to split when they have more than 16 keys and values. This
//!   means that sled can hold a little less than **4,294,967,296 total items**
//!   (index nodes in the tree will also consume pages, but ideally far fewer
//!   than 1%). This is easy to increase without requiring migration, as it is
//!   entirely a runtime concern, but nobody has expressed any interest in this
//!   being larger yet. Note to future folks who need to increase this: increase
//!   the width of the Node1 type in the pagetable module, and correspondingly
//!   increase the number of bits that are used to index into it. It's just a
//!   simple wait-free grow-only 2-level pagetable.
//! * keys and values use `usize` for the length fields due to the way that Rust
//!   uses `usize` for slice lengths, and will be limited to the target
//!   platform's pointer width. On 64-bit machines, this will be 64 bits. On
//!   32-bit machines, it will be limited to `u32::max_value()`.
//! * Due to the 32-bit limitation on slice sizes on 32-bit architectures, we
//!   currently do not support systems large enough for the snapshot file to
//!   reach over 4gb. The snapshot file tends to be a small fraction of the
//!   total db size, and it's likely we'll be able to implement a streaming
//!   deserializer if this ever becomes an issue, but it seems unclear if anyone
//!   will encounter this limitation.
