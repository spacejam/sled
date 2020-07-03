//! Merge operators are an extremely powerful tool for use in embedded kv
//! stores. They allow users to specify custom logic for combining multiple
//! versions of a value into one.
//!
//! As a motivating example, imagine that you have a counter. In a traditional
//! kv store, you would need to read the old value, modify it, then write it
//! back (RMW). If you want to increment the counter from multiple threads, you
//! would need to either use higher-level locking or you need to spin in a CAS
//! loop until your increment is successful. Merge operators remove the need for
//! all of this by allowing multiple threads to "merge" in the desired
//! operation, rather than performing a read, then modification, then later
//! writing. `+1 -> +1 -> +1` instead of `w(r(key) + 1) -> w(r(key)+ 1) ->
//! w(r(key) + 1)`.
//!
//! Here's an example of using a merge operator to just concatenate merged bytes
//! together. Note that calling `set` acts as a value replacement, bypassing the
//! merging logic and replacing previously merged values. Calling `merge` is
//! like `set` but when the key is fetched, it will use the merge operator to
//! combine all `merge`'s since the last `set`.
//!
//! ```rust
//! fn concatenate_merge(
//! _key: &[u8],               // the key being merged
//! old_value: Option<&[u8]>,  // the previous value, if one existed
//! merged_bytes: &[u8]        // the new bytes being merged in
//! ) -> Option<Vec<u8>> {       // set the new value, return None to delete
//! let mut ret = old_value
//!     .map(|ov| ov.to_vec())
//!     .unwrap_or_else(|| vec![]);
//!
//! ret.extend_from_slice(merged_bytes);
//!
//! Some(ret)
//! }
//!
//! let config = ConfigBuilder::new()
//! .temporary(true)
//! .build();
//!
//! let tree = Tree::start(config).unwrap();
//! tree.set_merge_operator(concatenate_merge);
//!
//! tree.set(k, vec![0]);
//! tree.merge(k, vec![1]);
//! tree.merge(k, vec![2]);
//! assert_eq!(tree.get(&k), Ok(Some(vec![0, 1, 2])));
//!
//! // sets replace previously merged data,
//! // bypassing the merge function.
//! tree.set(k, vec![3]);
//! assert_eq!(tree.get(&k), Ok(Some(vec![3])));
//!
//! // merges on non-present values will add them
//! tree.del(&k);
//! tree.merge(k, vec![4]);
//! assert_eq!(tree.get(&k), Ok(Some(vec![4])));
//! ```
//!
//! ### beyond the basics
//!
//! Merge operators can be used to express arbitrarily complex logic. You can
//! use them to implement any sort of high-level data structure on top of sled,
//! using merges of different values to represent your desired operations.
//! Similar to the above example, you could implement a list that lets you push
//! items. Bloom filters are particularly easy to implement, and merge operators
//! also are quite handy for building persistent CRDTs.
//!
//! ### warnings
//!
//! If you call `merge` without setting a merge operator, an error will be
//! returned. Merge operators may be changed over time, but make sure you do
//! this carefully to avoid race conditions. If you need to push a one-time
//! operation to a value, use `update_and_fetch` or `fetch_and_update` instead.
