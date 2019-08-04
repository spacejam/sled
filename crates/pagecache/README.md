# pagecache

[![Build Status](https://travis-ci.org/spacejam/sled.svg?branch=master)](https://travis-ci.org/spacejam/sled)
[![crates.io](https://meritbadge.herokuapp.com/pagecache)](https://crates.io/crates/pagecache)
[![documentation](https://docs.rs/pagecache/badge.svg)](https://docs.rs/pagecache)

A construction kit for databases. Provides a lock-free log store and pagecache.

# References

* [LLAMA: A Cache/Storage Subsystem for Modern Hardware](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/llama-vldb2013.pdf)
* [The Design and Implementation of a Log-Structured File System](https://people.eecs.berkeley.edu/~brewer/cs262/LFS.pdf)

```rust
use {
    pagecache::{pin, Materializer, Config},
    serde::{Serialize, Deserialize},
};


#[derive(
    Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize,
)]
pub struct TestState(String);

impl Materializer for TestState {
    // Used to merge chains of partial pages into a form
    // that is useful for the `PageCache` owner.
    fn merge(&mut self, other: &TestState) {
        self.0.push_str(&other.0);
    }
}

fn main() {
    let config = pagecache::ConfigBuilder::new().temporary(true).build();
    let pc: pagecache::PageCache<TestState> =
        pagecache::PageCache::start(config).unwrap();
    {
        // We begin by initiating a new transaction, which
        // will prevent any witnessable memory from being
        // reclaimed before we drop this object.
        let tx = pc.begin().unwrap();

        // The first item in a page should be set using allocate,
        // which signals that this is the beginning of a new
        // page history.
        let (id, mut key) = pc.allocate(TestState("a".to_owned()), &tx).unwrap();

        // Subsequent atomic updates should be added with link.
        key = pc.link(id, key, TestState("b".to_owned()), &tx).unwrap().unwrap();
        key = pc.link(id, key, TestState("c".to_owned()), &tx).unwrap().unwrap();

        // When getting a page, the provided `Materializer` is
        // used to merge all pages together.
        let (mut key, page, size_on_disk) = pc.get(id, &tx).unwrap().unwrap();

        assert_eq!(page.0, "abc".to_owned());

        // You can completely rewrite a page by using `replace`:
        key = pc.replace(id, key, TestState("d".into()), &tx).unwrap().unwrap();

        let (key, page, size_on_disk) = pc.get(id, &tx).unwrap().unwrap();

         assert_eq!(page.0, "d".to_owned());
    }
}
```
