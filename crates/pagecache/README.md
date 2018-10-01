# pagecache

[![Build Status](https://travis-ci.org/spacejam/sled.svg?branch=master)](https://travis-ci.org/spacejam/sled)
[![crates.io](https://meritbadge.herokuapp.com/pagecache)](https://crates.io/crates/pagecache)
[![documentation](https://docs.rs/pagecache/badge.svg)](https://docs.rs/pagecache)

A construction kit for databases. Provides a lock-free log store and pagecache.

# References

* [LLAMA: A Cache/Storage Subsystem for Modern Hardware](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/llama-vldb2013.pdf)
* [The Design and Implementation of a Log-Structured File System](https://people.eecs.berkeley.edu/~brewer/cs262/LFS.pdf)

```
extern crate pagecache;

use pagecache::{PagePtr, pin, Materializer};

pub struct TestMaterializer;

// A PageCache must be used with a Materializer to
// assist with recovery of custom state, and to
// assemble partial page fragments into the form
// that is usable by the higher-level system.
impl Materializer for TestMaterializer {
    // The possibly fragmented page, written to log storage sequentially, and
    // read in parallel from multiple locations on disk when serving
    // a request to read the page. These will be merged to a single version
    // at read time, and possibly cached.
    type PageFrag = String;

    // The state returned by a call to `PageCache::recover`, as
    // described by `Materializer::recover`
    type Recovery = ();

    // Create a new `Materializer` with the previously recovered
    // state if any existed.
    fn new(last_recovery: &Option<Self::Recovery>) -> Self {
        TestMaterializer
    }

    // Used to merge chains of partial pages into a form
    // that is useful for the `PageCache` owner.
    fn merge(&self, frags: &[&Self::PageFrag]) -> Self::PageFrag {
        let mut consolidated = String::new();
        for frag in frags.into_iter() {
            consolidated.push_str(&*frag);
        }

        consolidated
    }

    // Used to feed custom recovery information back to a higher-level abstraction
    // during startup. For example, a B-Link tree must know what the current
    // root node is before it can start serving requests.
    fn recover(&self, _: &Self::PageFrag) -> Option<Self::Recovery> {
        None
    }
}

fn main() {
    let config = pagecache::ConfigBuilder::new().temporary(true);
    let pc: pagecache::PageCache<TestMaterializer, _, _> =
        pagecache::PageCache::start(config.build());
    {
        let guard = pin();
        let id = pc.allocate(&guard);

        // The first item in a page should be set using replace,
        // which signals that this is the beginning of a new
        // page history, and that any previous items associated
        // with this page should be forgotten.
        let key = pc.replace(id, PagePtr::null(), "a".to_owned(), &guard).unwrap();

        // Subsequent atomic updates should be added with link.
        let key = pc.link(id, key, "b".to_owned(), &guard).unwrap();
        let _key = pc.link(id, key, "c".to_owned(), &guard).unwrap();

        // When getting a page, the provide `Materializer` is
        // used to merge all pages together.
        let (consolidated, _key) = pc.get(id, &guard).unwrap();

        assert_eq!(consolidated, "abc".to_owned());
    }
}
```
