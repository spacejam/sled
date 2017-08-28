Now that some property and sanitization tests exist, it's safe to open this up for wider contribution!

Specifically, there are a few areas that I would love external help with:

* performance tuning: this should theoretically be able to outperform rocksdb on reads,
  and innodb on writes! let's get there! the `Lru` usage is particularly nasty on the 
  flamegraphs at the moment.
* more testing: ALICE for testing crash safety, quickcheck on the `LockFreeLog` and `PageCache`
* a better C API: the current one is pretty unfriendly
* better docs: whatever you find confusing!
* the deep end: prototyping a compiler plugin that allows specifiable blocks of code to be
instrumented with pauses that cause interesting interleavings to be exhausted. I don't think
this can be done with just a macro yet...

General considerations:

* all PR's block on failing tests
* all PR's block on breaking API changes (with the sole exception of the emerging C API)
