# Unreleased

## New Features

* #1178 batches and transactions are now unified for subscribers.
* #1231 `Tree::get_zero_copy` allows for reading a value directly
  in-place without making an `IVec` first.
* #1250 the global `print_profile` function has been added
  which is enabled when compiling with the `metrics` feature.
* #1254 `IVec` data will now always have an alignment of 8,
  which may enable interesting architecture-specific use cases.
* #1307 & #1315 `Db::contains_tree` can be used to see if a
  `Tree` with a given name already exists.

## Improvements

* #1214 a new slab-style storage engine has been added which
  replaces the previous file-per-blob technique for storing
  large pages.
* #1231 tree nodes now get merged into a single-allocation
  representation that is able to dynamically avoid various
  overheads, resulting in significant efficiency improvements.

## Breaking Changes

* #1400 Bump MSRV to 1.57.
* #1399 Thread support is now required on all platforms.
* #1135 The "no_metrics" anti-feature has been replaced with
  the "metrics" positive feature.
* #1178 the `Event` enum has become a unified struct that allows
  subscribers to iterate over each (Tree, key, optional value)
  involved in single key operations, batches, or transactions in
  a unified way.
* #1178 the `Event::key` method has been removed in favor of the
  new more comprehensive `iter` method.
* #1214 The deprecated `Config::build` method has been removed.
* #1248 The deprecated `Tree::set` method has been removed.
* #1248 The deprecated `Tree::del` method has been removed.
* #1250 The `Config::print_profile_on_drop` method has been
  removed in favor of the global `print_profile` function.
* #1252 The deprecated `Db::open` method has been removed.
* #1252 The deprecated `Config::segment_cleanup_skew` method
  has been removed.
* #1252 The deprecated `Config::segment_cleanup_threshold`
  method has been removed.
* #1252 The deprecated `Config::snapshot_path` method has
  been removed.
* #1253 The `IVec::subslice` method has been removed.
* #1275 Keys and values are now limited to 128gb on 64-bit
  platforms and 512mb on 32-bit platforms.
* #1281 `Config`'s `cache_capacity` is now a usize, as u64
  doesn't make sense for things that must fit in memory anyway.
* #1314 `Subscriber::next_timeout` now requires a mutable self
  reference.
* #1349 The "measure_allocs" feature has been removed.
* #1354 `Error` has been modified to be Copy, removing all
  heap-allocated variants.

## Bug Fixes

* #1202 Fix a space leak where blobs were not
  removed when replaced by another blob.
* #1229 the powerful ALICE crash consistency tool has been
  used to discover several crash vulnerabilities, now fixed.

# 0.34.7

## Bug Fixes

* #1314 Fix a bug in Subscriber's Future impl.

# 0.34.6

## Improvements

* documentation improved

# 0.34.5

## Improvements

* #1164 widen some trait bounds on trees and batches

# 0.34.4

## New Features

* #1151 `Send` is implemented for `Iter`
* #1167 added `Tree::first` and `Tree::last` functions
  to retrieve the first or last items in a `Tree`, unless
  the `Tree` is empty.

## Bug Fixes

* #1159 dropping a `Db` instance will no-longer
  prematurely shut-down the background flusher
  thread.
* #1168 fixed an issue that was causing panics during
  recovery in 32-bit code.
* #1170 when encountering corrupted storage data,
  the recovery process will panic less often.

# 0.34.3

## New Features

* #1146 added `TransactionalTree::generate_id`

# 0.34.2

## Improvements

* #1133 transactions and writebatch performance has been
  significantly improved by removing a bottleneck in
  the atomic batch stability tracking code.

# 0.34.1

## New Features

* #1136 Added the `TransactionalTree::flush` method to
  flush the underlying database after the transaction
  commits and before the transaction returns.

# 0.34

## Improvements

* #1132 implemented From<sled::Error> for io::Error to
  reduce friction in some situations.

## Breaking Changes

* #1131 transactions performed on `Tree`s from different
  `Db`s will now safely fail.
* #1131 transactions may now only be performed on tuples
  of up to 14 elements. For higher numbers, please use
  slices.

# 0.33

## Breaking Changes

* #1125 the backtrace crate has been made optional, which
  cuts several seconds off compilation time, but may cause
  breakage if you interacted with the backtrace field
  of corruption-related errors.

## Bug Fixes

* #1128 `Tree::pop_min` and `Tree::pop_max` had a bug where
  they were not atomic.

# 0.32.1

## New Features

* #1116 `IVec::subslice` has been added to facilitate
  creating zero-copy subsliced `IVec`s that are backed
  by the same data.

## Bug Fixes

* #1120 Fixed a use-after-free caused by missing `ref` keyword
  on a `Copy` type in a pattern match in `IVec::as_mut`.
* #1108 conversions from `Box<[u8]>` to `IVec` are fixed.

# 0.32

## New Features

* #1079 `Transactional` is now implemented for
  `[&Tree]` and `[Tree]` so you can avoid the
  previous friction of using tuples, as was
  necessary previously.
* #1058 The minimum supported Rust version (MSRV)
  is now 1.39.0.
* #1037 `Subscriber` now implements `Future` (non-fused)
  so prefix watching may now be iterated over via
  `while let Some(event) = (&mut subscriber).await {}`

## Improvements

* #965 concurrency control is now dynamically enabled
  for atomic point operations, so that it may be
  avoided unless transactional functionality is
  being used in the system. This significantly
  increases performance for workloads that do not
  use transactions.
* A number of memory optimizations have been implemented.
* Disk usage has been significantly reduced for many
  workloads.
* #1016 On 64-bit systems, we can now store 1-2 trillion items.
* #993 Added DerefMut and AsMut<[u8]> for `IVec` where it
  works similarly to a `Cow`, making a private copy
  if the backing `Arc`'s strong count is not 1.
* #1020 The sled wiki has been moved into the documentation
  itself, and is accessible through the `doc` module
  exported in lib.

## Breaking Changes

* #975 Changed the default `segment_size` from 8m to 512k.
  This will result in far smaller database files due
  to better file garbage collection granularity.
* #975 deprecated several `Config` options that will be
  removed over time.
* #1000 rearranged some transaction-related imports, and
  moved them to the `transaction` module away from
  the library root to keep the top level docs clean.
* #1015 `TransactionalTree::apply_batch` now accepts
  its argument by reference instead of by value.
* `Event` has been changed to make the inner fields
  named instead of anonymous.
* #1057 read-only mode has been removed due to not having
  the resources to properly keep it tested while
  making progress on high priority issues. This may
  be correctly implemented in the future if resources
  permit.
* The conversion between `Box<[u8]>` and `IVec` has
  been temporarily removed. This is re-added in 0.32.1.

# 0.31

## Improvements

* #947 dramatic read and recovery optimizations
* #921 reduced the reliance on locks while
  performing multithreaded IO on windows.
* #928 use `sync_file_range` on linux instead
  of a full fsync for most writes.
* #946 io_uring support changed to the `rio` crate
* #939 reduced memory consumption during
  zstd decompression

## Breaking Changes

* #927 use SQLite-style varints for serializing
  `u64`. This dramatically reduces the written
  bytes for databases that store small keys and
  values.
* #943 use varints for most of the fields in
  message headers, causing an additional large
  space reduction. combined with #927, these
  changes reduce bytes written by 68% for workloads
  writing small items.

# 0.30.3

* Documentation-only release

# 0.30.2

## New Features

* Added the `open` function for quickly
  opening a database at a path with default
  configuration.

# 0.30.1

## Bugfixes

* Fixed an issue where an idle threadpool worker
  would spin in a hot loop until work arrived

# 0.30

## Breaking Changes

* Migrated to a new storage format

## Bugfixes

* Fixed a bug where cache was not being evicted.
* Fixed a bug with using transactions with
  compression.

# 0.29.2

## New Features

* The `create_new` option has been added
  to `Config`, allowing the user to specify
  that a database should only be freshly
  created, rather than re-opened.

# 0.29.1

## Bugfixes

* Fixed a bug where prefix encoding could be
  incorrectly handled when merging nodes together.

# 0.29

## New Features

* The `Config::open` method has been added to give
  `Config` a similar feel to std's `fs::OpenOptions`.
  The `Config::build` and `Db::start` methods are
  now deprecated in favor of calling `Config::open`
  directly.
* A `checksum` method has been added to Tree and Db
  for use in verifying backups and migrations.
* Transactions may now involve up to 69 different
  tables. Nice.
* The `TransactionError::Abort` variant has had
  a generic member added that can be returned
  as a way to return information from a
  manually-aborted transaction. An `abort` helper
  function has been added to reduce the boiler-
  plate required to return aborted results.

## Breaking Changes

* The `ConfigBuilder` structure has been removed
  in favor of a simplified `Config` structure
  with the same functionality.
* The way that sled versions are detected at
  initialization time is now independent of serde.
* The `cas` method is deprecated in favor of the new
  `compare_and_swap` method which now returns the
  proposed value that failed to be applied.
* Tree nodes now have constant prefix encoding
  lengths.
* The `io_buf_size` configurable renamed to
  `segment_size`.
* The `io_buf_size` configurable method has been
  removed from ConfigBuilder. This can be manually
  set by setting the attribute directly on the
  ConfigBuilder, but this is discouraged.
  Additionally, this must now be a power of 2.
* The `page_consolidation_threshold` method has been
  removed from ConfigBuilder, and this is now
  a constant of 10.

# 0.28

## Breaking Changes

* `Iter` no longer has a lifetime parameter.
* `Db::open_tree` now returns a `Tree` instead of
  an `Arc<Tree>`. `Tree` now has an inner type that
  uses an `Arc`, so you don't need to think about it.

## Bug Fixes

* A bug with prefix encoding has been fixed that
  led to nodes with keys longer than 256 bytes
  being stored incorrectly, which led to them
  being inaccessible and also leading to infinite
  loops during iteration.
* Several cases of incorrect unsafe code were removed
  from the sled crate. No bugs are known to have been
  encountered, but they may have resulted in
  incorrect optimizations in future refactors.

# 0.27

## Breaking Changes

* `Event::Set` has been renamed to `Event::Insert` and
  `Event::Del` has been renamed to `Event::Remove`. These
  names better align with the methods of BTreeMap from
  the standard library.

## Bug Fixes

* A deadlock was possible in very high write volume
  situations when the segment accountant lock was
  taken by all IO threads while a task was blocked
  trying to submit a file truncation request to the
  threadpool while holding the segment accountant lock.

## New Features

* `flush_async` has been added to perform time-intensive
  flushing in an asynchronous manner, returning a Future.

# 0.26.1

## Improvements

* std::thread is no longer used on platforms other than
  linux, macos, and windows, which increases portability.

# 0.26

## New Features

* Transactions! You may now call `Tree::transaction` and
  perform reads, writes, and deletes within a provided
  closure with a `TransactionalTree` argument. This
  closure may be called multiple times if the transaction
  encounters a concurrent update in the process of its
  execution. Transactions may also be used on tuples of
  `Tree` objects, where the closure will then be
  parameterized on `TransactionalTree` instances providing
  access to each of the provided `Tree` instances. This
  allows you to atomically read and modify multiple
  `Tree` instances in a single atomic operation.
  These transactions are serializable, fully ACID,
  and optimistic.
* `Tree::apply_batch` allows you to apply a `Batch`
* `TransactionalTree::apply_batch` allow you to
  apply a `Batch` from within a transaction.

## Breaking Changes

* `Tree::batch` has been removed. Now you can directly
  create a `Batch` with `Batch::default()` and then apply
  it to a `Tree` with `Tree::apply_batch` or during a
  transaction using `TransactionalTree::apply_batch`.
  This facilitates multi-`Tree` batches via transactions.
* `Event::Merge` has been removed, and `Tree::merge` will
  now send a complete `Event::Set` item to be distributed
  to all listening subscribers.
