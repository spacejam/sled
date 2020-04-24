# Unreleased

## New Features

* The minimum supported Rust version (MSRV) is now 1.37.0.
* `Subscriber` now implements `Future` (non-fused)
  so prefix watching may now be iterated over via
  `while let Some(event) = (&mut subscriber).await {}`

## Improvements

* Added a config `Mode` which over time will be
  the main performance-related tunable, with only
  2 options: Fast or Small.
* A number of memory optimizations have been implemented.
* On 64-bit systems, we can now store 1-2 trillion items.
* Added DerefMut and AsMut<[u8]> for `IVec` where it
  works similarly to a `Cow`, making a private copy
  if the backing `Arc`'s strong count is not 1.

## Breaking Changes

* Minimum supported Rust version is now 1.40.
* Changed the default `segment_size` from 8m to 512k.
  This will result in far smaller database files.
* deprecated several `Config` options that will be
  removed over time.
* rearranged some transaction-related imports, and
  moved them to the `transaction` module away from
  the library root to keep the top level docs clean.
* #1015 `TransactionalTree::apply_batch` now accepts
  its argument by reference instead of by value.
* `Event` has been changed to make the inner fields
  named instead of anonymous.

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
