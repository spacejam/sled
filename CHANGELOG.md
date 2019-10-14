# Unreleased

## New Features

* The `Config::open` method has been added to give
  `Config` a similar feel to std's `fs::OpenOptions`.
  The `Config::build` and `Db::start` methods are
  now deprecated in favor of calling `Config::open`
  directly
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
