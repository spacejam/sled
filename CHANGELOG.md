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
