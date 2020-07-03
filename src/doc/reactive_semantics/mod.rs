//! As of sled `0.16.8` we support the [`watch_prefix` feature](https://docs.rs/sled/latest/sled/struct.Tree.html#method.watch_prefix) which allows a caller to create an iterator over all events that happen to keys that begin with a specified prefix. Supplying an empty vector allows you to subscribe to all updates on the `Tree`.
//!
//! #### reactive architectures
//!
//! Subscription to keys prefixed with "topic names" can allow you to treat sled
//! as a durable message bus.
//!
//! #### replicated systems
//!
//! Watching the empty prefix will subscribe to all updates on the entire
//! database. You can feed this into a replication system
//!
//! #### analysis tools and auditing
//!
//! #### ordering guarantees
//!
//! Updates are received in-order for particular keys, but updates for different
//! keys may be observed in different orders by different `Subscriber`s. As an
//! example, consider updating the keys `k1` and `k2` twice, adding 1 to the
//! current value. Different `Subscriber`s may observe the following histories:
//!
//! ```
//! Set(k1, 100), Set(k1, 101), Set(k2, 200), Set(k2, 201)
//! or
//! Set(k1, 100), Set(k2, 200), Set(k1, 101), Set(k2, 201)
//! or
//! Set(k1, 100), Set(k2, 200), Set(k2, 201), Set(k1, 101)
//! or
//! Set(k2, 200), Set(k1, 100), Set(k1, 101), Set(k2, 201)
//! or
//! Set(k2, 200), Set(k1, 100), Set(k2, 201), Set(k1, 101)
//! or
//! Set(k2, 200), Set(k2, 201), Set(k1, 100), Set(k1, 101)
//! ```
