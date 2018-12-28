use super::*;

/// A space is a durable datastructure that shares
/// the operational concerns of all other spaces
/// in the system.
pub enum Space {
    /// `Meta` is a Tree mapping from namespace
    /// identifier to root page ID
    Meta,
    /// A linearizable KV store supporting point,
    /// range, and merge operator operations
    Tree(Option<MergeOperator>),
    /// A transactional KV store supporting point
    /// and range operations
    Tx,
    /// A durable metric collector
    Metrics,
    /// A monotonic ID generator that may skip
    /// ID's on restart
    IdGen,
}

// TODO
// 1. create meta
// 2. create default keyspace Tree
// 3. create_keyspace
// 4. get_keyspace_handle
// 5. del_keyspace
