//! <p align="center">
//!   <img src="https://raw.githubusercontent.com/spacejam/sled/master/art/tree_face.png" width="20%" height="auto" />
//! </p>
//!
//! # Experiences with Other Systems
//!
//! sled is motivated by the experiences gained while working with other
//! stateful systems, outlined below.
//!
//! Most of the points below are learned from being burned, rather than
//! delighted.
//!
//! #### MySQL
//!
//! * make it easy to tail the replication stream in flexible topologies
//! * support merging shards a la MariaDB
//! * support mechanisms for live, lock-free schema updates a la
//!   pt-online-schema-change
//! * include GTID in all replication information
//! * actively reduce tree fragmentation
//! * give operators and distributed database creators first-class support for
//!   replication, sharding, backup, tuning, and diagnosis
//! * O_DIRECT + real linux AIO is worth the effort
//!
//! #### Redis
//!
//! * provide high-level collections that let engineers get to their business
//!   logic as quickly as possible instead of forcing them to define a schema in
//!   a relational system (usually spending an hour+ googling how to even do it)
//! * don't let single slow requests block all other requests to a shard
//! * let operators peer into the sequence of operations that hit the database
//!   to track down bad usage
//! * don't force replicas to retrieve the entire state of the leader when they
//!   begin replication
//!
//! #### HBase
//!
//! * don't split "the source of truth" across too many decoupled systems or you
//!   will always have downtime
//! * give users first-class APIs to peer into their system state without
//!   forcing them to write scrapers
//! * serve http pages for high-level overviews and possibly log access
//! * coprocessors are awesome but people should have easy ways of doing
//!   secondary indexing
//!
//! #### RocksDB
//!
//! * give users tons of flexibility with different usage patterns
//! * don't force users to use distributed machine learning to discover
//!   configurations that work for their use cases
//! * merge operators are extremely powerful
//! * merge operators should be usable from serial transactions across multiple
//!   keys
//!
//! #### etcd
//!
//! * raft makes operating replicated systems SO MUCH EASIER than popular
//!   relational systems / redis etc...
//! * modify raft to use leader leases instead of using the paxos register,
//!   avoiding livelocks in the presence of simple partitions
//! * give users flexible interfaces
//! * reactive semantics are awesome, but access must be done through smart
//!   clients, because users will assume watches are reliable
//! * if we have smart clients anyway, quorum reads can be cheap by
//!   lower-bounding future reads to the raft id last observed
//! * expose the metrics and operational levers required to build a self-driving
//!   stateful system on top of k8s/mesos/cloud providers/etc...
//!
//! #### Tendermint
//!
//! * build things in a testable way from the beginning
//! * don't seek gratuitous concurrency
//! * allow replication streams to be used in flexible ways
//! * instant finality (or interface finality, the thing should be done by the
//!   time the request successfully returns to the client) is mandatory for nice
//!   high-level interfaces that don't push optimism (and rollbacks) into
//!   interfacing systems
//!
//! #### LMDB
//!
//! * approach a wait-free tree traversal for reads
//! * use modern tree structures that can support concurrent writers
//! * multi-process is nice for browsers etc...
//! * people value read performance and are often forgiving of terrible write
//!   performance for most workloads
//!
//! #### Zookeeper
//! * reactive semantics are awesome, but access must be done through smart
//!   clients, because users will assume watches are reliable
//! * the more important the system, the more you should keep old snapshots
//!   around for emergency recovery
//! * never assume a hostname that was resolvable in the past will be resolvable
//!   in the future
//! * if a critical thread dies, bring down the entire system
//! * make replication configuration as simple as possible. people will mess up
//!   the order and cause split brains if this is not automated.
