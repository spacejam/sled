//! Here's a look at where sled is at, and where it's going architecturally. The system is very much under active development, and we have a ways to go. If specific areas are interesting to you, I'd love to [work together](https://github.com/spacejam/sled/blob/master/CONTRIBUTING.md)! If your business has a need for particular items below, you can [fund development of particular features](https://opencollective.com/sled).
//!
//! People face unnecessary hardship when working with existing embedded
//! databases. They tend to have sharp performance trade-offs, are difficult to
//! tune, have unclear consistency guarantees, and are generally inflexible.
//! Facebook uses distributed machine learning to find configurations that
//! achieve great performance for specific workloads on rocksdb. Most engineers
//! don't have access to that kind of infrastructure. We would like to build
//! sled so that it can be optimized using simple local methods, with as little
//! user input as possible, and in many cases exceed the performance of popular
//! systems today.
//!
//! This is how we aim to improve the situation:
//!
//! * low configuration required to get great performance on a wide variety of
//!   workloads by using a modified Bw-Tree and keeping workload metrics that
//!   allow us to self-tune
//! * first-class subscriber semantics for operations on specified prefixes of
//!   keys
//! * first-class programmatic access to the binary replication stream
//! * serializable transactions
//!
//! ### Indexing
//!
//! sled started as an implementation of a Bw-Tree, but over time has moved away
//! from certain architectural aspects that have been difficult to tune. The
//! first thing to be dropped from the original Bw-Tree design was the in-memory
//! representation of a node as a long linked list of updates, terminating in
//! the actual base tree node. It was found that by leaning into atomic
//! reference counting, it became quite performant to perform RCU on entire tree
//! nodes for every update, because a tree node only needs 2 allocations (the
//! node itself, and a vector of children). All other items are protected by
//! their own rust `Arc`. This made reads dramatically faster, and allowed them
//! to avoid allocations that were required previously to build up a dynamic
//! "view" over a chain of partial updates.
//!
//! A current area of effort is to store tree nodes as a Rust
//! `RwLock<Arc<Node>>`. The Rust `Arc` has a cool method called `make_mut`
//! which can provide mutable access to an Arc if the strong count is 1, or make
//! a clone if it isn't and then provide a mutable reference to the local clone.
//! This will allow us to perform even fewer allocations and avoid RCU on the
//! tree nodes in cases of lower contention. Nesting an `Arc` in a lock
//! structure allows for an interesting "snapshot read" semantic that allows
//! writers not to block on readers. It is a middle ground between a `RwLock`
//! and RCU that trades lower memory pressure for occasional blocking when a
//! writer is holding a writer lock. This is expected to be a fairly low cost,
//! but benchmarks have not yet been produced for this prospective architecture.
//!
//! The merge and split strategies are kept from the Bw-Tree, but this might be
//! switched to using pagecache-level transactions once a cicada-like
//! transaction protocol is implemented on top of it.
//!
//! * [The Bw-Tree: A B-tree for New Hardware
//! Platforms](https://15721.courses.cs.cmu.edu/spring2018/papers/08-oltpindexes1/bwtree-icde2013.pdf)
//! * [Building a Bw-Tree Takes More Than Just Buzz Words](http://www.cs.cmu.edu/~huanche1/publications/open_bwtree.pdf)
//!
//! ### Caching
//!
//! sled uses a pagecache that is based on LLAMA. This lets us write small
//! updates to pages without rewriting the entire page, achieving low write
//! amplification. Flash storage lets us scatter random reads in parallel, so to
//! read a logical page, we may read several fragments and collect them in
//! memory. The pagecache can be used to back any high level structure, and
//! provides a lock-free interface that supports RCU-style access patterns. When
//! the number of page deltas reaches a certain length, we squish the page
//! updates into a single blob.
//!
//! The caching is currently pretty naive. We use 256 cache shards by default. Each cache shard is a simple LRU cache implemented as a doubly-linked list protected by a `Mutex`. Future directions may take inspiration from ZFS's adaptive replacement cache, which will give us scan and thrash resistance. See [#65](https://github.com/spacejam/sled/issues/65).
//!
//! * [LLAMA: A Cache/Storage Subsystem for Modern Hardware](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/llama-vldb2013.pdf)
//! * [Adaptive replacement cache](https://en.wikipedia.org/w/index.php?title=Adaptive_replacement_cache&oldid=865482923)
//!
//! ### Concurrency Control
//!
//! sled supports point reads and writes in serializable transactions across
//! multiple trees. This is fairly limited, and does not yet use a
//! high-performance concurrency control mechanism. In order to support scans,
//! we need to be able to catch phantom conflicts. To do this, we are taking
//! some inspiration from Cicada, in terms of how they include index nodes in
//! transactions, providing a really nice way to materialize conflicts relating
//! to phantoms. sled has an ID generator built into it now, accessible from the
//! `generate_id` method on `Tree`. This can churn out 75-125 million unique
//! monotonic ID's per second on a macbook pro, so we may not need to adopt
//! Cicada's distributed timestamp generation techniques for a long time. We
//! will be using Cicada's approach to adaptive validation, causing early aborts
//! when higher contention is detected.
//!
//! * [Cicada: Dependably Fast Multi-Core In-Memory Transactions](http://15721.courses.cs.cmu.edu/spring2018/papers/06-mvcc2/lim-sigmod2017.pdf)
//!
//! ### Storage
//!
//! sled splits the main storage file into fixed-sized segments. We track which
//! pages live in which segments. A page may live in several segments, because
//! we support writing partial updates to a page with our LLAMA-like approach.
//! When a page with several fragments is squished together, we mark the page as
//! freed from the previous segments. When a segment reaches a configurable low
//! threshold of live pages, we start moving the remaining pages to other
//! segments so that underutilized segments can be reused, and we generally keep
//! the amount of fragmentation in the system controlled.
//!
//! As of July 2019, sled is naive about where it puts rewritten pages. Future directions will separate base pages from page deltas, and possibly have generational considerations. See [#450](https://github.com/spacejam/sled/issues/450). Also, when values reach a particularly large size, it no longer makes sense to inline them in leaf nodes of the tree. Taking a cue from `WiscKey`, we can eventually split these out, but we can be much more fine grained about placement strategy over time. Generally, being smart about rewriting and defragmentation is where sled may carve out the largest performance gains over existing production and research systems.
//!
//! * [The Design and Implementation of a Log-Structured File System](https://people.eecs.berkeley.edu/~brewer/cs262/LFS.pdf)
//! * [`WiscKey`: Separating Keys from Values in SSD-conscious Storage](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf)
//! * [Monkey: Optimal Navigable Key-Value Store](http://stratos.seas.harvard.edu/files/stratos/files/monkeykeyvaluestore.pdf)
//! * [Designing Access Methods: The RUM Conjecture](https://stratos.seas.harvard.edu/files/stratos/files/rum.pdf)
//! * [The Unwritten Contract of Solid State Drives](http://pages.cs.wisc.edu/~jhe/eurosys17-he.pdf)
//! * [The five-minute rule twenty years later, and how flash memory changes the
//!   rules](http://www.cs.cmu.edu/~damon2007/pdf/graefe07fiveminrule.pdf)
//! * [An Efficient Memory-Mapped Key-Value Store for `FlashStorage`](http://www.exanest.eu/pub/SoCC18_efficient_kv_store.pdf)
//! * [Generalized File System Dependencies](http://featherstitch.cs.ucla.edu/publications/featherstitch-sosp07.pdf)
//!
//! ### Replication
//!
//! We want to give database implementors great tools for replicating their data
//! backed by sled. We will provide first-class binary replication stream
//! access, as well as subscriber to high level tree updates that happen on
//! specified prefixes. These updates should be witnessed in the same order that
//! they appear in the log by all consumers.
//!
//! We will likely include a default replication implementation, based either on
//! raft or harpoon (raft but with leases instead of a paxos register-based
//! leadership mechanism to protect against bouncing leadership in the presence
//! of partitions). Additionally, we can get nice throughput gains over vanilla
//! raft by separating the concerns of block replication and consensus on
//! metadata. Blocks can be replicated in a more fragmented + p2p-like manner,
//! with HOL-blocking-prone consensus being run on ordering of said blocks. This
//! pushes a bit more complexity into `RequestVotes` compared to vanilla raft,
//! but allows us to increase throughput a bit.
//!
//! ### Reclamation
//!
//! We use epoch-based reclamation to ensure that we don't free memory until any
//! possible witnessing threads are done with their work. This is the mechanism
//! that lets us return zero-copy to values in our pagecache for tree gets.
//!
//! Right now we use crossbeam-epoch for this. We may create a shallow fork
//! (gladly contributed upstream if the maintainers are interested) that allows
//! different kinds of workloads to bound the amount of garbage that they clean
//! up, possibly punting more cleanups to a threadpool and operations that seem
//! to prioritize throughput rather than latency.
//!
//! Possible future directions include using something like
//! quiescent-state-based-reclamation, but we need to study more before
//! considering alternative approaches.
//!
//! * [Comparative Performance of Memory Reclamation Strategies for Lock-free and Concurrently-readable Data Structures](http://www.cs.utoronto.ca/~tomhart/papers/tomhart_thesis.pdf)
//!
//! ### Checkpointing
//!
//! Sled has an extremely naive checkpoint strategy. It periodically takes the
//! last snapshot, scans the segments in the log with an LSN higher than last
//! LSN applied to the snapshot, building a snapshot from the segments it reads.
//! A snapshot is effectively a CRDT, because it can use the LSN number on read
//! messages as a last-write-wins register. It is currently the same mechanism
//! as the recovery mechanism, where the data is read directly off the disk and
//! page metadata is stored in a snapshot that is updated. The snapshot is
//! entirely an optimization for recovery, and can be deleted without impacting
//! recovery correctness.
//!
//! We are moving to a CRDT-like snapshot recovery technique, and we can easily
//! parallelize recovery up until the "safety buffer" for the last few segments
//! of the log.
//!
//! We would also like to move toward the delta-checkpoint model used in
//! Hekaton, as it would allow us to further parallelize generation of
//! checkpoint information.
//!
//! ### Misc Considerations
//!
//! * [How to Architect a Query Compiler, Revisited](https://www.cs.purdue.edu/homes/rompf/papers/tahboub-sigmod18.pdf)
//!     * shows that we can compile queries without resorting to complex
//!       implementations by utilizing Futamura projections
//! * [CMU 15-721 (Spring 2018) Advanced Database Systems](https://15721.courses.cs.cmu.edu/spring2018/schedule.html)
//!     * a wonderful overview of the state of the art in various database
//!       topics. start here if you want to contribute deeply and don't know
//!       where to begin!
//! * [Everything You Always Wanted to Know About Synchronization but Were Afraid to Ask](http://sigops.org/s/conferences/sosp/2013/papers/p33-david.pdf)
//!     * suggests that we should eventually aim for an approach that is
//!       shared-nothing across sockets, but lock-free within them
