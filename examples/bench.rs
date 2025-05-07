use std::path::Path;
use std::sync::Barrier;
use std::thread::scope;
use std::time::{Duration, Instant};
use std::{fs, io};

use num_format::{Locale, ToFormattedString};

use sled::{Config, Db as SledDb};

type Db = SledDb<1024>;

const N_WRITES_PER_THREAD: u32 = 4 * 1024 * 1024;
const MAX_CONCURRENCY: u32 = 4;
const CONCURRENCY: &[usize] = &[/*1, 2, 4,*/ MAX_CONCURRENCY as _];
const BYTES_PER_ITEM: u32 = 8;

trait Databench: Clone + Send {
    type READ: AsRef<[u8]>;
    const NAME: &'static str;
    const PATH: &'static str;
    fn open() -> Self;
    fn remove_generic(&self, key: &[u8]);
    fn insert_generic(&self, key: &[u8], value: &[u8]);
    fn get_generic(&self, key: &[u8]) -> Option<Self::READ>;
    fn flush_generic(&self);
    fn print_stats(&self);
}

impl Databench for Db {
    type READ = sled::InlineArray;

    const NAME: &'static str = "sled 1.0.0-alpha";
    const PATH: &'static str = "timing_test.sled-new";

    fn open() -> Self {
        sled::Config {
            path: Self::PATH.into(),
            zstd_compression_level: 3,
            cache_capacity_bytes: 1024 * 1024 * 1024,
            entry_cache_percent: 20,
            flush_every_ms: Some(200),
            ..Config::default()
        }
        .open()
        .unwrap()
    }

    fn insert_generic(&self, key: &[u8], value: &[u8]) {
        self.insert(key, value).unwrap();
    }
    fn remove_generic(&self, key: &[u8]) {
        self.remove(key).unwrap();
    }
    fn get_generic(&self, key: &[u8]) -> Option<Self::READ> {
        self.get(key).unwrap()
    }
    fn flush_generic(&self) {
        self.flush().unwrap();
    }
    fn print_stats(&self) {
        dbg!(self.stats());
    }
}

/*
impl Databench for old_sled::Db {
    type READ = old_sled::IVec;

    const NAME: &'static str = "sled 0.34.7";
    const PATH: &'static str = "timing_test.sled-old";

    fn open() -> Self {
        old_sled::open(Self::PATH).unwrap()
    }
    fn insert_generic(&self, key: &[u8], value: &[u8]) {
        self.insert(key, value).unwrap();
    }
    fn get_generic(&self, key: &[u8]) -> Option<Self::READ> {
        self.get(key).unwrap()
    }
    fn flush_generic(&self) {
        self.flush().unwrap();
    }
}
*/

/*
impl Databench for Arc<rocksdb::DB> {
    type READ = Vec<u8>;

    const NAME: &'static str = "rocksdb 0.21.0";
    const PATH: &'static str = "timing_test.rocksdb";

    fn open() -> Self {
        Arc::new(rocksdb::DB::open_default(Self::PATH).unwrap())
    }
    fn insert_generic(&self, key: &[u8], value: &[u8]) {
        self.put(key, value).unwrap();
    }
    fn get_generic(&self, key: &[u8]) -> Option<Self::READ> {
        self.get(key).unwrap()
    }
    fn flush_generic(&self) {
        self.flush().unwrap();
    }
}
*/

/*
struct Lmdb {
    env: heed::Env,
    db: heed::Database<
        heed::types::UnalignedSlice<u8>,
        heed::types::UnalignedSlice<u8>,
    >,
}

impl Clone for Lmdb {
    fn clone(&self) -> Lmdb {
        Lmdb { env: self.env.clone(), db: self.db.clone() }
    }
}

impl Databench for Lmdb {
    type READ = Vec<u8>;

    const NAME: &'static str = "lmdb";
    const PATH: &'static str = "timing_test.lmdb";

    fn open() -> Self {
        let _ = std::fs::create_dir_all(Self::PATH);
        let env = heed::EnvOpenOptions::new()
            .map_size(1024 * 1024 * 1024)
            .open(Self::PATH)
            .unwrap();
        let db = env.create_database(None).unwrap();
        Lmdb { env, db }
    }
    fn insert_generic(&self, key: &[u8], value: &[u8]) {
        let mut wtxn = self.env.write_txn().unwrap();
        self.db.put(&mut wtxn, key, value).unwrap();
        wtxn.commit().unwrap();
    }
    fn get_generic(&self, key: &[u8]) -> Option<Self::READ> {
        let rtxn = self.env.read_txn().unwrap();
        let ret = self.db.get(&rtxn, key).unwrap().map(Vec::from);
        rtxn.commit().unwrap();
        ret
    }
    fn flush_generic(&self) {
        // NOOP
    }
}
*/

/*
struct Sqlite {
    connection: rusqlite::Connection,
}

impl Clone for Sqlite {
    fn clone(&self) -> Sqlite {
        Sqlite { connection: rusqlite::Connection::open(Self::PATH).unwrap() }
    }
}

impl Databench for Sqlite {
    type READ = Vec<u8>;

    const NAME: &'static str = "sqlite";
    const PATH: &'static str = "timing_test.sqlite";

    fn open() -> Self {
        let connection = rusqlite::Connection::open(Self::PATH).unwrap();
        connection
            .execute(
                "create table if not exists bench (
                     key integer primary key,
                     val integer not null
                 )",
                [],
            )
            .unwrap();
        Sqlite { connection }
    }
    fn insert_generic(&self, key: &[u8], value: &[u8]) {
        loop {
            let res = self.connection.execute(
                "insert or ignore into bench (key, val) values (?1, ?2)",
                [
                    format!("{}", u32::from_be_bytes(key.try_into().unwrap())),
                    format!(
                        "{}",
                        u32::from_be_bytes(value.try_into().unwrap())
                    ),
                ],
            );
            if res.is_ok() {
                break;
            }
        }
    }
    fn get_generic(&self, key: &[u8]) -> Option<Self::READ> {
        let mut stmt = self
            .connection
            .prepare("SELECT b.val from bench b WHERE key = ?1")
            .unwrap();
        let mut rows =
            stmt.query([u32::from_be_bytes(key.try_into().unwrap())]).unwrap();

        let value = rows.next().unwrap()?;
        value.get(0).ok()
    }
    fn flush_generic(&self) {
        // NOOP
    }
}
*/

fn allocated() -> usize {
    #[cfg(feature = "testing-count-allocator")]
    {
        return sled::alloc::allocated();
    }
    0
}

fn freed() -> usize {
    #[cfg(feature = "testing-count-allocator")]
    {
        return sled::alloc::freed();
    }
    0
}

fn resident() -> usize {
    #[cfg(feature = "testing-count-allocator")]
    {
        return sled::alloc::resident();
    }
    0
}

fn inserts<D: Databench>(store: &D) -> Vec<InsertStats> {
    println!("{} inserts", D::NAME);
    let mut i = 0_u32;

    let factory = move || {
        i += 1;
        (store.clone(), i - 1)
    };

    let f = |state: (D, u32)| {
        let (store, offset) = state;
        let start = N_WRITES_PER_THREAD * offset;
        let end = N_WRITES_PER_THREAD * (offset + 1);
        for i in start..end {
            let k: &[u8] = &i.to_be_bytes();
            store.insert_generic(k, k);
        }
    };

    let mut ret = vec![];

    for concurrency in CONCURRENCY {
        let insert_elapsed =
            execute_lockstep_concurrent(factory, f, *concurrency);

        let flush_timer = Instant::now();
        store.flush_generic();

        let wps = (N_WRITES_PER_THREAD * *concurrency as u32) as u64
            * 1_000_000_u64
            / u64::try_from(insert_elapsed.as_micros().max(1))
                .unwrap_or(u64::MAX);

        ret.push(InsertStats {
            thread_count: *concurrency,
            inserts_per_second: wps,
        });

        println!(
            "{} inserts/s with {concurrency} threads over {:?}, then {:?} to flush {}",
            wps.to_formatted_string(&Locale::en),
            insert_elapsed,
            flush_timer.elapsed(),
            D::NAME,
        );
    }

    ret
}

fn removes<D: Databench>(store: &D) -> Vec<RemoveStats> {
    println!("{} removals", D::NAME);
    let mut i = 0_u32;

    let factory = move || {
        i += 1;
        (store.clone(), i - 1)
    };

    let f = |state: (D, u32)| {
        let (store, offset) = state;
        let start = N_WRITES_PER_THREAD * offset;
        let end = N_WRITES_PER_THREAD * (offset + 1);
        for i in start..end {
            let k: &[u8] = &i.to_be_bytes();
            store.remove_generic(k);
        }
    };

    let mut ret = vec![];

    for concurrency in CONCURRENCY {
        let remove_elapsed =
            execute_lockstep_concurrent(factory, f, *concurrency);

        let flush_timer = Instant::now();
        store.flush_generic();

        let wps = (N_WRITES_PER_THREAD * *concurrency as u32) as u64
            * 1_000_000_u64
            / u64::try_from(remove_elapsed.as_micros().max(1))
                .unwrap_or(u64::MAX);

        ret.push(RemoveStats {
            thread_count: *concurrency,
            removes_per_second: wps,
        });

        println!(
            "{} removes/s with {concurrency} threads over {:?}, then {:?} to flush {}",
            wps.to_formatted_string(&Locale::en),
            remove_elapsed,
            flush_timer.elapsed(),
            D::NAME,
        );
    }

    ret
}

fn gets<D: Databench>(store: &D) -> Vec<GetStats> {
    println!("{} reads", D::NAME);

    let factory = || store.clone();

    let f = |store: D| {
        let start = 0;
        let end = N_WRITES_PER_THREAD * MAX_CONCURRENCY;
        for i in start..end {
            let k: &[u8] = &i.to_be_bytes();
            store.get_generic(k);
        }
    };

    let mut ret = vec![];

    for concurrency in CONCURRENCY {
        let get_stone_elapsed =
            execute_lockstep_concurrent(factory, f, *concurrency);

        let rps = (N_WRITES_PER_THREAD * MAX_CONCURRENCY * *concurrency as u32)
            as u64
            * 1_000_000_u64
            / u64::try_from(get_stone_elapsed.as_micros().max(1))
                .unwrap_or(u64::MAX);

        ret.push(GetStats { thread_count: *concurrency, gets_per_second: rps });

        println!(
            "{} gets/s with concurrency of {concurrency}, {:?} total reads {}",
            rps.to_formatted_string(&Locale::en),
            get_stone_elapsed,
            D::NAME
        );
    }
    ret
}

fn execute_lockstep_concurrent<
    State: Send,
    Factory: FnMut() -> State,
    F: Sync + Fn(State),
>(
    mut factory: Factory,
    f: F,
    concurrency: usize,
) -> Duration {
    let barrier = &Barrier::new(concurrency + 1);
    let f = &f;

    scope(|s| {
        let mut threads = vec![];

        for _ in 0..concurrency {
            let state = factory();

            let thread = s.spawn(move || {
                barrier.wait();
                f(state);
            });

            threads.push(thread);
        }

        barrier.wait();
        let get_stone = Instant::now();

        for thread in threads.into_iter() {
            thread.join().unwrap();
        }

        get_stone.elapsed()
    })
}

#[derive(Debug, Clone, Copy)]
struct InsertStats {
    thread_count: usize,
    inserts_per_second: u64,
}

#[derive(Debug, Clone, Copy)]
struct GetStats {
    thread_count: usize,
    gets_per_second: u64,
}

#[derive(Debug, Clone, Copy)]
struct RemoveStats {
    thread_count: usize,
    removes_per_second: u64,
}

#[allow(unused)]
#[derive(Debug, Clone)]
struct Stats {
    post_insert_disk_space: u64,
    post_remove_disk_space: u64,
    allocated_memory: usize,
    freed_memory: usize,
    resident_memory: usize,
    insert_stats: Vec<InsertStats>,
    get_stats: Vec<GetStats>,
    remove_stats: Vec<RemoveStats>,
}

impl Stats {
    fn print_report(&self) {
        println!(
            "bytes on disk after inserts: {}",
            self.post_insert_disk_space.to_formatted_string(&Locale::en)
        );
        println!(
            "bytes on disk after removes: {}",
            self.post_remove_disk_space.to_formatted_string(&Locale::en)
        );
        println!(
            "bytes in memory: {}",
            self.resident_memory.to_formatted_string(&Locale::en)
        );
        for stats in &self.insert_stats {
            println!(
                "{} threads {} inserts per second",
                stats.thread_count,
                stats.inserts_per_second.to_formatted_string(&Locale::en)
            );
        }
        for stats in &self.get_stats {
            println!(
                "{} threads {} gets per second",
                stats.thread_count,
                stats.gets_per_second.to_formatted_string(&Locale::en)
            );
        }
        for stats in &self.remove_stats {
            println!(
                "{} threads {} removes per second",
                stats.thread_count,
                stats.removes_per_second.to_formatted_string(&Locale::en)
            );
        }
    }
}

fn bench<D: Databench>() -> Stats {
    let store = D::open();

    let insert_stats = inserts(&store);

    let before_flush = Instant::now();
    store.flush_generic();
    println!("final flush took {:?} for {}", before_flush.elapsed(), D::NAME);

    let post_insert_disk_space = du(D::PATH.as_ref()).unwrap();

    let get_stats = gets(&store);

    let remove_stats = removes(&store);

    store.print_stats();

    Stats {
        post_insert_disk_space,
        post_remove_disk_space: du(D::PATH.as_ref()).unwrap(),
        allocated_memory: allocated(),
        freed_memory: freed(),
        resident_memory: resident(),
        insert_stats,
        get_stats,
        remove_stats,
    }
}

fn du(path: &Path) -> io::Result<u64> {
    fn recurse(mut dir: fs::ReadDir) -> io::Result<u64> {
        dir.try_fold(0, |acc, file| {
            let file = file?;
            let size = match file.metadata()? {
                data if data.is_dir() => recurse(fs::read_dir(file.path())?)?,
                data => data.len(),
            };
            Ok(acc + size)
        })
    }

    recurse(fs::read_dir(path)?)
}

fn main() {
    let _ = env_logger::try_init();

    let new_stats = bench::<Db>();

    println!(
        "raw data size: {}",
        (MAX_CONCURRENCY * N_WRITES_PER_THREAD * BYTES_PER_ITEM)
            .to_formatted_string(&Locale::en)
    );
    println!("sled 1.0 space stats:");
    new_stats.print_report();

    /*
    let old_stats = bench::<old_sled::Db>();
    dbg!(old_stats);

    let new_sled_vs_old_sled_storage_ratio =
        new_stats.disk_space as f64 / old_stats.disk_space as f64;
    let new_sled_vs_old_sled_allocated_memory_ratio =
        new_stats.allocated_memory as f64 / old_stats.allocated_memory as f64;
    let new_sled_vs_old_sled_freed_memory_ratio =
        new_stats.freed_memory as f64 / old_stats.freed_memory as f64;
    let new_sled_vs_old_sled_resident_memory_ratio =
        new_stats.resident_memory as f64 / old_stats.resident_memory as f64;

    dbg!(new_sled_vs_old_sled_storage_ratio);
    dbg!(new_sled_vs_old_sled_allocated_memory_ratio);
    dbg!(new_sled_vs_old_sled_freed_memory_ratio);
    dbg!(new_sled_vs_old_sled_resident_memory_ratio);

    let rocksdb_stats = bench::<Arc<rocksdb::DB>>();

    bench::<Lmdb>();

    bench::<Sqlite>();
    */

    /*
    let new_sled_vs_rocksdb_storage_ratio =
        new_stats.disk_space as f64 / rocksdb_stats.disk_space as f64;
    let new_sled_vs_rocksdb_allocated_memory_ratio =
        new_stats.allocated_memory as f64 / rocksdb_stats.allocated_memory as f64;
    let new_sled_vs_rocksdb_freed_memory_ratio =
        new_stats.freed_memory as f64 / rocksdb_stats.freed_memory as f64;
    let new_sled_vs_rocksdb_resident_memory_ratio =
        new_stats.resident_memory as f64 / rocksdb_stats.resident_memory as f64;

    dbg!(new_sled_vs_rocksdb_storage_ratio);
    dbg!(new_sled_vs_rocksdb_allocated_memory_ratio);
    dbg!(new_sled_vs_rocksdb_freed_memory_ratio);
    dbg!(new_sled_vs_rocksdb_resident_memory_ratio);
    */

    /*
    let scan = Instant::now();
    let count = stone.iter().count();
    assert_eq!(count as u64, N_WRITES_PER_THREAD);
    let scan_elapsed = scan.elapsed();
    println!(
        "{} scanned items/s, total {:?}",
        (N_WRITES_PER_THREAD * 1_000_000) / u64::try_from(scan_elapsed.as_micros().max(1)).unwrap_or(u64::MAX),
        scan_elapsed
    );
    */

    /*
    let scan_rev = Instant::now();
    let count = stone.range(..).rev().count();
    assert_eq!(count as u64, N_WRITES_PER_THREAD);
    let scan_rev_elapsed = scan_rev.elapsed();
    println!(
        "{} reverse-scanned items/s, total {:?}",
        (N_WRITES_PER_THREAD * 1_000_000) / u64::try_from(scan_rev_elapsed.as_micros().max(1)).unwrap_or(u64::MAX),
        scan_rev_elapsed
    );
    */
}
