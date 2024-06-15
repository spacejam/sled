use std::mem::size_of;
use std::process::exit;
use std::thread;
use std::time::Duration;

use rand::Rng;

use sled::{
    Config, Db as SledDb, Heap, HeapRecovery, MetadataStore, ObjectCache,
};

mod crash_batches;
mod crash_heap;
mod crash_iter;
mod crash_metadata_store;
mod crash_object_cache;
mod crash_sequential_writes;
mod crash_tx;

pub use crash_batches::run_crash_batches;
pub use crash_heap::run_crash_heap;
pub use crash_iter::run_crash_iter;
pub use crash_metadata_store::run_crash_metadata_store;
pub use crash_object_cache::run_crash_object_cache;
pub use crash_sequential_writes::run_crash_sequential_writes;
pub use crash_tx::run_crash_tx;

type Db = SledDb<8>;

// test names, also used as dir names
pub const SEQUENTIAL_WRITES_DIR: &str = "sequential_writes";
pub const BATCHES_DIR: &str = "batches";
pub const ITER_DIR: &str = "iter";
pub const TX_DIR: &str = "tx";
pub const METADATA_STORE_DIR: &str = "metadata_store";
pub const HEAP_DIR: &str = "heap";
pub const OBJECT_CACHE_DIR: &str = "object_cache";

const CRASH_DIR: &str = "crash_test_files";

fn spawn_killah() {
    thread::spawn(|| {
        let runtime = rand::thread_rng().gen_range(0..60_000);
        thread::sleep(Duration::from_micros(runtime));
        exit(9);
    });
}

fn u32_to_vec(u: u32) -> Vec<u8> {
    let buf: [u8; size_of::<u32>()] = u.to_be_bytes();
    buf.to_vec()
}

fn slice_to_u32(b: &[u8]) -> u32 {
    let mut buf = [0u8; size_of::<u32>()];
    buf.copy_from_slice(&b[..size_of::<u32>()]);

    u32::from_be_bytes(buf)
}

fn tree_to_string(tree: &Db) -> String {
    let mut ret = String::from("{");
    for kv_res in tree.iter() {
        let (k, v) = kv_res.unwrap();
        let k_s = slice_to_u32(&k);
        let v_s = slice_to_u32(&v);
        ret.push_str(&format!("{}:{}, ", k_s, v_s));
    }
    ret.push_str("}");
    ret
}
