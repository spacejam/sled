#[macro_use]
extern crate serde_derive;
extern crate docopt;
extern crate env_logger;
extern crate jemallocator;
extern crate log;
extern crate rand;
extern crate sled;

use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
};

use docopt::Docopt;
use rand::{thread_rng, Rng};

#[cfg_attr(not(feature = "no_jemalloc"), global_allocator)]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

static TOTAL: AtomicUsize = AtomicUsize::new(0);
static SEQ: AtomicUsize = AtomicUsize::new(0);

const USAGE: &'static str = "
Usage: stress [--threads=<#>] [--burn-in] [--duration=<s>] \
    [--key-len=<l>] [--val-len=<l>] \
    [--get-prop=<p>] \
    [--set-prop=<p>] \
    [--del-prop=<p>] \
    [--cas-prop=<p>] \
    [--scan-prop=<p>] \
    [--merge-prop=<p>] \
    [--entries=<n>] \
    [--sequential]

Options:
    --threads=<#>      Number of threads [default: 4].
    --burn-in          Don't halt until we receive a signal.
    --duration=<s>     Seconds to run for [default: 10].
    --key-len=<l>      The length of keys [default: 1].
    --val-len=<l>      The length of values [default: 100].
    --get-prop=<p>     The relative proportion of get requests [default: 75].
    --set-prop=<p>     The relative proportion of set requests [default: 5].
    --del-prop=<p>     The relative proportion of del requests [default: 5].
    --cas-prop=<p>     The relative proportion of cas requests [default: 5].
    --scan-prop=<p>    The relative proportion of scan requests [default: 5].
    --merge-prop=<p>   The relative proportion of merge requests [default: 5].
    --entries=<n>      The total keyspace [default: 100000].
    --sequential       Run the test in sequential mode instead of random.
";

#[derive(Deserialize, Clone)]
struct Args {
    flag_threads: usize,
    flag_burn_in: bool,
    flag_duration: u64,
    flag_key_len: usize,
    flag_val_len: usize,
    flag_get_prop: usize,
    flag_set_prop: usize,
    flag_del_prop: usize,
    flag_cas_prop: usize,
    flag_scan_prop: usize,
    flag_merge_prop: usize,
    flag_entries: usize,
    flag_sequential: bool,
}

// defaults will be applied later based on USAGE above
static mut ARGS: Args = Args {
    flag_threads: 0,
    flag_burn_in: false,
    flag_duration: 0,
    flag_key_len: 0,
    flag_val_len: 0,
    flag_get_prop: 0,
    flag_set_prop: 0,
    flag_del_prop: 0,
    flag_cas_prop: 0,
    flag_scan_prop: 0,
    flag_merge_prop: 0,
    flag_entries: 0,
    flag_sequential: false,
};

fn report(shutdown: Arc<AtomicBool>) {
    let mut last = 0;
    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(std::time::Duration::from_secs(1));
        let total = TOTAL.load(Ordering::Acquire);

        println!("did {} ops", total - last);

        last = total;
    }
}

fn concatenate_merge(
    _key: &[u8],              // the key being merged
    old_value: Option<&[u8]>, // the previous value, if one existed
    merged_bytes: &[u8],      // the new bytes being merged in
) -> Option<Vec<u8>> {
    // set the new value, return None to delete
    let mut ret =
        old_value.map(|ov| ov.to_vec()).unwrap_or_else(|| vec![]);

    ret.extend_from_slice(merged_bytes);

    Some(ret)
}

fn run(tree: Arc<sled::Db>, shutdown: Arc<AtomicBool>) {
    let args = unsafe { ARGS.clone() };

    let get_max = args.flag_get_prop;
    let set_max = get_max + args.flag_set_prop;
    let del_max = set_max + args.flag_del_prop;
    let cas_max = del_max + args.flag_cas_prop;
    let scan_max = cas_max + args.flag_scan_prop;
    let merge_max = scan_max + args.flag_merge_prop;

    let bytes = |len| -> Vec<u8> {
        let i = if args.flag_sequential {
            SEQ.fetch_add(1, Ordering::Relaxed)
        } else {
            thread_rng().gen::<usize>()
        } % args.flag_entries;

        let i_bytes: [u8; std::mem::size_of::<usize>()] =
            unsafe { std::mem::transmute(i) };

        i_bytes.into_iter().cycle().take(len).cloned().collect()
    };
    let mut rng = thread_rng();

    while !shutdown.load(Ordering::Relaxed) {
        TOTAL.fetch_add(1, Ordering::Release);
        let key = bytes(args.flag_key_len);
        let choice = rng.gen_range(0, merge_max + 1);

        match choice {
            v if v <= get_max => {
                tree.get(&key).unwrap();
            }
            v if v > get_max && v <= set_max => {
                tree.set(&key, bytes(args.flag_val_len)).unwrap();
            }
            v if v > set_max && v <= del_max => {
                tree.del(&key).unwrap();
            }
            v if v > del_max && v <= cas_max => {
                let old_k = bytes(args.flag_val_len);

                let old = if rng.gen::<bool>() {
                    Some(old_k.as_slice())
                } else {
                    None
                };

                let new = if rng.gen::<bool>() {
                    Some(bytes(args.flag_val_len))
                } else {
                    None
                };

                match tree.cas(&key, old, new) {
                    Ok(_) | Err(sled::Error::CasFailed(_)) => {}
                    other => panic!("operational error: {:?}", other),
                }
            }
            v if v > cas_max && v <= scan_max => {
                let _ = tree
                    .scan(&key)
                    .take(rng.gen_range(0, 15))
                    .map(|res| res.unwrap())
                    .collect::<Vec<_>>();
            }
            _ => {
                tree.merge(&key, bytes(args.flag_val_len)).unwrap();
            }
        }
    }
}

fn main() {
    setup_logger();

    let args = unsafe {
        ARGS = Docopt::new(USAGE)
            .and_then(|d| {
                d.argv(std::env::args().into_iter()).deserialize()
            })
            .unwrap_or_else(|e| e.exit());
        ARGS.clone()
    };

    let shutdown = Arc::new(AtomicBool::new(false));

    let config = sled::ConfigBuilder::new()
        .io_bufs(2)
        .io_buf_size(8_000_000)
        .blink_node_split_size(4096)
        .page_consolidation_threshold(10)
        .cache_bits(6)
        .cache_capacity(1_000_000_000)
        .flush_every_ms(Some(10))
        .snapshot_after_ops(100_000)
        .print_profile_on_drop(true)
        .merge_operator(concatenate_merge)
        .build();

    let tree = Arc::new(sled::Db::start(config).unwrap());

    let mut threads = vec![];

    let now = std::time::Instant::now();

    let n_threads = args.flag_threads;

    for i in 0..n_threads + 1 {
        let tree = tree.clone();
        let shutdown = shutdown.clone();

        let t = if i == 0 {
            thread::spawn(move || report(shutdown))
        } else {
            thread::spawn(move || run(tree, shutdown))
        };

        threads.push(t);
    }

    if !args.flag_burn_in {
        thread::sleep(std::time::Duration::from_secs(unsafe {
            ARGS.flag_duration
        }));
        shutdown.store(true, Ordering::SeqCst);
    }

    for t in threads.into_iter() {
        t.join().unwrap();
    }

    let ops = TOTAL.load(Ordering::SeqCst);
    let time = now.elapsed().as_secs() as usize;

    println!(
        "did {} total ops in {} seconds. {} ops/s",
        ops,
        time,
        (ops * 1_000) / (time * 1_000)
    );
}

pub fn setup_logger() {
    use std::io::Write;

    fn tn() -> String {
        std::thread::current()
            .name()
            .unwrap_or("unknown")
            .to_owned()
    }

    let mut builder = env_logger::Builder::new();
    builder
        .format(|buf, record| {
            writeln!(
                buf,
                "{:05} {:25} {:10} {}",
                record.level(),
                tn(),
                record
                    .module_path()
                    .unwrap()
                    .split("::")
                    .last()
                    .unwrap(),
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Info);

    if std::env::var("RUST_LOG").is_ok() {
        builder.parse(&std::env::var("RUST_LOG").unwrap());
    }

    let _r = builder.try_init();
}
