use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
};

use docopt::Docopt;
use rand::{thread_rng, Rng};
use serde::Deserialize;

#[cfg_attr(
    // only enable jemalloc on linux and macos by default
    all(
        any(target_os = "linux", target_os = "macos"),
        not(feature = "no_jemalloc"),
    ),
    global_allocator
)]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

static TOTAL: AtomicUsize = AtomicUsize::new(0);
static SEQ: AtomicUsize = AtomicUsize::new(0);

const USAGE: &str = "
Usage: stress [--threads=<#>] [--burn-in] [--duration=<s>] \
    [--key-len=<l>] [--val-len=<l>] \
    [--get-prop=<p>] \
    [--set-prop=<p>] \
    [--del-prop=<p>] \
    [--cas-prop=<p>] \
    [--scan-prop=<p>] \
    [--merge-prop=<p>] \
    [--entries=<n>] \
    [--sequential] \
    [--total-ops=<n>]

Options:
    --threads=<#>      Number of threads [default: 4].
    --burn-in          Don't halt until we receive a signal.
    --duration=<s>     Seconds to run for [default: 10].
    --key-len=<l>      The length of keys [default: 10].
    --val-len=<l>      The length of values [default: 100].
    --get-prop=<p>     The relative proportion of get requests [default: 94].
    --set-prop=<p>     The relative proportion of set requests [default: 2].
    --del-prop=<p>     The relative proportion of del requests [default: 1].
    --cas-prop=<p>     The relative proportion of cas requests [default: 1].
    --scan-prop=<p>    The relative proportion of scan requests [default: 1].
    --merge-prop=<p>   The relative proportion of merge requests [default: 1].
    --entries=<n>      The total keyspace [default: 100000].
    --sequential       Run the test in sequential mode instead of random.
    --total-ops=<n>    Stop test after executing a total number of operations.
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
    flag_total_ops: Option<usize>,
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
    flag_total_ops: None,
};

fn report(shutdown: Arc<AtomicBool>) {
    let mut last = 0;
    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(std::time::Duration::from_secs(1));
        let total = TOTAL.load(Ordering::Acquire);

        println!("did {} ops, {}mb RSS", total - last, rss() / (1024 * 1024));

        last = total;
    }
}

fn concatenate_merge(
    _key: &[u8],              // the key being merged
    old_value: Option<&[u8]>, // the previous value, if one existed
    merged_bytes: &[u8],      // the new bytes being merged in
) -> Option<Vec<u8>> {
    // set the new value, return None to delete
    let mut ret = old_value.map(|ov| ov.to_vec()).unwrap_or_else(|| vec![]);

    ret.extend_from_slice(merged_bytes);

    Some(ret)
}

fn run(tree: Arc<sled::Db>, shutdown: Arc<AtomicBool>) {
    let args = unsafe { ARGS.clone() };

    let get_max = args.flag_get_prop;
    let set_max = get_max + args.flag_set_prop;
    let del_max = set_max + args.flag_del_prop;
    let cas_max = del_max + args.flag_cas_prop;
    let merge_max = cas_max + args.flag_merge_prop;
    let scan_max = merge_max + args.flag_scan_prop;

    let bytes = |len| -> Vec<u8> {
        let i = if args.flag_sequential {
            SEQ.fetch_add(1, Ordering::Relaxed)
        } else {
            thread_rng().gen::<usize>()
        } % args.flag_entries;

        let i_bytes = i.to_be_bytes();

        i_bytes
            .iter()
            .skip_while(|v| **v == 0)
            .cycle()
            .take(len)
            .copied()
            .collect()
    };
    let mut rng = thread_rng();

    while !shutdown.load(Ordering::Relaxed) {
        let op = TOTAL.fetch_add(1, Ordering::Release);
        let key = bytes(args.flag_key_len);
        let choice = rng.gen_range(0, scan_max + 1);

        match choice {
            v if v <= get_max => {
                tree.get(&key).unwrap();
            }
            v if v > get_max && v <= set_max => {
                tree.insert(&key, bytes(args.flag_val_len)).unwrap();
            }
            v if v > set_max && v <= del_max => {
                tree.remove(&key).unwrap();
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

                if let Err(e) = tree.compare_and_swap(&key, old, new) {
                    panic!("operational error: {:?}", e);
                }
            }
            v if v > cas_max && v <= merge_max => {
                tree.merge(&key, bytes(args.flag_val_len)).unwrap();
            }
            _ => {
                let iter = tree.range(key..).map(|res| res.unwrap());

                if op % 2 == 0 {
                    let _ = iter.take(rng.gen_range(0, 15)).collect::<Vec<_>>();
                } else {
                    let _ = iter
                        .rev()
                        .take(rng.gen_range(0, 15))
                        .collect::<Vec<_>>();
                }
            }
        }
    }
}

fn rss() -> usize {
    #[cfg(target_os = "linux")]
    {
        use std::io::prelude::*;
        use std::io::BufReader;

        let mut buf = String::new();
        let mut f =
            BufReader::new(std::fs::File::open("/proc/self/statm").unwrap());
        f.read_line(&mut buf).unwrap();
        let mut parts = buf.split_whitespace();
        let rss_pages = parts.nth(1).unwrap().parse::<usize>().unwrap();
        rss_pages * 4096
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

fn main() {
    #[cfg(feature = "logging")]
    setup_logger();

    let args = unsafe {
        ARGS = Docopt::new(USAGE)
            .and_then(|d| d.argv(std::env::args()).deserialize())
            .unwrap_or_else(|e| e.exit());
        ARGS.clone()
    };

    let shutdown = Arc::new(AtomicBool::new(false));

    let config = sled::Config::new()
        .cache_capacity(256 * 1024 * 1024)
        .flush_every_ms(Some(200))
        .print_profile_on_drop(true);

    let tree = Arc::new(config.open().unwrap());
    tree.set_merge_operator(concatenate_merge);

    let mut threads = vec![];

    let now = std::time::Instant::now();

    let n_threads = args.flag_threads;

    for i in 0..=n_threads {
        let tree = tree.clone();
        let shutdown = shutdown.clone();

        let t = if i == 0 {
            thread::Builder::new()
                .name("reporter".into())
                .spawn(move || report(shutdown))
                .unwrap()
        } else {
            thread::spawn(move || run(tree, shutdown))
        };

        threads.push(t);
    }

    if let Some(ops) = args.flag_total_ops {
        assert!(!args.flag_burn_in, "don't set both --burn-in and --total-ops");
        while TOTAL.load(Ordering::Relaxed) < ops {
            thread::sleep(std::time::Duration::from_millis(50));
        }
        shutdown.store(true, Ordering::SeqCst);
    } else if !args.flag_burn_in {
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

#[cfg(feature = "logging")]
pub fn setup_logger() {
    use std::io::Write;

    color_backtrace::install();

    fn tn() -> String {
        std::thread::current().name().unwrap_or("unknown").to_owned()
    }

    let mut builder = env_logger::Builder::new();
    builder
        .format(|buf, record| {
            writeln!(
                buf,
                "{:05} {:25} {:10} {}",
                record.level(),
                tn(),
                record.module_path().unwrap().split("::").last().unwrap(),
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Info);

    if std::env::var("RUST_LOG").is_ok() {
        builder.parse(&std::env::var("RUST_LOG").unwrap());
    }

    let _r = builder.try_init();
}
