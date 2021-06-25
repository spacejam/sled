use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
};

#[cfg(feature = "dh")]
use dhat::{Dhat, DhatAlloc};

use num_format::{Locale, ToFormattedString};
use rand::{thread_rng, Rng};

#[cfg(feature = "jemalloc")]
mod alloc {
    use jemallocator::Jemalloc;
    use std::alloc::Layout;

    #[global_allocator]
    static ALLOCATOR: Jemalloc = Jemalloc;
}

#[cfg(feature = "memshred")]
mod alloc {
    use std::alloc::{Layout, System};

    #[global_allocator]
    static ALLOCATOR: Alloc = Alloc;

    #[derive(Default, Debug, Clone, Copy)]
    struct Alloc;

    unsafe impl std::alloc::GlobalAlloc for Alloc {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ret = System.alloc(layout);
            assert_ne!(ret, std::ptr::null_mut());
            std::ptr::write_bytes(ret, 0xa1, layout.size());
            ret
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            std::ptr::write_bytes(ptr, 0xde, layout.size());
            System.dealloc(ptr, layout)
        }
    }
}

#[cfg(feature = "measure_allocs")]
mod alloc {
    use std::alloc::{Layout, System};
    use std::sync::atomic::{AtomicUsize, Ordering::Release};

    pub static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
    pub static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);

    #[global_allocator]
    static ALLOCATOR: Alloc = Alloc;

    #[derive(Default, Debug, Clone, Copy)]
    struct Alloc;

    unsafe impl std::alloc::GlobalAlloc for Alloc {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            ALLOCATIONS.fetch_add(1, Release);
            ALLOCATED_BYTES.fetch_add(layout.size(), Release);
            System.alloc(layout)
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            System.dealloc(ptr, layout)
        }
    }
}

#[global_allocator]
#[cfg(feature = "dh")]
static ALLOCATOR: DhatAlloc = DhatAlloc;

static TOTAL: AtomicUsize = AtomicUsize::new(0);

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
    [--total-ops=<n>] \
    [--flush-every=<ms>]

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
    --flush-every=<m>  Flush and sync the database every ms [default: 200].
    --cache-mb=<mb>    Size of the page cache in megabytes [default: 1024].
";

#[derive(Debug, Clone, Copy)]
struct Args {
    threads: usize,
    burn_in: bool,
    duration: u64,
    key_len: usize,
    val_len: usize,
    get_prop: usize,
    set_prop: usize,
    del_prop: usize,
    cas_prop: usize,
    scan_prop: usize,
    merge_prop: usize,
    entries: usize,
    sequential: bool,
    total_ops: Option<usize>,
    flush_every: u64,
    cache_mb: usize,
}

impl Default for Args {
    fn default() -> Args {
        Args {
            threads: 4,
            burn_in: false,
            duration: 10,
            key_len: 10,
            val_len: 100,
            get_prop: 94,
            set_prop: 2,
            del_prop: 1,
            cas_prop: 1,
            scan_prop: 1,
            merge_prop: 1,
            entries: 100000,
            sequential: false,
            total_ops: None,
            flush_every: 200,
            cache_mb: 1024,
        }
    }
}

fn parse<'a, I, T>(mut iter: I) -> T
where
    I: Iterator<Item = &'a str>,
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Debug,
{
    iter.next().expect(USAGE).parse().expect(USAGE)
}

impl Args {
    fn parse() -> Args {
        let mut args = Args::default();
        for raw_arg in std::env::args().skip(1) {
            let mut splits = raw_arg[2..].split('=');
            match splits.next().unwrap() {
                "threads" => args.threads = parse(&mut splits),
                "burn-in" => args.burn_in = true,
                "duration" => args.duration = parse(&mut splits),
                "key-len" => args.key_len = parse(&mut splits),
                "val-len" => args.val_len = parse(&mut splits),
                "get-prop" => args.get_prop = parse(&mut splits),
                "set-prop" => args.set_prop = parse(&mut splits),
                "del-prop" => args.del_prop = parse(&mut splits),
                "cas-prop" => args.cas_prop = parse(&mut splits),
                "scan-prop" => args.scan_prop = parse(&mut splits),
                "merge-prop" => args.merge_prop = parse(&mut splits),
                "entries" => args.entries = parse(&mut splits),
                "sequential" => args.sequential = true,
                "total-ops" => args.total_ops = Some(parse(&mut splits)),
                "flush-every" => args.flush_every = parse(&mut splits),
                "cache-mb" => args.cache_mb = parse(&mut splits),
                other => panic!("unknown option: {}, {}", other, USAGE),
            }
        }
        args
    }
}

fn report(shutdown: Arc<AtomicBool>) {
    let mut last = 0;
    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(std::time::Duration::from_secs(1));
        let total = TOTAL.load(Ordering::Acquire);

        println!(
            "did {} ops, {}mb RSS",
            (total - last).to_formatted_string(&Locale::en),
            rss() / (1024 * 1024)
        );

        last = total;
    }
}

fn concatenate_merge(
    _key: &[u8],              // the key being merged
    old_value: Option<&[u8]>, // the previous value, if one existed
    merged_bytes: &[u8],      // the new bytes being merged in
) -> Option<Vec<u8>> {
    // set the new value, return None to delete
    let mut ret = old_value.map(|ov| ov.to_vec()).unwrap_or_else(Vec::new);

    ret.extend_from_slice(merged_bytes);

    Some(ret)
}

fn run(args: Args, tree: Arc<sled::Db>, shutdown: Arc<AtomicBool>) {
    let get_max = args.get_prop;
    let set_max = get_max + args.set_prop;
    let del_max = set_max + args.del_prop;
    let cas_max = del_max + args.cas_prop;
    let merge_max = cas_max + args.merge_prop;
    let scan_max = merge_max + args.scan_prop;

    let keygen = |len| -> sled::IVec {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let i = if args.sequential {
            SEQ.fetch_add(1, Ordering::Relaxed)
        } else {
            thread_rng().gen::<usize>()
        } % args.entries;

        let start = if len < 8 { 8 - len } else { 0 };

        let i_keygen = &i.to_be_bytes()[start..];

        i_keygen.iter().cycle().take(len).copied().collect()
    };

    let valgen = |len| -> sled::IVec {
        if len == 0 {
            return vec![].into();
        }

        let i: usize = thread_rng().gen::<usize>() % (len * 8);

        let i_keygen = i.to_be_bytes();

        i_keygen
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
        let key = keygen(args.key_len);
        let choice = rng.gen_range(0, scan_max + 1);

        match choice {
            v if v <= get_max => {
                tree.get_zero_copy(&key, |_| {}).unwrap();
            }
            v if v > get_max && v <= set_max => {
                let value = valgen(args.val_len);
                tree.insert(&key, value).unwrap();
            }
            v if v > set_max && v <= del_max => {
                tree.remove(&key).unwrap();
            }
            v if v > del_max && v <= cas_max => {
                let old = if rng.gen::<bool>() {
                    let value = valgen(args.val_len);
                    Some(value)
                } else {
                    None
                };

                let new = if rng.gen::<bool>() {
                    let value = valgen(args.val_len);
                    Some(value)
                } else {
                    None
                };

                if let Err(e) = tree.compare_and_swap(&key, old, new) {
                    panic!("operational error: {:?}", e);
                }
            }
            v if v > cas_max && v <= merge_max => {
                let value = valgen(args.val_len);
                tree.merge(&key, value).unwrap();
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

    #[cfg(feature = "dh")]
    let _dh = Dhat::start_heap_profiling();

    let args = Args::parse();

    let shutdown = Arc::new(AtomicBool::new(false));

    dbg!(args);

    let config = sled::Config::new()
        .cache_capacity(args.cache_mb * 1024 * 1024)
        .flush_every_ms(if args.flush_every == 0 {
            None
        } else {
            Some(args.flush_every)
        });

    let tree = Arc::new(config.open().unwrap());
    tree.set_merge_operator(concatenate_merge);

    let mut threads = vec![];

    let now = std::time::Instant::now();

    let n_threads = args.threads;

    for i in 0..=n_threads {
        let tree = tree.clone();
        let shutdown = shutdown.clone();

        let t = if i == 0 {
            thread::Builder::new()
                .name("reporter".into())
                .spawn(move || report(shutdown))
                .unwrap()
        } else {
            thread::spawn(move || run(args, tree, shutdown))
        };

        threads.push(t);
    }

    if let Some(ops) = args.total_ops {
        assert!(!args.burn_in, "don't set both --burn-in and --total-ops");
        while TOTAL.load(Ordering::Relaxed) < ops {
            thread::sleep(std::time::Duration::from_millis(50));
        }
        shutdown.store(true, Ordering::SeqCst);
    } else if !args.burn_in {
        thread::sleep(std::time::Duration::from_secs(args.duration));
        shutdown.store(true, Ordering::SeqCst);
    }

    for t in threads.into_iter() {
        t.join().unwrap();
    }
    let ops = TOTAL.load(Ordering::SeqCst);
    let time = now.elapsed().as_secs() as usize;

    println!(
        "did {} total ops in {} seconds. {} ops/s",
        ops.to_formatted_string(&Locale::en),
        time,
        ((ops * 1_000) / (time * 1_000)).to_formatted_string(&Locale::en)
    );

    #[cfg(feature = "measure_allocs")]
    println!(
        "allocated {} bytes in {} allocations",
        alloc::ALLOCATED_BYTES
            .load(Ordering::Acquire)
            .to_formatted_string(&Locale::en),
        alloc::ALLOCATIONS
            .load(Ordering::Acquire)
            .to_formatted_string(&Locale::en),
    );

    #[cfg(feature = "metrics")]
    sled::print_profile();
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

    if let Ok(env) = std::env::var("RUST_LOG") {
        builder.parse_filters(&env);
    }

    let _r = builder.try_init();
}
