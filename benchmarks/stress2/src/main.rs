#[macro_use]
extern crate serde_derive;
extern crate chan_signal;
extern crate docopt;
extern crate rand;
extern crate sled;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use chan_signal::Signal;
use docopt::Docopt;
use rand::{thread_rng, Rng};

const USAGE: &'static str = "
Usage: stress [--threads=<#>] [--burn-in] [--duration=<s>] [--kv-len=<l>]

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
};

fn report(shutdown: Arc<AtomicBool>, total: Arc<AtomicUsize>) {
    let mut last = 0;
    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(std::time::Duration::from_secs(1));
        let total = total.load(Ordering::Acquire);

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

fn run(
    tree: Arc<sled::Tree>,
    shutdown: Arc<AtomicBool>,
    total: Arc<AtomicUsize>,
) {
    let args = unsafe { ARGS.clone() };

    let bytes = |len| {
        thread_rng().gen_iter::<u8>().take(len).collect::<Vec<_>>()
    };
    let mut rng = thread_rng();

    while !shutdown.load(Ordering::Relaxed) {
        total.fetch_add(1, Ordering::Release);
        let key = bytes(args.flag_key_len);

        let get_max = args.flag_get_prop;
        let set_max = get_max + args.flag_set_prop;
        let del_max = set_max + args.flag_del_prop;
        let cas_max = del_max + args.flag_cas_prop;
        let scan_max = cas_max + args.flag_scan_prop;
        let merge_max = scan_max + args.flag_merge_prop;

        let choice = rng.gen_range(0, merge_max + 1);

        match choice {
            v if v <= get_max => {
                tree.get(&*key).unwrap();
            }
            v if v <= set_max => {
                tree.set(key, bytes(args.flag_val_len)).unwrap();
            }
            v if v <= del_max => {
                tree.del(&*key).unwrap();
            }
            v if v <= cas_max => match tree.cas(
                key,
                Some(&*bytes(args.flag_val_len)),
                Some(bytes(args.flag_val_len)),
            ) {
                Ok(_) | Err(sled::Error::CasFailed(_)) => {}
                other => panic!("operational error: {:?}", other),
            },
            v if v <= scan_max => {
                let _ = tree
                    .scan(&*key)
                    .take(rng.gen_range(0, 15))
                    .map(|res| res.unwrap())
                    .collect::<Vec<_>>();
            }
            _ => {
                tree.merge(key, bytes(args.flag_val_len)).unwrap();
            }
        }
    }
}

fn main() {
    let signal = chan_signal::notify(&[Signal::INT, Signal::TERM]);

    let args = unsafe {
        ARGS = Docopt::new(USAGE)
            .and_then(|d| {
                d.argv(std::env::args().into_iter()).deserialize()
            }).unwrap_or_else(|e| e.exit());
        ARGS.clone()
    };

    let total = Arc::new(AtomicUsize::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));

    let config = sled::ConfigBuilder::new()
        .io_bufs(2)
        .io_buf_size(8_000_000)
        .blink_node_split_size(4096)
        .page_consolidation_threshold(10)
        .cache_bits(6)
        .cache_capacity(1_000_000_000)
        .flush_every_ms(Some(10))
        .snapshot_after_ops(100000000)
        .print_profile_on_drop(true)
        .merge_operator(concatenate_merge)
        .build();

    let tree = Arc::new(sled::Tree::start(config).unwrap());

    let mut threads = vec![];

    let now = std::time::Instant::now();

    let n_threads = args.flag_threads;

    for i in 0..n_threads + 1 {
        let tree = tree.clone();
        let shutdown = shutdown.clone();
        let total = total.clone();

        let t = if i == 0 {
            thread::spawn(move || report(shutdown, total))
        } else {
            thread::spawn(move || run(tree, shutdown, total))
        };

        threads.push(t);
    }

    if unsafe { ARGS.flag_burn_in } {
        println!("waiting on signal");
        signal.recv();
        println!("got shutdown signal, cleaning up...");
    } else {
        thread::sleep(std::time::Duration::from_secs(unsafe {
            ARGS.flag_duration
        }));
    }

    shutdown.store(true, Ordering::SeqCst);

    for t in threads.into_iter() {
        t.join().unwrap();
    }

    let ops = total.load(Ordering::SeqCst);
    let time = now.elapsed().as_secs() as usize;

    println!(
        "did {} total ops in {} seconds. {} ops/s",
        ops,
        time,
        (ops * 1_000) / (time * 1_000)
    );
}
