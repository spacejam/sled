#[macro_use]
extern crate serde_derive;
extern crate docopt;
extern crate chan_signal;
extern crate rand;
extern crate sled;

use std::mem;
use std::thread;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use chan_signal::Signal;
use docopt::Docopt;
use rand::{Rng, thread_rng};

static mut KEY_BYTES: usize = 2;

const USAGE: &'static str = "
Usage: stress2 [options]

Options:
    --burn-in          Don't halt until we receive a signal.
    --duration=<s>     Seconds to run for [default: 10].
    --get=<threads>    Threads spinning on get operations [default: 1].
    --set=<threads>    Threads spinning on set operations [default: 1].
    --del=<threads>    Threads spinning on del operations [default: 0].
    --cas=<threads>    Threads spinning on cas operations [default: 0].
    --scan=<threads>   Threads spinning on scan operations [default: 0].
    --key-len=<bytes>  Length of keys and values [default: 2].
";

#[derive(Deserialize)]
struct Args {
    flag_burn_in: bool,
    flag_duration: u64,
    flag_get: usize,
    flag_set: usize,
    flag_del: usize,
    flag_cas: usize,
    flag_scan: usize,
    flag_key_len: usize,
}

fn report(shutdown: Arc<AtomicBool>, total: Arc<AtomicUsize>) {
    let mut last = 0;
    while !shutdown.load(Ordering::Relaxed) {
        thread::sleep(std::time::Duration::from_secs(1));
        let total = total.load(Ordering::Acquire);

        println!("did {} ops", total - last);

        last = total;
    }
}

fn byte() -> Vec<u8> {
    let mut v = unsafe { vec![0; KEY_BYTES] };
    thread_rng().fill_bytes(&mut v);
    v
}

fn prepopulate(tree: Arc<sled::Tree>) {
    let max = 256_usize.pow(unsafe { KEY_BYTES as u32 });
    for i in 0..max {
        let bytes: [u8; 8] = unsafe { mem::transmute(i) };
        let k = bytes[8 - unsafe { KEY_BYTES }..8].to_vec();
        let v = vec![];
        tree.set(k, v);
    }
}

fn main() {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.argv(std::env::args().into_iter()).deserialize())
        .unwrap_or_else(|e| e.exit());

    unsafe {
        KEY_BYTES = args.flag_key_len;
    }

    let total = Arc::new(AtomicUsize::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));

    let config = sled::Config::default()
        .io_bufs(2)
        .blink_fanout(15)
        .page_consolidation_threshold(10)
        .cache_fixup_threshold(1)
        .cache_bits(6)
        .cache_capacity(128 * 1024 * 1024)
        .flush_every_ms(None)
        // .io_buf_size(1 << 16)
        .path("stress2.db".to_string())
        .snapshot_after_ops(1 << 16);

    println!("recovering");
    let tree = Arc::new(config.tree());

    macro_rules! cloned {
        ($f:expr) => {{
            let tree = tree.clone();
            let total = total.clone();
            let shutdown = shutdown.clone();
            thread::spawn(|| $f(tree, shutdown, total))
        }};
    }

    println!("prepopulating");
    prepopulate(tree.clone());

    let mut threads =
        vec![cloned!(|_, shutdown, total| report(shutdown, total))];

    macro_rules! spin_up {
        ($($n:expr, $fn:expr;)*)=> (
            $(
            for _ in 0..$n {
                let tree = tree.clone();
                let shutdown = shutdown.clone();
                let total = total.clone();
                let thread = thread::Builder::new()
                    .stack_size(2 << 23) // make some bigass 16mb stacks
                    .spawn(move || {
                    while !shutdown.load(Ordering::Relaxed) {
                        total.fetch_add(1, Ordering::Release);
                        $fn(&tree);
                    }
                }).unwrap();
                threads.push(thread);
            }
            )*
        );
    }

    println!("spinning up threads");
    #[rustfmt_skip]
    spin_up![
        args.flag_get, |t: &Arc<sled::Tree>| t.get(&*byte());
        args.flag_set, |t: &Arc<sled::Tree>| t.set(byte(), byte());
        args.flag_del, |t: &Arc<sled::Tree>| t.del(&*byte());
        args.flag_cas, |t: &Arc<sled::Tree>| {
            let _ = t.cas(byte(), Some(byte()), Some(byte()));
        };
        args.flag_scan, |t: &Arc<sled::Tree>| t.scan(&*byte())
            .take(thread_rng().gen_range(1, 3))
            .collect::<Vec<_>>();
    ];

    let now = std::time::Instant::now();

    if args.flag_burn_in {
        let signal = chan_signal::notify(&[Signal::INT, Signal::TERM]);

        println!("burning in");
        signal.recv();
        println!("got shutdown signal, cleaning up...");
    } else {
        thread::sleep(std::time::Duration::from_secs(args.flag_duration));
    }

    shutdown.store(true, Ordering::SeqCst);

    for t in threads.into_iter() {
        let _ = t.join();
    }

    let ops = total.load(Ordering::SeqCst);
    let time = now.elapsed().as_secs() as usize;

    sled::M.print_profile();

    println!(
        "did {} total ops in {} seconds. {} ops/s",
        ops,
        time,
        (ops * 1_000) / (time * 1_000)
    );
}
