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

fn do_set(tree: Arc<sled::Tree>, shutdown: Arc<AtomicBool>, total: Arc<AtomicUsize>) {
    while !shutdown.load(Ordering::Relaxed) {
        total.fetch_add(1, Ordering::Release);
        tree.set(byte(), byte());
    }
}

fn do_get(tree: Arc<sled::Tree>, shutdown: Arc<AtomicBool>, total: Arc<AtomicUsize>) {
    while !shutdown.load(Ordering::Relaxed) {
        total.fetch_add(1, Ordering::Release);
        tree.get(&*byte());
    }
}

fn do_del(tree: Arc<sled::Tree>, shutdown: Arc<AtomicBool>, total: Arc<AtomicUsize>) {
    while !shutdown.load(Ordering::Relaxed) {
        total.fetch_add(1, Ordering::Release);
        tree.del(&*byte());
    }
}

fn do_cas(tree: Arc<sled::Tree>, shutdown: Arc<AtomicBool>, total: Arc<AtomicUsize>) {
    while !shutdown.load(Ordering::Relaxed) {
        total.fetch_add(1, Ordering::Release);
        if let Err(_) = tree.cas(byte(), Some(byte()), Some(byte())) {};
    }
}

fn do_scan(tree: Arc<sled::Tree>, shutdown: Arc<AtomicBool>, total: Arc<AtomicUsize>) {
    while !shutdown.load(Ordering::Relaxed) {
        total.fetch_add(1, Ordering::Release);
        tree.scan(&*byte())
            .take(thread_rng().gen_range(1, 3))
            .collect::<Vec<_>>();
    }
}

fn prepopulate(tree: Arc<sled::Tree>) {
    for i in 0..256_usize.pow(unsafe { KEY_BYTES as u32 }) {
        let bytes: [u8; 8] = unsafe { mem::transmute(i) };
        let k = bytes[8 - unsafe { KEY_BYTES }..8].to_vec();
        let v = vec![];
        tree.set(k, v);
    }
}

fn main() {
    let signal = chan_signal::notify(&[Signal::INT, Signal::TERM]);

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
        .cache_fixup_threshold(2)
        .cache_bits(6)
        .cache_capacity(128_000_000)
        .flush_every_ms(None)
        .snapshot_after_ops(1000000);

    let tree = Arc::new(config.tree());

    macro_rules! cloned {
        ($f:expr) => {{
            let tree = tree.clone();
            let shutdown = shutdown.clone();
            let total = total.clone();
            thread::spawn(|| $f(tree, shutdown, total))
        }};
    }

    prepopulate(tree.clone());

    let mut threads = vec![cloned!(|_, shutdown, total| report(shutdown, total))];

    for _ in 0..args.flag_get {
        threads.push(cloned!(|tree, shutdown, total| do_get(tree, shutdown, total)));
    }

    for _ in 0..args.flag_set {
        threads.push(cloned!(|tree, shutdown, total| do_set(tree, shutdown, total)));
    }

    for _ in 0..args.flag_del {
        threads.push(cloned!(|tree, shutdown, total| do_del(tree, shutdown, total)));
    }

    for _ in 0..args.flag_cas {
        threads.push(cloned!(|tree, shutdown, total| do_cas(tree, shutdown, total)));
    }

    for _ in 0..args.flag_scan {
        threads.push(cloned!(|tree, shutdown, total| do_scan(tree, shutdown, total)));
    }


    let now = std::time::Instant::now();

    if args.flag_burn_in {
        signal.recv();
        println!("got shutdown signal, cleaning up...");
    } else {
        thread::sleep(std::time::Duration::from_secs(args.flag_duration));
    }

    shutdown.store(true, Ordering::SeqCst);

    for t in threads.into_iter() {
        t.join().unwrap();
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
