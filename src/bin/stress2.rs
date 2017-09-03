#[macro_use]
extern crate serde_derive;
extern crate docopt;
extern crate chan_signal;
extern crate rand;
extern crate rsdb;

use std::mem;
use std::thread;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use chan_signal::Signal;
use docopt::Docopt;
use rand::{Rng, thread_rng};

const USAGE: &'static str = "
Usage: stress2 [--threads=<#>] [--burn-in] [--duration=<s>]

Options:
    --burn-in          Don't halt until we receive a signal.
    --duration=<s>     Seconds to run for [default: 10].
";

#[derive(Deserialize)]
struct Args {
    flag_burn_in: bool,
    flag_duration: u64,
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
    vec![thread_rng().gen::<u8>()]
}

fn do_set(tree: Arc<rsdb::Tree>, shutdown: Arc<AtomicBool>, total: Arc<AtomicUsize>) {
    while !shutdown.load(Ordering::Relaxed) {
        total.fetch_add(1, Ordering::Release);
        tree.set(byte(), byte());
    }
}

fn do_get(tree: Arc<rsdb::Tree>, shutdown: Arc<AtomicBool>, total: Arc<AtomicUsize>) {
    let mut k = 0;
    while !shutdown.load(Ordering::Relaxed) {
        total.fetch_add(1, Ordering::Release);
        tree.get(&*vec![k]);
        k = if k == 256 { 0 } else { k + 1 };
    }
}

fn do_del(tree: Arc<rsdb::Tree>, shutdown: Arc<AtomicBool>, total: Arc<AtomicUsize>) {
    while !shutdown.load(Ordering::Relaxed) {
        total.fetch_add(1, Ordering::Release);
        tree.del(&*byte());
    }
}

fn do_cas(tree: Arc<rsdb::Tree>, shutdown: Arc<AtomicBool>, total: Arc<AtomicUsize>) {
    while !shutdown.load(Ordering::Relaxed) {
        total.fetch_add(1, Ordering::Release);
        if let Err(_) = tree.cas(byte(), Some(byte()), Some(byte())) {};
    }
}

fn do_scan(tree: Arc<rsdb::Tree>, shutdown: Arc<AtomicBool>, total: Arc<AtomicUsize>) {
    while !shutdown.load(Ordering::Relaxed) {
        total.fetch_add(1, Ordering::Release);
        tree.scan(&*byte())
            .take(thread_rng().gen_range(1, 16))
            .collect::<Vec<_>>();
    }
}

fn prepopulate(tree: Arc<rsdb::Tree>, keys: usize) {
    for i in 0..keys {
        let bytes: [u8; 8] = unsafe { mem::transmute(i) };
        let k = bytes.to_vec();
        let v = vec![];
        tree.set(k, v);
    }
}

fn main() {
    let signal = chan_signal::notify(&[Signal::INT, Signal::TERM]);

    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.argv(std::env::args().into_iter()).deserialize())
        .unwrap_or_else(|e| e.exit());

    let total = Arc::new(AtomicUsize::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));

    let config = rsdb::Config::default()
        .io_bufs(2)
        .io_buf_size(8_000_000)
        .blink_fanout(15)
        .page_consolidation_threshold(10)
        .cache_fixup_threshold(2)
        .cache_bits(6)
        .cache_capacity(128_000_000)
        .flush_every_ms(Some(100))
        .snapshot_after_ops(100000000);

    let tree = Arc::new(config.tree());

    macro_rules! cloned {
        ($f:expr) => {{
            let tree = tree.clone();
            let shutdown = shutdown.clone();
            let total = total.clone();
            thread::spawn(|| $f(tree, shutdown, total))
        }};
    }

    prepopulate(tree.clone(), 256);

    let threads = vec![
        cloned!(|_, shutdown, total| report(shutdown, total)),
        cloned!(|tree, shutdown, total| do_get(tree, shutdown, total)),
        cloned!(|tree, shutdown, total| do_set(tree, shutdown, total)),
        cloned!(|tree, shutdown, total| do_del(tree, shutdown, total)),
        cloned!(|tree, shutdown, total| do_cas(tree, shutdown, total)),
        cloned!(|tree, shutdown, total| do_scan(tree, shutdown, total)),
    ];

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

    println!("did {} total ops in {} seconds. {} ops/s", ops, time, ops / time);
}
