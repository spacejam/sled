extern crate rsdb;
extern crate rand;

#[cfg(feature = "stress")]
#[macro_use]
extern crate serde_derive;

#[cfg(feature = "stress")]
extern crate docopt;

#[cfg(feature = "stress")]
extern crate chan_signal;

use std::thread;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[cfg(feature = "stress")]
use chan_signal::Signal;

#[cfg(feature = "stress")]
use docopt::Docopt;

use rand::{Rng, thread_rng};

const USAGE: &'static str = "
Usage: stress [--threads=<#>] [--burn-in] [--duration=<s>]

Options:
    --threads=<#>      Number of threads [default: 4].
    --burn-in          Don't halt until we receive a signal.
    --duration=<s>     Seconds to run for [default: 10].
";

#[cfg_attr(feature = "docopt", derive(Deserialize))]
struct Args {
    flag_threads: usize,
    flag_burn_in: bool,
    flag_duration: u64,
}

fn run(tree: Arc<rsdb::Tree>, shutdown: Arc<AtomicBool>, total: Arc<AtomicUsize>) {
    let mut rng = thread_rng();
    let mut byte = || vec![rng.gen::<u8>()];
    let mut rng = thread_rng();

    let mut ops = 0;

    while !shutdown.load(Ordering::Relaxed) {
        ops += 1;
        let choice = rng.gen_range(0, 5);

        match choice {
            0 => {
                tree.set(byte(), byte());
            }
            1 => {
                tree.get(&*byte());
            }
            2 => {
                tree.del(&*byte());
            }
            3 => {
                if let Err(_) = tree.cas(byte(), Some(byte()), Some(byte())) {};
            }
            4 => {
                tree.scan(&*byte())
                    .take(rng.gen_range(0, 15))
                    .collect::<Vec<_>>();
            }
            _ => panic!("impossible choice"),
        }

    }

    total.fetch_add(ops, Ordering::Release);
}

fn main() {
    #[cfg(feature = "stress")]
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.argv(std::env::args().into_iter()).deserialize())
        .unwrap_or_else(|e| e.exit());

    #[cfg(feature = "stress")]
    let signal = chan_signal::notify(&[Signal::INT, Signal::TERM]);

    let total = Arc::new(AtomicUsize::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));

    let nonce: String = thread_rng().gen_ascii_chars().take(10).collect();
    let path = format!("rsdb_stress_{}", nonce);
    let config = rsdb::Config::default()
        .io_bufs(2)
        .io_buf_size(2 << 13)
        .blink_fanout(3)
        .page_consolidation_threshold(3)
        .cache_bits(2)
        .cache_capacity(1_000_000)
        .flush_every_ms(Some(30))
        .snapshot_after_ops(1000)
        .path(Some(path));
    let tree = Arc::new(config.tree());

    let mut threads = vec![];

    let now = std::time::Instant::now();

    #[cfg(feature = "stress")]
    let n_threads = args.flag_threads;

    #[cfg(not(feature = "stress"))]
    let n_threads = 4;

    for _ in 0..n_threads {
        let tree = tree.clone();
        let shutdown = shutdown.clone();
        let total = total.clone();
        let t = thread::spawn(move || run(tree, shutdown, total));
        threads.push(t);
    }

    #[cfg(feature = "stress")]
    {
        if args.flag_burn_in {
            signal.recv();
            println!("got shutdown signal, cleaning up...");
        } else {
            thread::sleep(std::time::Duration::from_secs(args.flag_duration));
        }
    }

    #[cfg(not(feature = "stress"))] thread::sleep(std::time::Duration::from_secs(10));

    shutdown.store(true, Ordering::SeqCst);

    for t in threads.into_iter() {
        t.join().unwrap();
    }

    let ops = total.load(Ordering::SeqCst);
    let time = now.elapsed().as_secs() as usize;

    println!("did {} total ops in {} seconds. {} ops/s", ops, time, ops / time);

    tree.__delete_all_files();
}
