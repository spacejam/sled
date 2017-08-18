#[macro_use]
extern crate serde_derive;
extern crate rsdb;
extern crate docopt;
extern crate chan_signal;
extern crate rand;

use std::thread;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chan_signal::Signal;
use docopt::Docopt;
use rand::{Rng, thread_rng};

const USAGE: &'static str = "
Usage: stress [--threads=<#>] [--burn-in] [--duration=<s>]

Options:
    --threads=<#>      Number of threads [default: 4].
    --burn-in          Don't halt until we receive a signal.
    --duration=<s>     Seconds to run for [default: 20].
";

#[derive(Deserialize)]
struct Args {
    flag_threads: usize,
    flag_burn_in: bool,
    flag_duration: u64,
}

fn run(tree: Arc<rsdb::Tree>, shutdown: Arc<AtomicBool>) {
    let mut rng = thread_rng();
    let mut byte = || vec![rng.gen::<u8>()];
    let mut rng = thread_rng();

    while !shutdown.load(Ordering::Relaxed) {
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
                //tree.cas(byte(), Some(byte()), Some(byte()));
            }
            4 => {
                //tree.scan(&*byte()) .take(rng.gen_range(0, 256)) .collect::<Vec<_>>();
            }
            _ => (), // panic!("impossible choice"),
        }

    }
}

fn main() {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.argv(std::env::args().into_iter()).deserialize())
        .unwrap_or_else(|e| e.exit());

    let signal = chan_signal::notify(&[Signal::INT, Signal::TERM]);
    let shutdown = Arc::new(AtomicBool::new(false));

    let nonce: String = thread_rng().gen_ascii_chars().take(10).collect();
    let path = format!("rsdb_stress_{}", nonce);
    let config = rsdb::Config::default()
        .io_bufs(2)
        .io_buf_size(2 << 13)
        .blink_fanout(3)
        .page_consolidation_threshold(3)
        .cache_bits(2)
        .cache_capacity(2 << 14)
        .flush_every_ms(Some(30))
        .snapshot_after_ops(1000)
        .path(Some(path));
    let tree = Arc::new(config.tree());

    let mut threads = vec![];

    for _ in 0..args.flag_threads {
        let tree = tree.clone();
        let shutdown = shutdown.clone();
        let t = thread::spawn(move || run(tree, shutdown));
        threads.push(t);
    }

    if args.flag_burn_in {
        signal.recv();
        println!("got shutdown signal, cleaning up...");
    } else {
        thread::sleep(std::time::Duration::from_secs(args.flag_duration));
    }

    shutdown.store(true, Ordering::Release);

    for t in threads.into_iter() {
        t.join().unwrap();
    }

    tree.__delete_all_files();
}
