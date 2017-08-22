extern crate clap;
extern crate num_cpus;
#[macro_use]
extern crate log;
extern crate rand;
extern crate sled;

use std::error::Error;
use std::io::prelude::*;
use std::process;
use std::collections::HashMap;
use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use clap::{App, Arg};
use rand::Rng;

use sled::Tree;

fn main() {
    let cpus = &(num_cpus::get().to_string());

    let matches = App::new("sled bench")
        .version("0.1.0")
        .about("sled benchmarking tool")
        .author("Tyler Neely, Philipp Muens")
        .arg(Arg::with_name("num_threads")
            .short("t")
            .long("num-threads")
            .help("Number of threads to use")
            .default_value(cpus)
            .takes_value(true))
        .arg(Arg::with_name("num_operations")
            .short("o")
            .long("num-operations")
            .help("Number of total operations")
            .default_value("1000000")
            .takes_value(true))
        .arg(Arg::with_name("freshness_bias")
            .long("freshness-bias")
            .help("Freshness bias")
            .possible_value("old")
            .possible_value("new")
            .possible_value("random")
            .default_value("random")
            .takes_value(true))
        .arg(Arg::with_name("non_present_key_chance")
            .long("non-present-key-chance")
            .help("The chance that a request may be sent for a key that does not exist")
            .default_value("true")
            .takes_value(true))
        // proportions
        .arg(Arg::with_name("set")
            .long("set")
            .help("Proportion of sets")
            .default_value("0")
            .takes_value(true))
        .arg(Arg::with_name("scan")
            .long("scan")
            .help("Proportion of scan")
            .default_value("0")
            .takes_value(true))
        .arg(Arg::with_name("get")
            .long("get")
            .help("Proportion of gets")
            .default_value("0")
            .takes_value(true))
        .arg(Arg::with_name("delete")
            .long("del")
            .help("Proportion of delete")
            .default_value("0")
            .takes_value(true))
        .arg(Arg::with_name("cas")
            .long("cas")
            .help("Proportion of cas")
            .default_value("0")
            .takes_value(true))
        // key sizes
        .arg(Arg::with_name("key_size_min")
            .long("key-size-min")
            .help("Minimum key size")
            .default_value("5")
            .takes_value(true))
        .arg(Arg::with_name("key_size_max")
            .long("key-size-max")
            .help("Maximum key size")
            .default_value("5")
            .takes_value(true))
        .arg(Arg::with_name("key_size_median")
            .long("key-size-median")
            .help("Median key size")
            .default_value("5")
            .takes_value(true))
        // value sizes
        .arg(Arg::with_name("value_size_min")
            .long("value-size-min")
            .help("Minimum value size")
            .default_value("5")
            .takes_value(true))
        .arg(Arg::with_name("value_size_max")
            .long("value-size-max")
            .help("Maximum value size")
            .default_value("5")
            .takes_value(true))
        .arg(Arg::with_name("value_size_median")
            .long("value-size-median")
            .help("Median value size")
            .default_value("5")
            .takes_value(true))
        // scan iterrations
        .arg(Arg::with_name("scan_iter_min")
            .long("scan-iter-min")
            .help("Minimum scan iteration")
            .default_value("5")
            .takes_value(true))
        .arg(Arg::with_name("scan_iter_max")
            .long("scan-iter-max")
            .help("Maximum scan iterations")
            .default_value("5")
            .takes_value(true))
        .arg(Arg::with_name("scan_iter_median")
            .long("scan-iter-median")
            .help("Median scan iterations")
            .default_value("5")
            .takes_value(true))
        .get_matches();

    let mut args = HashMap::new();

    args.insert("num_threads", matches.value_of("num_threads").unwrap());
    args.insert("num_operations", matches.value_of("num_operations").unwrap());
    args.insert("freshness_bias", matches.value_of("freshness_bias").unwrap());
    args.insert(
        "non_present_key_chance",
        matches.value_of("non_present_key_chance").unwrap(),
    );
    args.insert("set", matches.value_of("set").unwrap());
    args.insert("scan", matches.value_of("scan").unwrap());
    args.insert("get", matches.value_of("get").unwrap());
    args.insert("delete", matches.value_of("delete").unwrap());
    args.insert("cas", matches.value_of("cas").unwrap());
    args.insert("key_size_min", matches.value_of("key_size_min").unwrap());
    args.insert("key_size_max", matches.value_of("key_size_max").unwrap());
    args.insert("key_size_median", matches.value_of("key_size_median").unwrap());
    args.insert("value_size_min", matches.value_of("value_size_min").unwrap());
    args.insert("value_size_max", matches.value_of("value_size_max").unwrap());
    args.insert("value_size_median", matches.value_of("value_size_median").unwrap());
    args.insert("scan_iter_min", matches.value_of("scan_iter_min").unwrap());
    args.insert("scan_iter_max", matches.value_of("scan_iter_max").unwrap());
    args.insert("scan_iter_median", matches.value_of("scan_iter_median").unwrap());

    let mut stderr = std::io::stderr();

    let config = Config::new(&args).unwrap_or_else(|err| {
        writeln!(&mut stderr, "Problem parsing arguments: {}", err)
            .expect("Could not write to stderr");
        process::exit(1);
    });

    if let Err(e) = run(config) {
        writeln!(&mut stderr, "Application error: {}", e).expect("Could not write to stderr");
        process::exit(1);
    }
}

fn run(config: Config) -> Result<(), Box<Error>> {
    println!("running benchmarking suite...");

    // create a default sled config
    let sled_config = sled::Config::default();
    let tree = sled_config.tree();

    perform_tree_operations(tree, config);

    Ok(())
}

fn perform_tree_operations(tree: Tree, config: Config) {
    let sum_ops = config.set + config.scan + config.get + config.delete + config.cas;
    let ops = vec![
        (Op::Set, config.set),
        (Op::Scan, config.scan),
        (Op::Get, config.get),
        (Op::Delete, config.delete),
        (Op::Cas, config.cas),
    ];
    let ops_per_thread = config.num_operations / config.num_threads; // TODO update to handle division errors
    let mut threads = Vec::new();
    let ops_per_second = AtomicUsize::new(0);

    let tree = Arc::new(tree);
    let ops = Arc::new(ops);
    let ops_per_second = Arc::new(ops_per_second);

    // thread which spits out bench-related results every 1 second
    {
        let ops_per_second = ops_per_second.clone();
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(1));
                let ops_per_second = ops_per_second.swap(0, Ordering::Relaxed);
                println!("throughput: {}", ops_per_second);
            }
        });
    }

    for i in 0..config.num_threads {
        let tree = tree.clone();
        let ops = ops.clone();
        let ops_per_second = ops_per_second.clone();
        let t = thread::spawn(move || for _ in 0..ops_per_thread {
            let op_to_perform = get_operation_choice(&ops, sum_ops);

            match op_to_perform {
                Some(&Op::Set) => perform_set_operation(&tree),
                Some(&Op::Scan) => perform_scan_operation(&tree),
                Some(&Op::Get) => perform_get_operation(&tree),
                Some(&Op::Delete) => perform_delete_operation(&tree),
                Some(&Op::Cas) => perform_cas_operation(&tree),
                _ => (),
            };

            ops_per_second.fetch_add(1, Ordering::Relaxed);
        });
        threads.push(t);
    }

    for t in threads.into_iter() {
        t.join();
    }
}

fn perform_set_operation(tree: &Tree) {
    info!("Performing set operation");
    let kv = KV::new();

    tree.set(kv.key, kv.value);
}

fn perform_scan_operation(tree: &Tree) {
    info!("Performing scan operation");
    let kv = KV::new();

    tree.scan(&kv.key);
}

fn perform_get_operation(tree: &Tree) {
    info!("Performing get operation");
    let kv = KV::new();

    tree.get(&kv.key);
}

fn perform_delete_operation(tree: &Tree) {
    info!("Performing delete operation");
    let kv = KV::new();

    tree.del(&kv.key);
}

fn perform_cas_operation(tree: &Tree) {
    info!("Performing cas operation for key");
    let kv = KV::new();

    let old_value = kv.value;
    let new_value = KV::new().value;

    tree.cas(kv.key, Some(old_value), Some(new_value));
}

fn get_operation_choice(ops: &Vec<(Op, usize)>, sum_ops: usize) -> Option<&Op> {
    let mut choice = rand::thread_rng().gen_range::<usize>(0, sum_ops);

    for &(ref op, ref weight) in ops {
        if *weight >= choice {
            return Some(op);
        }
        choice -= *weight;
    }
    return None;
}

/// Tree operations
enum Op {
    Set,
    Scan,
    Get,
    Delete,
    Cas,
}

/// Key-Value struct which contains the keys and values
struct KV {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl KV {
    /// creates a new Key-Value instance
    fn new() -> KV {
        let mut key: Vec<u8> = rand::thread_rng()
            .gen_iter::<u8>()
            .take(2)
            .collect::<Vec<u8>>();
        let mut value: Vec<u8> = rand::thread_rng()
            .gen_iter::<u8>()
            .take(2)
            .collect::<Vec<u8>>();
        let mut key_padding: Vec<u8> = vec![0; 62];
        let mut value_padding: Vec<u8> = vec![0; 510];

        key.append(&mut key_padding);
        value.append(&mut value_padding);

        KV {
            key,
            value,
        }
    }
}

/// Configuration which is passed in via CLI arguments
struct Config {
    num_threads: usize,
    num_operations: usize,
    freshness_bias: String,
    non_present_key_chance: bool,
    set: usize,
    scan: usize,
    get: usize,
    delete: usize,
    cas: usize,
    key_size_min: usize,
    key_size_max: usize,
    key_size_median: usize,
    value_size_min: usize,
    value_size_max: usize,
    value_size_median: usize,
    scan_iter_min: usize,
    scan_iter_max: usize,
    scan_iter_median: usize,
}

impl Config {
    /// creates a new config instance which contains all config-related values
    fn new(args: &HashMap<&str, &str>) -> Result<Config, &'static str> {
        // cast all values to their respective target data type
        let num_threads = match args.get("num_threads") {
            Some(x) => {
                let parsed = x.parse::<usize>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if num_threads.is_err() {
            return Err("num_threads is not a valid value");
        }

        let num_operations = match args.get("num_operations") {
            Some(x) => {
                let parsed = x.parse::<usize>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if num_operations.is_err() {
            return Err("num_operations is not a valid value");
        }

        let freshness_bias = match args.get("freshness_bias") {
            Some(x) => Ok(x.to_string()),
            None => Err(()),
        };
        if freshness_bias.is_err() {
            return Err("freshness_bias is not a valid value");
        }

        let non_present_key_chance = match args.get("non_present_key_chance") {
            Some(x) => {
                let parsed = x.parse::<bool>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if non_present_key_chance.is_err() {
            return Err("non_present_key_chance is not a valid value");
        }

        let set = match args.get("set") {
            Some(x) => {
                let parsed = x.parse::<usize>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if set.is_err() {
            return Err("set is not a valid value");
        }

        let scan = match args.get("scan") {
            Some(x) => {
                let parsed = x.parse::<usize>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if scan.is_err() {
            return Err("scan is not a valid value");
        }

        let get = match args.get("get") {
            Some(x) => {
                let parsed = x.parse::<usize>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if get.is_err() {
            return Err("get is not a valid value");
        }

        let delete = match args.get("delete") {
            Some(x) => {
                let parsed = x.parse::<usize>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if delete.is_err() {
            return Err("delete is not a valid value");
        }

        let cas = match args.get("cas") {
            Some(x) => {
                let parsed = x.parse::<usize>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if cas.is_err() {
            return Err("cas is not a valid value");
        }

        let key_size_min = match args.get("key_size_min") {
            Some(x) => {
                let parsed = x.parse::<usize>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if key_size_min.is_err() {
            return Err("key_size_min is not a valid value");
        }

        let key_size_max = match args.get("key_size_max") {
            Some(x) => {
                let parsed = x.parse::<usize>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if key_size_max.is_err() {
            return Err("key_size_max is not a valid value");
        }

        let key_size_median = match args.get("key_size_median") {
            Some(x) => {
                let parsed = x.parse::<usize>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if key_size_median.is_err() {
            return Err("key_size_median is not a valid value");
        }

        let value_size_min = match args.get("value_size_min") {
            Some(x) => {
                let parsed = x.parse::<usize>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if value_size_min.is_err() {
            return Err("value_size_min is not a valid value");
        }

        let value_size_max = match args.get("value_size_max") {
            Some(x) => {
                let parsed = x.parse::<usize>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if value_size_max.is_err() {
            return Err("value_size_max is not a valid value");
        }

        let value_size_median = match args.get("value_size_median") {
            Some(x) => {
                let parsed = x.parse::<usize>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if value_size_median.is_err() {
            return Err("value_size_median is not a valid value");
        }

        let scan_iter_min = match args.get("scan_iter_min") {
            Some(x) => {
                let parsed = x.parse::<usize>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if scan_iter_min.is_err() {
            return Err("scan_iter_min is not a valid value");
        }

        let scan_iter_max = match args.get("scan_iter_max") {
            Some(x) => {
                let parsed = x.parse::<usize>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if scan_iter_max.is_err() {
            return Err("scan_iter_max is not a valid value");
        }

        let scan_iter_median = match args.get("scan_iter_median") {
            Some(x) => {
                let parsed = x.parse::<usize>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if scan_iter_median.is_err() {
            return Err("scan_iter_median is not a valid value");
        }

        // perform validations
        check_min_max_median(
            key_size_min.unwrap(),
            key_size_max.unwrap(),
            key_size_median.unwrap(),
        );
        check_min_max_median(
            value_size_min.unwrap(),
            value_size_max.unwrap(),
            value_size_median.unwrap(),
        );
        check_min_max_median(
            scan_iter_min.unwrap(),
            scan_iter_max.unwrap(),
            scan_iter_median.unwrap(),
        );

        Ok(Config {
            num_threads: num_threads.unwrap(),
            num_operations: num_operations.unwrap(),
            freshness_bias: freshness_bias.unwrap(),
            non_present_key_chance: non_present_key_chance.unwrap(),
            set: set.unwrap(),
            scan: scan.unwrap(),
            get: get.unwrap(),
            delete: delete.unwrap(),
            cas: cas.unwrap(),
            key_size_min: key_size_min.unwrap(),
            key_size_max: key_size_max.unwrap(),
            key_size_median: key_size_median.unwrap(),
            value_size_min: value_size_min.unwrap(),
            value_size_max: value_size_max.unwrap(),
            value_size_median: value_size_median.unwrap(),
            scan_iter_min: scan_iter_min.unwrap(),
            scan_iter_max: scan_iter_max.unwrap(),
            scan_iter_median: scan_iter_median.unwrap(),
        })
    }
}

fn check_min_max_median(min: usize, max: usize, median: usize) {
    if !(min <= median && median <= max) {
        warn!(
            "Please check and provide different min({}), max({}) and median({}) values",
            min,
            max,
            median
        );
    }
}
