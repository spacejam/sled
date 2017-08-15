extern crate clap;
extern crate num_cpus;
#[macro_use]
extern crate log;
extern crate rsdb;

use std::error::Error;
use std::io::prelude::*;
use std::process;
use std::collections::HashMap;

use clap::{App, Arg};

fn main() {
    let cpus = &(num_cpus::get().to_string());

    let matches = App::new("RSDB bench")
        .version("0.1.0")
        .about("RSDB benchmarking tool")
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
    Ok(())
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
