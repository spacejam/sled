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
        .author("Tyler Neely <t@jujit.su>")
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
        .arg(Arg::with_name("proportion_set")
            .long("prop-set")
            .help("Proportion of sets")
            .default_value("10")
            .takes_value(true))
        .arg(Arg::with_name("proportion_scan")
            .long("prop-scan")
            .help("Proportion of scan")
            .default_value("4")
            .takes_value(true))
        .arg(Arg::with_name("proportion_get")
            .long("prop-get")
            .help("Proportion of gets")
            .default_value("80")
            .takes_value(true))
        .arg(Arg::with_name("proportion_delete")
            .long("prop-del")
            .help("Proportion of delete")
            .default_value("5")
            .takes_value(true))
        .arg(Arg::with_name("proportion_cas")
            .long("prop-cas")
            .help("Proportion of cas")
            .default_value("1")
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
    args.insert("non_present_key_chance", matches.value_of("non_present_key_chance").unwrap());
    args.insert("proportion_set", matches.value_of("proportion_set").unwrap());
    args.insert("proportion_scan", matches.value_of("proportion_scan").unwrap());
    args.insert("proportion_get", matches.value_of("proportion_get").unwrap());
    args.insert("proportion_delete", matches.value_of("proportion_delete").unwrap());
    args.insert("proportion_cas", matches.value_of("proportion_cas").unwrap());
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
        writeln!(
            &mut stderr,
            "Problem parsing arguments: {}",
            err
        ).expect("Could not write to stderr");
        process::exit(1);
    });

    if let Err(e) = run(config) {
        writeln!(
            &mut stderr,
            "Application error: {}",
            e
        ).expect("Could not write to stderr");
        process::exit(1);
    }
}

fn run(config: Config) -> Result<(), Box<Error>> {
    println!("running benchmarking suite...");
    Ok(())
}

/// Configuration which is passed in via CLI arguments
struct Config {
    num_threads: u64,
    num_operations: u64,
    freshness_bias: String,
    non_present_key_chance: bool,
    proportion_set: u64,
    proportion_scan: u64,
    proportion_get: u64,
    proportion_delete: u64,
    proportion_cas: u64,
    key_size_min: u64,
    key_size_max: u64,
    key_size_median: u64,
    value_size_min: u64,
    value_size_max: u64,
    value_size_median: u64,
    scan_iter_min: u64,
    scan_iter_max: u64,
    scan_iter_median: u64,
}

impl Config {
    /// creates a new config instance which contains all config-related values
    fn new(args: &HashMap<&str, &str>) -> Result<Config, &'static str> {
        // cast all values to their respective target data type
        let num_threads = match args.get("num_threads") {
            Some(x) => {
                let parsed = x.parse::<u64>();
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
                let parsed = x.parse::<u64>();
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

        let proportion_set = match args.get("proportion_set") {
            Some(x) => {
                let parsed = x.parse::<u64>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if proportion_set.is_err() {
            return Err("proportion_set is not a valid value");
        }

        let proportion_scan = match args.get("proportion_scan") {
            Some(x) => {
                let parsed = x.parse::<u64>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if proportion_scan.is_err() {
            return Err("proportion_scan is not a valid value");
        }

        let proportion_get = match args.get("proportion_get") {
            Some(x) => {
                let parsed = x.parse::<u64>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if proportion_get.is_err() {
            return Err("proportion_get is not a valid value");
        }

        let proportion_delete = match args.get("proportion_delete") {
            Some(x) => {
                let parsed = x.parse::<u64>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if proportion_delete.is_err() {
            return Err("proportion_delete is not a valid value");
        }

        let proportion_cas = match args.get("proportion_cas") {
            Some(x) => {
                let parsed = x.parse::<u64>();
                if parsed.is_ok() {
                    Ok(parsed.unwrap())
                } else {
                    Err(())
                }
            }
            None => Err(()),
        };
        if proportion_cas.is_err() {
            return Err("proportion_cas is not a valid value");
        }

        let key_size_min = match args.get("key_size_min") {
            Some(x) => {
                let parsed = x.parse::<u64>();
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
                let parsed = x.parse::<u64>();
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
                let parsed = x.parse::<u64>();
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
                let parsed = x.parse::<u64>();
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
                let parsed = x.parse::<u64>();
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
                let parsed = x.parse::<u64>();
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
                let parsed = x.parse::<u64>();
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
                let parsed = x.parse::<u64>();
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
                let parsed = x.parse::<u64>();
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
        check_min_max_median(key_size_min.unwrap(), key_size_max.unwrap(), key_size_median.unwrap());
        check_min_max_median(value_size_min.unwrap(), value_size_max.unwrap(), value_size_median.unwrap());
        check_min_max_median(scan_iter_min.unwrap(), scan_iter_max.unwrap(), scan_iter_median.unwrap());

        Ok(Config {
            num_threads: num_threads.unwrap(),
            num_operations: num_operations.unwrap(),
            freshness_bias: freshness_bias.unwrap(),
            non_present_key_chance: non_present_key_chance.unwrap(),
            proportion_set: proportion_set.unwrap(),
            proportion_scan: proportion_scan.unwrap(),
            proportion_get: proportion_get.unwrap(),
            proportion_delete: proportion_delete.unwrap(),
            proportion_cas: proportion_cas.unwrap(),
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

fn check_min_max_median(min: u64, max: u64, median: u64) {
    if !(min <= median && median <= max) {
        warn!("Please check and provide different min({}), max({}) and median({}) values", min, max, median);
    }
}
