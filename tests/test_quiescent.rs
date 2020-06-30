#![cfg(all(target_os = "linux", not(miri)))]

mod common;

use std::time::{Duration, Instant};

use common::cleanup;

#[test]
fn test_quiescent_cpu_time() {
    const DB_DIR: &str = "sleeper";
    cleanup(DB_DIR);

    fn run() {
        let start = Instant::now();
        let db = sled::open(DB_DIR).unwrap();
        std::thread::sleep(Duration::from_secs(10));
        drop(db);
        let end = Instant::now();

        let (user_cpu_time, system_cpu_time) = unsafe {
            let mut resource_usage: libc::rusage = std::mem::zeroed();
            let return_value = libc::getrusage(
                libc::RUSAGE_SELF,
                (&mut resource_usage) as *mut libc::rusage,
            );
            if return_value != 0 {
                panic!("error {} from getrusage()", *libc::__errno_location());
            }
            (resource_usage.ru_utime, resource_usage.ru_stime)
        };

        let user_cpu_seconds =
            user_cpu_time.tv_sec as f64 + user_cpu_time.tv_usec as f64 * 1e-6;
        let system_cpu_seconds = system_cpu_time.tv_sec as f64
            + system_cpu_time.tv_usec as f64 * 1e-6;
        let real_time_elapsed = end.duration_since(start);

        if user_cpu_seconds + system_cpu_seconds > 1.0 {
            panic!(
                "Database used too much CPU during a quiescent workload. User: {}s, system: {}s (wall clock: {}s)",
                user_cpu_seconds,
                system_cpu_seconds,
                real_time_elapsed.as_secs_f64(),
            );
        }
    }

    let child = unsafe { libc::fork() };
    if child == 0 {
        common::setup_logger();
        if let Err(e) = std::thread::spawn(run).join() {
            println!("test failed: {:?}", e);
            std::process::exit(15);
        } else {
            std::process::exit(0);
        }
    } else {
        let mut status = 0;
        unsafe {
            libc::waitpid(child, &mut status as *mut libc::c_int, 0);
        }
        if status != 0 {
            cleanup(DB_DIR);
            panic!("child exited abnormally");
        }
    }

    cleanup(DB_DIR);
}
