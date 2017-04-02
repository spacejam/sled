extern crate rsdb;
extern crate rand;

use std::thread;

use rand::Rng;

use rsdb::Log;

fn main() {
    let log = Log::start_system("stress.log".to_owned());
    let mut threads = vec![];
    for i in 0..6 {
        let log = log.clone();
        let name = format!("thread {}", i);
        let t = thread::Builder::new()
            .name(name)
            .spawn(move || {
                let mut rng = rand::thread_rng();
                for i in 0..50_000 {
                    if rng.next_f32() > 0.9 {
                        let buf = vec![6; i % 8192];
                        let res = log.reserve(buf.len());
                        let id = res.log_id();
                        if rng.next_f32() > 0.95 {
                            thread::sleep_ms(100);
                        }
                        res.complete(&*buf);
                        log.make_stable(id);
                    } else {
                        let buf = vec![i as u8; i % 8192];
                        log.write(&*buf);
                    }
                }
            })
            .unwrap();
        threads.push(t);
    }

    for t in threads.into_iter() {
        t.join().unwrap();
    }

    log.shutdown();
}
