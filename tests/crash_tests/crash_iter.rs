use std::sync::{Arc, Barrier};
use std::thread;

use super::*;

const CACHE_SIZE: usize = 256;

pub fn run_crash_iter() {
    const N_FORWARD: usize = 50;
    const N_REVERSE: usize = 50;

    let path = std::path::Path::new(CRASH_DIR).join(ITER_DIR);
    let config = Config::new()
        .cache_capacity_bytes(CACHE_SIZE)
        .path(path)
        .flush_every_ms(Some(1));

    let db: Db = config.open().expect("couldn't open iter db");
    let t = db.open_tree(b"crash_iter_test").unwrap();

    thread::Builder::new()
        .name("crash_iter_flusher".to_string())
        .spawn({
            let t = t.clone();
            move || loop {
                t.flush().unwrap();
            }
        })
        .unwrap();

    const INDELIBLE: [&[u8]; 16] = [
        &[0u8],
        &[1u8],
        &[2u8],
        &[3u8],
        &[4u8],
        &[5u8],
        &[6u8],
        &[7u8],
        &[8u8],
        &[9u8],
        &[10u8],
        &[11u8],
        &[12u8],
        &[13u8],
        &[14u8],
        &[15u8],
    ];

    for item in &INDELIBLE {
        t.insert(*item, *item).unwrap();
    }
    t.flush().unwrap();

    let barrier = Arc::new(Barrier::new(N_FORWARD + N_REVERSE + 2));
    let mut threads = vec![];

    for i in 0..N_FORWARD {
        let t = thread::Builder::new()
            .name(format!("forward({})", i))
            .spawn({
                let t = t.clone();
                let barrier = barrier.clone();
                move || {
                    barrier.wait();
                    loop {
                        let expected = INDELIBLE.iter();
                        let mut keys = t.iter().keys();

                        for expect in expected {
                            loop {
                                let k = keys.next().unwrap().unwrap();
                                assert!(
                                    &*k <= *expect,
                                    "witnessed key is {:?} but we expected \
                                     one <= {:?}, so we overshot due to a \
                                     concurrent modification",
                                    k,
                                    expect,
                                );
                                if &*k == *expect {
                                    break;
                                }
                            }
                        }
                    }
                }
            })
            .unwrap();
        threads.push(t);
    }

    for i in 0..N_REVERSE {
        let t = thread::Builder::new()
            .name(format!("reverse({})", i))
            .spawn({
                let t = t.clone();
                let barrier = barrier.clone();
                move || {
                    barrier.wait();
                    loop {
                        let expected = INDELIBLE.iter().rev();
                        let mut keys = t.iter().keys().rev();

                        for expect in expected {
                            loop {
                                if let Some(Ok(k)) = keys.next() {
                                    assert!(
                                        &*k >= *expect,
                                        "witnessed key is {:?} but we expected \
                                         one >= {:?}, so we overshot due to a \
                                         concurrent modification\n{:?}",
                                        k,
                                        expect,
                                        t,
                                    );
                                    if &*k == *expect {
                                        break;
                                    }
                                } else {
                                    panic!("undershot key on tree: \n{:?}", t);
                                }
                            }
                        }
                    }
                }
            })
            .unwrap();

        threads.push(t);
    }

    let inserter = thread::Builder::new()
        .name("inserter".into())
        .spawn({
            let t = t.clone();
            let barrier = barrier.clone();
            move || {
                barrier.wait();

                loop {
                    for i in 0..(16 * 16 * 8) {
                        let major = i / (16 * 8);
                        let minor = i % 16;

                        let mut base = INDELIBLE[major].to_vec();
                        base.push(minor as u8);
                        t.insert(base.clone(), base.clone()).unwrap();
                    }
                }
            }
        })
        .unwrap();

    threads.push(inserter);

    let deleter = thread::Builder::new()
        .name("deleter".into())
        .spawn({
            move || {
                barrier.wait();

                loop {
                    for i in 0..(16 * 16 * 8) {
                        let major = i / (16 * 8);
                        let minor = i % 16;

                        let mut base = INDELIBLE[major].to_vec();
                        base.push(minor as u8);
                        t.remove(&base).unwrap();
                    }
                }
            }
        })
        .unwrap();

    spawn_killah();

    threads.push(deleter);

    for thread in threads.into_iter() {
        thread.join().expect("thread should not have crashed");
    }
}
