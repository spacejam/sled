use super::*;

const CACHE_SIZE: usize = 1024 * 1024;

pub fn run_crash_tx() {
    let config = Config::new()
        .cache_capacity_bytes(CACHE_SIZE)
        .flush_every_ms(Some(1))
        .path(TX_DIR);

    let _db: Db = config.open().unwrap();

    spawn_killah();

    loop {}

    /*
        db.insert(b"k1", b"cats").unwrap();
        db.insert(b"k2", b"dogs").unwrap();
        db.insert(b"id", &0_u64.to_le_bytes()).unwrap();

        let mut threads = vec![];

        const N_WRITERS: usize = 50;
        const N_READERS: usize = 5;

        let barrier = Arc::new(Barrier::new(N_WRITERS + N_READERS));

        for _ in 0..N_WRITERS {
            let db = db.clone();
            let barrier = barrier.clone();
            let thread = std::thread::spawn(move || {
                barrier.wait();
                loop {
                    db.transaction::<_, _, ()>(|db| {
                        let v1 = db.remove(b"k1").unwrap().unwrap();
                        let v2 = db.remove(b"k2").unwrap().unwrap();

                        db.insert(b"id", &db.generate_id().unwrap().to_le_bytes())
                            .unwrap();

                        db.insert(b"k1", v2).unwrap();
                        db.insert(b"k2", v1).unwrap();
                        Ok(())
                    })
                    .unwrap();
                }
            });
            threads.push(thread);
        }

        for _ in 0..N_READERS {
            let db = db.clone();
            let barrier = barrier.clone();
            let thread = std::thread::spawn(move || {
                barrier.wait();
                let mut last_id = 0;
                loop {
                    let read_id = db
                        .transaction::<_, _, ()>(|db| {
                            let v1 = db.get(b"k1").unwrap().unwrap();
                            let v2 = db.get(b"k2").unwrap().unwrap();
                            let id = u64::from_le_bytes(
                                TryFrom::try_from(
                                    &*db.get(b"id").unwrap().unwrap(),
                                )
                                .unwrap(),
                            );

                            let mut results = vec![v1, v2];
                            results.sort();

                            assert_eq!(
                                [&results[0], &results[1]],
                                [b"cats", b"dogs"]
                            );

                            Ok(id)
                        })
                        .unwrap();
                    assert!(read_id >= last_id);
                    last_id = read_id;
                }
            });
            threads.push(thread);
        }

        spawn_killah();

        for thread in threads.into_iter() {
            thread.join().expect("threads should not crash");
        }

        let v1 = db.get(b"k1").unwrap().unwrap();
        let v2 = db.get(b"k2").unwrap().unwrap();
        assert_eq!([v1, v2], [b"cats", b"dogs"]);
    */
}
