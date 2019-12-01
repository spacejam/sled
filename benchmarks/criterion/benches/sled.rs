use criterion::{criterion_group, criterion_main, Criterion};

use jemallocator::Jemalloc;

use sled::Config;

#[cfg_attr(
    // only enable jemalloc on linux and macos by default
    any(target_os = "linux", target_os = "macos"),
    global_allocator
)]
static ALLOC: Jemalloc = Jemalloc;

fn counter() -> usize {
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

    static C: AtomicUsize = AtomicUsize::new(0);

    C.fetch_add(1, Relaxed)
}

/// Generates a random number in `0..n`.
fn random(n: u32) -> u32 {
    use std::cell::Cell;
    use std::num::Wrapping;

    thread_local! {
        static RNG: Cell<Wrapping<u32>> = Cell::new(Wrapping(1406868647));
    }

    RNG.with(|rng| {
        // This is the 32-bit variant of Xorshift.
        //
        // Source: https://en.wikipedia.org/wiki/Xorshift
        let mut x = rng.get();
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        rng.set(x);

        // This is a fast alternative to `x % n`.
        //
        // Author: Daniel Lemire
        // Source: https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
        ((x.0 as u64).wrapping_mul(n as u64) >> 32) as u32
    })
}

fn sled_bulk_load(c: &mut Criterion) {
    let mut count = 0_u32;
    let mut bytes = |len| -> Vec<u8> {
        count += 1;
        count.to_be_bytes().into_iter().cycle().take(len).copied().collect()
    };

    let mut bench = |key_len, val_len| {
        let db = Config::new()
            .path(format!("bulk_k{}_v{}", key_len, val_len))
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        c.bench_function(
            &format!("bulk load key/value lengths {}/{}", key_len, val_len),
            |b| {
                b.iter(|| {
                    db.insert(bytes(key_len), bytes(val_len)).unwrap();
                })
            },
        );
    };

    for key_len in &[10_usize, 128, 256, 512] {
        for val_len in &[0_usize, 10, 128, 256, 512, 1024, 2048, 4096, 8192] {
            bench(*key_len, *val_len)
        }
    }
}

fn sled_monotonic_crud(c: &mut Criterion) {
    let db = Config::new().temporary(true).flush_every_ms(None).open().unwrap();

    c.bench_function("monotonic inserts", |b| {
        let mut count = 0_u32;
        b.iter(|| {
            count += 1;
            db.insert(count.to_be_bytes(), vec![]).unwrap();
        })
    });

    c.bench_function("monotonic gets", |b| {
        let mut count = 0_u32;
        b.iter(|| {
            count += 1;
            db.get(count.to_be_bytes()).unwrap();
        })
    });

    c.bench_function("monotonic removals", |b| {
        let mut count = 0_u32;
        b.iter(|| {
            count += 1;
            db.remove(count.to_be_bytes()).unwrap();
        })
    });
}

fn sled_random_crud(c: &mut Criterion) {
    const SIZE: u32 = 65536;

    let db = Config::new().temporary(true).flush_every_ms(None).open().unwrap();

    c.bench_function("random inserts", |b| {
        b.iter(|| {
            let k = random(SIZE).to_be_bytes();
            db.insert(k, vec![]).unwrap();
        })
    });

    c.bench_function("random gets", |b| {
        b.iter(|| {
            let k = random(SIZE).to_be_bytes();
            db.get(k).unwrap();
        })
    });

    c.bench_function("random removals", |b| {
        b.iter(|| {
            let k = random(SIZE).to_be_bytes();
            db.remove(k).unwrap();
        })
    });
}

fn sled_empty_opens(c: &mut Criterion) {
    let _ = std::fs::remove_dir_all("empty_opens");
    c.bench_function("empty opens", |b| {
        b.iter(|| {
            Config::new()
                .path(format!("empty_opens/{}.db", counter()))
                .flush_every_ms(None)
                .open()
                .unwrap()
        })
    });
    let _ = std::fs::remove_dir_all("empty_opens");
}

criterion_group!(
    benches,
    sled_bulk_load,
    sled_monotonic_crud,
    sled_random_crud,
    sled_empty_opens
);
criterion_main!(benches);
