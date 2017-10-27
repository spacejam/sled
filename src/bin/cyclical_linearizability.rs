extern crate sled;

/// Verifies that the keys in the tree are correctly recovered.
/// Panics if they are incorrect.
/// Returns the key that should be resumed at, and the current cycle value.
fn verify(tree: &sled::Tree) -> (u32, u32) {
    // key 0 should always be the highest value, as that's where we increment
    // at some point, it might go down by one
    // it should never return, or go down again after that
    let mut iter = tree.iter();
    let highest = match iter.next() {
        None => return (0, 0),
        Some((_k, v)) => slice_to_u32(&*v),
    };

    let highest_vec = u32_to_vec(highest);

    // find how far we got
    let mut contiguous = 1;
    let mut lowest = 0;
    for (mut k, v) in iter {
        if v == highest_vec {
            contiguous += 1;
        } else {
            k.reverse();
            println!(
                "different k: {} v: {}",
                slice_to_u32(&*k),
                slice_to_u32(&*v)
            );
            let expected = highest - 1;
            let actual = slice_to_u32(&*v);
            assert_eq!(expected, actual);
            lowest = actual;
            break;
        }
    }

    println!("0 through {} are {}", contiguous, highest);

    let lowest_vec = u32_to_vec(lowest);

    // ensure nothing changes after this point
    let low_beginning = u32_to_vec(contiguous + 1);

    println!("from {} and up expecting {:?}", contiguous + 1, lowest_vec);
    for (mut k, v) in tree.scan(&*low_beginning) {
        if v != lowest_vec {
            k.reverse();
            println!("k: {} v: {}", slice_to_u32(&*k), slice_to_u32(&*v));
        }
        assert_eq!(v, lowest_vec);
    }

    (contiguous, highest)
}

fn u32_to_vec(u: u32) -> Vec<u8> {
    let buf: [u8; 4] = unsafe { std::mem::transmute(u) };
    buf.to_vec()
}

fn slice_to_u32(b: &[u8]) -> u32 {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(b);

    unsafe { std::mem::transmute(buf) }
}

fn main() {
    // TODO CAS from multiple threads
    let config = sled::Config::default()
        .io_bufs(2)
        .blink_fanout(15)
        .page_consolidation_threshold(10)
        .cache_fixup_threshold(1)
        .cache_bits(6)
        .cache_capacity(128 * 1024 * 1024)
        .flush_every_ms(None)
        // .io_buf_size(1 << 16)
        .path("cycles.db".to_string())
        .snapshot_after_ops(1 << 56);

    println!("restoring");
    let tree = config.tree();

    println!("verifying");
    let (key, highest) = verify(&tree);

    println!("verified! running...");

    let cycle: usize = 65536;

    let mut hu = ((highest as usize) * cycle) + key as usize;
    assert_eq!(hu % cycle, key as usize);
    assert_eq!(hu / cycle, highest as usize);

    loop {
        hu += 1;

        if hu / cycle > cycle {
            println!("completed full transit of 16-bit value space.");
            // TODO start over?
            break;
        }

        let mut key = u32_to_vec((hu % cycle) as u32);
        key.reverse();
        let value = u32_to_vec((hu / cycle) as u32);
        tree.set(key, value);
    }
}
