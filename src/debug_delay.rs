#![allow(clippy::float_arithmetic)]

use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use crate::Lazy;

/// This function is useful for inducing random jitter into our atomic
/// operations, shaking out more possible interleavings quickly. It gets
/// fully eliminated by the compiler in non-test code.
pub fn debug_delay() {
    use std::thread;
    use std::time::Duration;

    static GLOBAL_DELAYS: AtomicUsize = AtomicUsize::new(0);

    static INTENSITY: Lazy<u32, fn() -> u32> = Lazy::new(|| {
        std::env::var("SLED_LOCK_FREE_DELAY_INTENSITY")
            .unwrap_or_else(|_| "100".into())
            .parse()
            .expect(
                "SLED_LOCK_FREE_DELAY_INTENSITY must be set to a \
                 float (ideally between 1-1,000,000)",
            )
    });

    thread_local!(
        static LOCAL_DELAYS: std::cell::RefCell<usize> = std::cell::RefCell::new(0)
    );

    let global_delays = GLOBAL_DELAYS.fetch_add(1, Relaxed);
    let local_delays = LOCAL_DELAYS.with(|ld| {
        let mut ld = ld.borrow_mut();
        let old = *ld;
        *ld = std::cmp::max(global_delays + 1, *ld + 1);
        old
    });

    if global_delays == local_delays {
        // no other threads seem to be
        // calling this, so we may as
        // well skip it
        return;
    }

    if random(1000) == 1 {
        let duration = random(*INTENSITY);

        #[allow(clippy::cast_possible_truncation)]
        #[allow(clippy::cast_sign_loss)]
        thread::sleep(Duration::from_micros(duration as u64));
    }

    if random(2) == 0 {
        thread::yield_now();
    }
}

/// Generates a random number in `0..n`.
fn random(n: u32) -> u32 {
    use std::cell::Cell;
    use std::num::Wrapping;

    thread_local! {
        static RNG: Cell<Wrapping<u32>> = Cell::new(Wrapping(1406868647));
    }

    RNG.try_with(|rng| {
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
    .unwrap_or(0)
}
