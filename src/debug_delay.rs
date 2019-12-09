use std::cell::UnsafeCell;

use {
    rand::{
        rngs::{adapter::ReseedingRng, OsRng},
        CryptoRng, Rng, RngCore, SeedableRng,
    },
    rand_chacha::ChaCha20Core as Core,
    rand_distr::{Distribution, Gamma},
    std::sync::atomic::{AtomicUsize, Ordering::Relaxed},
};

use crate::{warn, Lazy};

/// This function is useful for inducing random jitter into our atomic
/// operations, shaking out more possible interleavings quickly. It gets
/// fully eliminated by the compiler in non-test code.
pub fn debug_delay() {
    use std::thread;
    use std::time::Duration;

    static GLOBAL_DELAYS: AtomicUsize = AtomicUsize::new(0);

    static INTENSITY: Lazy<f64, fn() -> f64> = Lazy::new(|| {
        std::env::var("SLED_LOCK_FREE_DELAY_INTENSITY")
            .unwrap_or_else(|_| "100.0".into())
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

    let mut rng = if let Some(rng) = try_thread_rng() {
        rng
    } else {
        warn!("already destroyed TLS when this debug delay was called");
        return;
    };

    if rng.gen_bool(1. / 1000.) {
        let gamma = Gamma::new(0.3, 1_000.0 * *INTENSITY).unwrap();
        let duration = gamma.sample(&mut try_thread_rng().unwrap());

        #[allow(clippy::cast_possible_truncation)]
        #[allow(clippy::cast_sign_loss)]
        thread::sleep(Duration::from_micros(duration as u64));
    }

    if rng.gen::<bool>() {
        thread::yield_now();
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ThreadRng {
    // use of raw pointer implies type is neither Send nor Sync
    rng: *mut ReseedingRng<Core, OsRng>,
}

const THREAD_RNG_RESEED_THRESHOLD: u64 = 1024 * 64;

thread_local!(
    static THREAD_RNG_KEY: UnsafeCell<ReseedingRng<Core, OsRng>> = {
        let r = Core::from_rng(OsRng).unwrap_or_else(|err|
                panic!("could not initialize thread_rng: {}", err));
        let rng = ReseedingRng::new(r,
                                    THREAD_RNG_RESEED_THRESHOLD,
                                    OsRng);
        UnsafeCell::new(rng)
    }
);

/// Access a thread-rng that may have been destroyed.
fn try_thread_rng() -> Option<ThreadRng> {
    THREAD_RNG_KEY.try_with(|t| ThreadRng { rng: t.get() }).ok()
}

impl RngCore for ThreadRng {
    #[inline]
    #[allow(unsafe_code)]
    fn next_u32(&mut self) -> u32 {
        unsafe { (*self.rng).next_u32() }
    }

    #[inline]
    #[allow(unsafe_code)]
    fn next_u64(&mut self) -> u64 {
        unsafe { (*self.rng).next_u64() }
    }

    #[allow(unsafe_code)]
    fn fill_bytes(&mut self, dest: &mut [u8]) {
        unsafe { (*self.rng).fill_bytes(dest) }
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
        self.fill_bytes(dest);
        Ok(())
    }
}

impl CryptoRng for ThreadRng {}
