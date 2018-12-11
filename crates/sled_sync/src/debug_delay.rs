use std::cell::UnsafeCell;
use std::rc::Rc;

use {
    log::warn,
    rand::{
        distributions::{Distribution, Gamma},
        rngs::EntropyRng,
        ReseedingRng, Rng, RngCore, SeedableRng,
    },
    rand_hc::Hc128Core,
};

/// This function is useful for inducing random jitter into our atomic
/// operations, shaking out more possible interleavings quickly. It gets
/// fully elliminated by the compiler in non-test code.
pub fn debug_delay() {
    use std::sync::atomic::spin_loop_hint;
    use std::thread;
    use std::time::Duration;

    let mut rng = if let Some(rng) = try_thread_rng() {
        rng
    } else {
        warn!(
            "already destroyed TLS when this debug delay was called"
        );
        return;
    };

    if rng.gen_bool(1. / 1000.) {
        let gamma = Gamma::new(0.3, 100000.0);
        let duration = gamma.sample(&mut try_thread_rng().unwrap());
        thread::sleep(Duration::from_micros(duration as u64));
    }

    spin_loop_hint();

    if rng.gen::<bool>() {
        thread::yield_now();
    }
}

#[derive(Clone, Debug)]
pub struct ThreadRng {
    rng: *mut ReseedingRng<Hc128Core, EntropyRng>,
}

const THREAD_RNG_RESEED_THRESHOLD: u64 = 32 * 1024 * 1024; // 32 MiB

thread_local!(
    static THREAD_RNG_KEY: Rc<UnsafeCell<ReseedingRng<Hc128Core, EntropyRng>>> = {
        let mut entropy_source = EntropyRng::new();
        let r = Hc128Core::from_rng(&mut entropy_source).unwrap_or_else(|err|
                panic!("could not initialize thread_rng: {}", err));
        let rng = ReseedingRng::new(r,
                                    THREAD_RNG_RESEED_THRESHOLD,
                                    entropy_source);
        Rc::new(UnsafeCell::new(rng))
    }
);

/// Access a thread-rng that may have been destroyed.
pub fn try_thread_rng() -> Option<ThreadRng> {
    THREAD_RNG_KEY.try_with(|t| ThreadRng { rng: t.get() }).ok()
}

impl RngCore for ThreadRng {
    #[inline(always)]
    fn next_u32(&mut self) -> u32 {
        unsafe { (*self.rng).next_u32() }
    }

    #[inline(always)]
    fn next_u64(&mut self) -> u64 {
        unsafe { (*self.rng).next_u64() }
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        unsafe { (*self.rng).fill_bytes(dest) }
    }

    fn try_fill_bytes(
        &mut self,
        dest: &mut [u8],
    ) -> Result<(), rand::Error> {
        Ok(self.fill_bytes(dest))
    }
}
