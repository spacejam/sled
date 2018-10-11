use epoch;

/// A guard used with epoch-based reclamation (EBR)
/// to track threads accessing shared lock-free
/// data structures, and safely drop data
/// after any thread may have accessed it.
pub type Guard = epoch::Guard;

/// Enter an epoch which allows us to safely
/// access shared data structures without
/// using mutexes.
pub fn pin() -> Guard {
    epoch::pin()
}

/// This function is useful for inducing random jitter into our atomic
/// operations, shaking out more possible interleavings quickly. It gets
/// fully elliminated by the compiler in non-test code.
#[inline(always)]
pub fn debug_delay() {
    #[cfg(any(test, feature = "lock_free_delays"))]
    {
        use std::sync::atomic::{spin_loop_hint, Ordering::SeqCst};
        use std::thread;
        use std::time::Duration;

        use rand::{
            distributions::{Distribution, Gamma},
            thread_rng, Rng,
        };

        if thread_rng().gen_bool(1. / 1000.) {
            let gamma = Gamma::new(0.3, 100000.0);
            let duration = gamma.sample(&mut thread_rng());
            thread::sleep(Duration::from_micros(duration as u64));
        }

        spin_loop_hint();

        if thread_rng().gen::<bool>() {
            thread::yield_now();
        }
    }
}

pub(crate) fn u64_to_arr(u: u64) -> [u8; 8] {
    [
        u as u8,
        (u >> 8) as u8,
        (u >> 16) as u8,
        (u >> 24) as u8,
        (u >> 32) as u8,
        (u >> 40) as u8,
        (u >> 48) as u8,
        (u >> 56) as u8,
    ]
}

pub(crate) fn arr_to_u64(arr: [u8; 8]) -> u64 {
    arr[0] as u64
        + ((arr[1] as u64) << 8)
        + ((arr[2] as u64) << 16)
        + ((arr[3] as u64) << 24)
        + ((arr[4] as u64) << 32)
        + ((arr[5] as u64) << 40)
        + ((arr[6] as u64) << 48)
        + ((arr[7] as u64) << 56)
}

pub(crate) fn arr_to_u32(arr: [u8; 4]) -> u32 {
    arr[0] as u32
        + ((arr[1] as u32) << 8)
        + ((arr[2] as u32) << 16)
        + ((arr[3] as u32) << 24)
}

pub(crate) fn u32_to_arr(u: u32) -> [u8; 4] {
    [u as u8, (u >> 8) as u8, (u >> 16) as u8, (u >> 24) as u8]
}
