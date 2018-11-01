extern crate crossbeam_epoch as epoch;
#[cfg(any(test, feature = "lock_free_delays"))]
extern crate rand;

pub use epoch::{pin, unprotected, Atomic, Guard, Owned, Shared};

/// This function is useful for inducing random jitter into our atomic
/// operations, shaking out more possible interleavings quickly. It gets
/// fully elliminated by the compiler in non-test code.
#[cfg_attr(not(feature = "no_inline"), inline)]
pub fn debug_delay() {
    #[cfg(any(test, feature = "lock_free_delays"))]
    {
        use std::sync::atomic::spin_loop_hint;
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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
