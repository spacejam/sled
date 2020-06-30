///! Inline of https://github.com/bltavares/atomic-shim

#[cfg(not(any(
    target_arch = "mips",
    target_arch = "powerpc",
    feature = "mutex"
)))]
pub use std::sync::atomic::{AtomicI64, AtomicU64};
#[cfg(any(target_arch = "mips", target_arch = "powerpc", feature = "mutex"))]
mod shim {
    use crossbeam_utils::sync::ShardedLock;
    use std::sync::atomic::Ordering;

    #[derive(Debug, Default)]
    pub struct AtomicU64 {
        value: ShardedLock<u64>,
    }

    impl AtomicU64 {
        pub fn new(v: u64) -> Self {
            Self { value: ShardedLock::new(v) }
        }

        #[allow(dead_code)]
        pub fn get_mut(&mut self) -> &mut u64 {
            self.value.get_mut().unwrap()
        }

        #[allow(dead_code)]
        pub fn into_inner(self) -> u64 {
            self.value.into_inner().unwrap()
        }

        #[allow(dead_code)]
        pub fn load(&self, _: Ordering) -> u64 {
            *self.value.read().unwrap()
        }

        #[allow(dead_code)]
        pub fn store(&self, value: u64, _: Ordering) {
            let mut lock = self.value.write().unwrap();
            *lock = value;
        }

        #[allow(dead_code)]
        pub fn swap(&self, value: u64, _: Ordering) -> u64 {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            *lock = value;
            prev
        }

        #[allow(dead_code)]
        pub fn compare_and_swap(
            &self,
            current: u64,
            new: u64,
            _: Ordering,
        ) -> u64 {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            if prev == current {
                *lock = new;
            };
            prev
        }

        #[allow(dead_code)]
        pub fn compare_exchange(
            &self,
            current: u64,
            new: u64,
            _: Ordering,
            _: Ordering,
        ) -> Result<u64, u64> {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            if prev == current {
                *lock = new;
                Ok(current)
            } else {
                Err(prev)
            }
        }

        #[allow(dead_code)]
        pub fn compare_exchange_weak(
            &self,
            current: u64,
            new: u64,
            success: Ordering,
            failure: Ordering,
        ) -> Result<u64, u64> {
            self.compare_exchange(current, new, success, failure)
        }

        #[allow(dead_code)]
        pub fn fetch_add(&self, val: u64, _: Ordering) -> u64 {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            *lock = prev.wrapping_add(val);
            prev
        }

        #[allow(dead_code)]
        pub fn fetch_sub(&self, val: u64, _: Ordering) -> u64 {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            *lock = prev.wrapping_sub(val);
            prev
        }

        #[allow(dead_code)]
        pub fn fetch_and(&self, val: u64, _: Ordering) -> u64 {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            *lock = prev & val;
            prev
        }

        #[allow(dead_code)]
        pub fn fetch_nand(&self, val: u64, _: Ordering) -> u64 {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            *lock = !(prev & val);
            prev
        }

        #[allow(dead_code)]
        pub fn fetch_or(&self, val: u64, _: Ordering) -> u64 {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            *lock = prev | val;
            prev
        }

        #[allow(dead_code)]
        pub fn fetch_xor(&self, val: u64, _: Ordering) -> u64 {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            *lock = prev ^ val;
            prev
        }
    }

    impl From<u64> for AtomicU64 {
        fn from(value: u64) -> Self {
            AtomicU64::new(value)
        }
    }

    #[derive(Debug, Default)]
    pub struct AtomicI64 {
        value: ShardedLock<i64>,
    }

    impl AtomicI64 {
        pub fn new(v: i64) -> Self {
            Self { value: ShardedLock::new(v) }
        }

        #[allow(dead_code)]
        pub fn get_mut(&mut self) -> &mut i64 {
            self.value.get_mut().unwrap()
        }

        #[allow(dead_code)]
        pub fn into_inner(self) -> i64 {
            self.value.into_inner().unwrap()
        }

        #[allow(dead_code)]
        pub fn load(&self, _: Ordering) -> i64 {
            *self.value.read().unwrap()
        }

        #[allow(dead_code)]
        pub fn store(&self, value: i64, _: Ordering) {
            let mut lock = self.value.write().unwrap();
            *lock = value;
        }

        #[allow(dead_code)]
        pub fn swap(&self, value: i64, _: Ordering) -> i64 {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            *lock = value;
            prev
        }

        #[allow(dead_code)]
        pub fn compare_and_swap(
            &self,
            current: i64,
            new: i64,
            _: Ordering,
        ) -> i64 {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            if prev == current {
                *lock = new;
            };
            prev
        }

        #[allow(dead_code)]
        pub fn compare_exchange(
            &self,
            current: i64,
            new: i64,
            _: Ordering,
            _: Ordering,
        ) -> Result<i64, i64> {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            if prev == current {
                *lock = new;
                Ok(current)
            } else {
                Err(prev)
            }
        }

        #[allow(dead_code)]
        pub fn compare_exchange_weak(
            &self,
            current: i64,
            new: i64,
            success: Ordering,
            failure: Ordering,
        ) -> Result<i64, i64> {
            self.compare_exchange(current, new, success, failure)
        }

        #[allow(dead_code)]
        pub fn fetch_add(&self, val: i64, _: Ordering) -> i64 {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            *lock = prev.wrapping_add(val);
            prev
        }

        #[allow(dead_code)]
        pub fn fetch_sub(&self, val: i64, _: Ordering) -> i64 {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            *lock = prev.wrapping_sub(val);
            prev
        }

        #[allow(dead_code)]
        pub fn fetch_and(&self, val: i64, _: Ordering) -> i64 {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            *lock = prev & val;
            prev
        }

        #[allow(dead_code)]
        pub fn fetch_nand(&self, val: i64, _: Ordering) -> i64 {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            *lock = !(prev & val);
            prev
        }

        #[allow(dead_code)]
        pub fn fetch_or(&self, val: i64, _: Ordering) -> i64 {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            *lock = prev | val;
            prev
        }

        #[allow(dead_code)]
        pub fn fetch_xor(&self, val: i64, _: Ordering) -> i64 {
            let mut lock = self.value.write().unwrap();
            let prev = *lock;
            *lock = prev ^ val;
            prev
        }
    }

    impl From<i64> for AtomicI64 {
        fn from(value: i64) -> Self {
            AtomicI64::new(value)
        }
    }
}

#[cfg(any(
    target_arch = "mips",
    target_arch = "powerpc",
    feature = "mutex"
))]
pub use shim::{AtomicI64, AtomicU64};
