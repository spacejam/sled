#[cfg(any(
    feature = "testing-shred-allocator",
    feature = "testing-count-allocator"
))]
pub use alloc::*;

// the memshred feature causes all allocated and deallocated
// memory to be set to a specific non-zero value of 0xa1 for
// uninitialized allocations and 0xde for deallocated memory,
// in the hope that it will cause memory errors to surface
// more quickly.

#[cfg(feature = "testing-shred-allocator")]
mod alloc {
    use std::alloc::{Layout, System};

    #[global_allocator]
    static ALLOCATOR: ShredAllocator = ShredAllocator;

    #[derive(Default, Debug, Clone, Copy)]
    struct ShredAllocator;

    unsafe impl std::alloc::GlobalAlloc for ShredAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ret = System.alloc(layout);
            assert_ne!(ret, std::ptr::null_mut());
            std::ptr::write_bytes(ret, 0xa1, layout.size());
            ret
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            std::ptr::write_bytes(ptr, 0xde, layout.size());
            System.dealloc(ptr, layout)
        }
    }
}

#[cfg(feature = "testing-count-allocator")]
mod alloc {
    use std::alloc::{Layout, System};

    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;

    static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
    static FREED: AtomicUsize = AtomicUsize::new(0);
    static RESIDENT: AtomicUsize = AtomicUsize::new(0);

    fn allocated() -> usize {
        ALLOCATED.swap(0, Ordering::Relaxed)
    }

    fn freed() -> usize {
        FREED.swap(0, Ordering::Relaxed)
    }

    fn resident() -> usize {
        RESIDENT.load(Ordering::Relaxed)
    }

    #[derive(Default, Debug, Clone, Copy)]
    struct CountingAllocator;

    unsafe impl std::alloc::GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ret = System.alloc(layout);
            assert_ne!(ret, std::ptr::null_mut());
            ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
            RESIDENT.fetch_add(layout.size(), Ordering::Relaxed);
            std::ptr::write_bytes(ret, 0xa1, layout.size());
            ret
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            std::ptr::write_bytes(ptr, 0xde, layout.size());
            FREED.fetch_add(layout.size(), Ordering::Relaxed);
            RESIDENT.fetch_sub(layout.size(), Ordering::Relaxed);
            System.dealloc(ptr, layout)
        }
    }
}
