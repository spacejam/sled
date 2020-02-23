#![allow(unsafe_code)]

use std::sync::atomic::{AtomicUsize, Ordering::Release};

// define a passthrough allocator that tracks alloc calls.
// adapted from the flatbuffer codebase
use std::alloc::{GlobalAlloc, Layout, System};

pub(crate) struct TrackingAllocator;

pub static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
pub static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Release);
        ALLOCATED_BYTES.fetch_add(layout.size(), Release);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}
