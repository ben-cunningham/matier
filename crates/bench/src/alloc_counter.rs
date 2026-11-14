//! A counting global allocator.
//!
//! Wraps [`System`] and bumps relaxed atomic counters on every allocation. Counters are
//! process-global (all threads), so on many-core machines the cache-line contention adds
//! measurable overhead (a few ns per allocation); disable the `count-allocs` feature when
//! measuring raw throughput or CPU and enable it when measuring allocation behaviour.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOCS: AtomicU64 = AtomicU64::new(0);
static ENABLED: AtomicBool = AtomicBool::new(false);

/// Global allocator that counts allocations. Install with `#[global_allocator]`.
pub struct CountingAlloc;

/// Point-in-time allocator counters.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllocStats {
    pub allocs: u64,
    pub bytes: u64,
    pub deallocs: u64,
}

/// Records that the counting allocator is installed (call once from `main`).
pub fn mark_enabled() {
    ENABLED.store(true, Relaxed);
}

/// Whether the counting allocator is installed.
pub fn enabled() -> bool {
    ENABLED.load(Relaxed)
}

/// Reads the current counters.
pub fn stats() -> AllocStats {
    AllocStats {
        allocs: ALLOCS.load(Relaxed),
        bytes: BYTES.load(Relaxed),
        deallocs: DEALLOCS.load(Relaxed),
    }
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(layout.size() as u64, Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(layout.size() as u64, Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOCS.fetch_add(1, Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // A realloc counts as one allocation of `new_size` bytes plus one deallocation.
        ALLOCS.fetch_add(1, Relaxed);
        DEALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(new_size as u64, Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}
