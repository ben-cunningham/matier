//! Sync shim: std types normally, `loom` types under `--cfg loom` so the cross-task protocols in
//! `shared.rs` can be model checked. Only the primitives used by `StreamShared` / `ConnShared` go through here.

#[cfg(not(matier_loom))]
mod imp {
    pub(crate) use atomic_waker::AtomicWaker;
    pub(crate) use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicUsize, Ordering};
    pub(crate) use std::sync::{Arc, Mutex};

    /// Unbounded lock-free MPMC queue (used as SPSC): crossbeam's `SegQueue`.
    pub(crate) struct EventQueue<T>(crossbeam_queue::SegQueue<T>);

    impl<T> EventQueue<T> {
        pub(crate) fn new() -> Self {
            Self(crossbeam_queue::SegQueue::new())
        }
        pub(crate) fn push(&self, v: T) {
            self.0.push(v);
        }
        pub(crate) fn pop(&self) -> Option<T> {
            self.0.pop()
        }
    }
}

#[cfg(matier_loom)]
mod imp {

    /// Adapter giving loom's `AtomicWaker` the `atomic-waker` signature.
    pub(crate) struct AtomicWaker(loom::future::AtomicWaker);

    impl AtomicWaker {
        pub(crate) fn new() -> Self {
            Self(loom::future::AtomicWaker::new())
        }
        pub(crate) fn register(&self, w: &std::task::Waker) {
            self.0.register(w.clone());
        }
        pub(crate) fn wake(&self) {
            self.0.wake();
        }
    }
    pub(crate) use loom::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicUsize, Ordering};
    pub(crate) use loom::sync::{Arc, Mutex};
    use std::collections::VecDeque;

    /// Loom stand-in for the lock-free queue (a mutexed deque; the protocols under test do not
    /// depend on the queue's internals, only on its push/pop linearizability).
    pub(crate) struct EventQueue<T>(Mutex<VecDeque<T>>);

    impl<T> EventQueue<T> {
        pub(crate) fn new() -> Self {
            Self(Mutex::new(VecDeque::new()))
        }
        pub(crate) fn push(&self, v: T) {
            self.0.lock().unwrap().push_back(v);
        }
        pub(crate) fn pop(&self) -> Option<T> {
            self.0.lock().unwrap().pop_front()
        }
    }
}

pub(crate) use imp::*;

/// Lock a mutex, ignoring poisoning (a panic elsewhere must not wedge the actor).
pub(crate) fn lock<T>(m: &Mutex<T>) -> impl std::ops::DerefMut<Target = T> + '_ {
    #[cfg(not(matier_loom))]
    {
        m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    #[cfg(matier_loom)]
    {
        m.lock().unwrap()
    }
}
