//! Inline single-producer / single-consumer queue for per-stream receive events.
//!
//! A fixed ring of [`RING_SLOTS`] slots lives inside `StreamShared`, so the common case (a response is a
//! head plus a handful of DATA events) costs zero allocations and no cross-thread frees. When the ring is
//! full the producer spills to a lock-free queue of boxed events; ordering is preserved by the `spill_len`
//! protocol described on [`RecvQueue::push`].
//!
//! Roles: exactly one producer at a time (the connection actor) and exactly one consumer at a time (the body
//! reader; on cancel the consumer drains and then hands off to the actor through the command channel, which
//! orders the two). Violating this is undefined behaviour, hence the `unsafe` methods.

use std::mem::MaybeUninit;

use super::sync::{AtomicUsize, EventQueue, Ordering, UnsafeCell};

/// Slots in the inline ring (tiny under loom so the spill path is reachable with few events).
#[cfg(not(matier_loom))]
pub(crate) const RING_SLOTS: usize = 4;
#[cfg(matier_loom)]
pub(crate) const RING_SLOTS: usize = 2;

/// Bounded SPSC ring. `head` is advanced only by the consumer, `tail` only by the producer.
struct Ring<T> {
    slots: [UnsafeCell<MaybeUninit<T>>; RING_SLOTS],
    head: AtomicUsize,
    tail: AtomicUsize,
}

impl<T> Ring<T> {
    fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| UnsafeCell::new(MaybeUninit::uninit())),
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    /// # Safety
    /// At most one thread may call `push` at a time (the producer role).
    unsafe fn push(&self, v: T) -> Result<(), T> {
        let t = self.tail.load(Ordering::Relaxed);
        // Acquire pairs with the consumer's Release store of `head`: the slot we are about to overwrite
        // has been fully read out.
        let h = self.head.load(Ordering::Acquire);
        if t.wrapping_sub(h) >= RING_SLOTS {
            return Err(v);
        }
        self.slots[t % RING_SLOTS].with_mut(|p| {
            // SAFETY: slot `t % N` is outside `[head, tail)`, so the consumer does not touch it, and there
            // is a single producer; it holds no initialized value (consumer moved it out).
            unsafe { (*p).write(v) };
        });
        // Release publishes the slot write to the consumer's Acquire load of `tail`.
        self.tail.store(t.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// # Safety
    /// At most one thread may call `pop` at a time (the consumer role).
    unsafe fn pop(&self) -> Option<T> {
        let h = self.head.load(Ordering::Relaxed);
        // Acquire pairs with the producer's Release store of `tail`: the slot write is visible.
        let t = self.tail.load(Ordering::Acquire);
        if h == t {
            return None;
        }
        let v = self.slots[h % RING_SLOTS].with_mut(|p| {
            // SAFETY: `h` is inside `[head, tail)`, hence initialized by the producer and not being written;
            // we are the only consumer; the value is moved out exactly once because `head` advances below.
            unsafe { (*p).assume_init_read() }
        });
        // Release hands the (now logically empty) slot back to the producer.
        self.head.store(h.wrapping_add(1), Ordering::Release);
        Some(v)
    }

    fn len(&self) -> usize {
        let h = self.head.load(Ordering::Acquire);
        let t = self.tail.load(Ordering::Acquire);
        t.wrapping_sub(h)
    }
}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        // `&mut self`: no other thread can be in `push`/`pop`.
        let h = self.head.load(Ordering::Relaxed);
        let t = self.tail.load(Ordering::Relaxed);
        let mut i = h;
        while i != t {
            self.slots[i % RING_SLOTS].with_mut(|p| {
                // SAFETY: every index in `[head, tail)` holds an initialized value that was never read out.
                unsafe { (*p).assume_init_drop() };
            });
            i = i.wrapping_add(1);
        }
    }
}

// SAFETY: values of `T` are moved between threads through the ring (needs `T: Send`); each slot is accessed by
// one side at a time, ordered by the head/tail Release/Acquire pairs.
unsafe impl<T: Send> Send for Ring<T> {}
// SAFETY: see above; shared access is the SPSC protocol.
unsafe impl<T: Send> Sync for Ring<T> {}

/// Inline ring plus an overflow queue; FIFO overall.
pub(crate) struct RecvQueue<T> {
    ring: Ring<T>,
    /// Events currently in `spill`: incremented by the producer before the push, decremented by the consumer
    /// after the pop.
    spill_len: AtomicUsize,
    spill: EventQueue<Box<T>>,
}

impl<T> RecvQueue<T> {
    pub(crate) fn new() -> Self {
        Self {
            ring: Ring::new(),
            spill_len: AtomicUsize::new(0),
            spill: EventQueue::new(),
        }
    }

    /// Producer. Order: an event goes to the ring only when the spill queue is empty; otherwise (or when the
    /// ring is full) it goes to the spill queue. The consumer drains the ring first, then the spill queue.
    /// Ring events are therefore always older than spill events, and an event is only placed in the ring after
    /// the consumer has popped every spilled event (`spill_len == 0` is observed after the consumer's decrement,
    /// which follows its pop).
    ///
    /// # Safety
    /// Single producer at a time.
    pub(crate) unsafe fn push(&self, v: T) {
        let v = if self.spill_len.load(Ordering::Acquire) == 0 {
            // SAFETY: forwarded producer contract.
            match unsafe { self.ring.push(v) } {
                Ok(()) => return,
                Err(v) => v,
            }
        } else {
            v
        };
        self.spill_len.fetch_add(1, Ordering::AcqRel);
        self.spill.push(Box::new(v));
    }

    /// Consumer.
    ///
    /// # Safety
    /// Single consumer at a time.
    pub(crate) unsafe fn pop(&self) -> Option<T> {
        // SAFETY: forwarded consumer contract.
        if let Some(v) = unsafe { self.ring.pop() } {
            return Some(v);
        }
        if self.spill_len.load(Ordering::Acquire) == 0 {
            return None;
        }
        // The first ring check may predate ring pushes that came before the spill started; those are older
        // than every spilled event, so look again now that `spill_len > 0` (Acquire) orders us after them.
        // While `spill_len > 0` the producer pushes only to the spill queue, so a second miss means the ring
        // is truly empty.
        // SAFETY: forwarded consumer contract.
        if let Some(v) = unsafe { self.ring.pop() } {
            return Some(v);
        }
        // The producer increments `spill_len` before pushing, so the item may not be visible yet; the caller
        // is woken after the producer's push (register-then-check), so `None` is safe.
        let v = self.spill.pop()?;
        self.spill_len.fetch_sub(1, Ordering::AcqRel);
        Some(*v)
    }

    /// Approximate number of queued events (exact when called by the producer or consumer with the other idle).
    pub(crate) fn len(&self) -> usize {
        self.ring.len() + self.spill_len.load(Ordering::Acquire)
    }
}
