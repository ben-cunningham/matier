//! FIFO waiter queue with O(1) enqueue, O(1) cancellation and O(1) pop-oldest: a doubly linked
//! list threaded through a slab. Each node carries a unique id so a stale cancel (the waiter was
//! already served and its slab key reused) can never remove somebody else's entry.

use slab::Slab;

const NONE: usize = usize::MAX;

struct Node<T> {
    id: u64,
    item: T,
    prev: usize,
    next: usize,
}

pub(crate) struct Waiters<T> {
    slab: Slab<Node<T>>,
    head: usize,
    tail: usize,
    next_id: u64,
}

impl<T> Waiters<T> {
    pub(crate) fn new() -> Self {
        Self {
            slab: Slab::new(),
            head: NONE,
            tail: NONE,
            next_id: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.slab.len()
    }

    /// Appends `item`; returns `(key, id)` for [`remove`](Self::remove).
    pub(crate) fn push_back(&mut self, item: T) -> (usize, u64) {
        let id = self.next_id;
        self.next_id += 1;
        let key = self.slab.insert(Node {
            id,
            item,
            prev: self.tail,
            next: NONE,
        });
        if self.tail == NONE {
            self.head = key;
        } else {
            self.slab[self.tail].next = key;
        }
        self.tail = key;
        (key, id)
    }

    fn unlink(&mut self, key: usize) -> Node<T> {
        let node = self.slab.remove(key);
        if node.prev == NONE {
            self.head = node.next;
        } else {
            self.slab[node.prev].next = node.next;
        }
        if node.next == NONE {
            self.tail = node.prev;
        } else {
            self.slab[node.next].prev = node.prev;
        }
        node
    }

    pub(crate) fn pop_front(&mut self) -> Option<T> {
        (self.head != NONE).then(|| self.unlink(self.head).item)
    }

    /// Removes the entry if it is still queued.
    pub(crate) fn remove(&mut self, key: usize, id: u64) -> Option<T> {
        match self.slab.get(key) {
            Some(n) if n.id == id => Some(self.unlink(key).item),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifo_and_cancel() {
        let mut w = Waiters::new();
        let a = w.push_back('a');
        let b = w.push_back('b');
        let c = w.push_back('c');
        assert_eq!(w.remove(b.0, b.1), Some('b'));
        assert_eq!(w.remove(b.0, b.1), None);
        assert_eq!(w.pop_front(), Some('a'));
        // stale cancel of a served waiter whose key gets reused
        let d = w.push_back('d');
        assert_eq!(w.remove(a.0, a.1), None);
        assert_eq!(w.len(), 2);
        assert_eq!(w.pop_front(), Some('c'));
        assert_eq!(w.remove(c.0, c.1), None);
        assert_eq!(w.pop_front(), Some('d'));
        assert!(w.pop_front().is_none());
        let _ = d;
        assert_eq!(w.len(), 0);
    }
}
