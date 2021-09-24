use std::{iter::FromIterator, mem, ptr, sync::atomic::Ordering};

use crate::slot::{DropSlot, Slot};

use super::{Cursor, Node};

/// An owned, non-thread-safe FIFO queue that can be trivially transformed into or created from
/// either an [`mpsc`](crate::mpsc) or an [`mpmc`](crate::mpmc) queue.
///
/// The implementation is fairly efficient, but should not be used as replacement for e.g. a
/// [`VecDeque`](std::collections::VecDeque) in general.
/// It's intended use-case is for cheap conversion from/into one of the concurrent queue types,
/// which feature an identical internal structure.
pub struct OwnedQueue<T> {
    head: Cursor<T>,
    tail: Cursor<T>,
}

unsafe impl<T: Send> Send for OwnedQueue<T> {}
unsafe impl<T: Sync> Sync for OwnedQueue<T> {}

impl<T> Default for OwnedQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> OwnedQueue<T> {
    /// Creates a new empty queue.
    pub fn new() -> Self {
        let ptr = Node::alloc();
        Self { head: Cursor { ptr, idx: 0 }, tail: Cursor { ptr, idx: 0 } }
    }

    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.head.ptr == self.tail.ptr && self.head.idx >= self.tail.idx
    }

    /// Returns the length of the queue.
    pub fn len(&self) -> usize {
        if self.head.ptr == self.tail.ptr {
            self.tail.idx - self.head.idx
        } else {
            let mut len = crate::NODE_SIZE - self.head.idx;
            let mut curr = self.head.ptr;
            loop {
                curr = unsafe { (*curr).next.load(Ordering::Relaxed) };
                if curr == self.tail.ptr {
                    break;
                }

                len += crate::NODE_SIZE;
            }

            len + self.tail.idx
        }
    }

    /// Pushes `elem` to the back of the queue.
    pub fn push_back(&mut self, elem: T) {
        let Cursor { ptr, idx } = self.tail;
        if idx < crate::NODE_SIZE {
            unsafe { (*ptr).slots[idx].write_unsync(elem) };
            self.tail.idx += 1;
        } else {
            let node = Node::alloc_with(elem);
            unsafe { (*ptr).next.store(node, Ordering::Relaxed) };
            self.tail = Cursor { ptr: node, idx: 1 };
        }
    }

    /// Pops the element at the front of the queue or returns `None` if it is empty.
    pub fn pop_front(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }

        // the cursor to the current head slot
        let Cursor { ptr: head, idx } = self.head;
        if idx < crate::NODE_SIZE {
            let res = unsafe { (*head).slots[idx].consume_unsync_unchecked() };
            self.head.idx += 1;
            Some(res)
        } else {
            let res = unsafe {
                let next = (*head).next.load(Ordering::Relaxed);
                let res = (*next).slots[0].consume_unsync_unchecked();
                self.head = Cursor { ptr: next, idx: 1 };
                Node::dealloc(head);

                res
            };

            Some(res)
        }
    }

    /// Leaks the queue and it returns its (raw) head and tail (pointer, index) tuples.
    pub(crate) fn into_raw_parts(self) -> (Cursor<T>, Cursor<T>) {
        let parts = (self.head, self.tail);
        mem::forget(self);
        parts
    }

    /// Creates an `OwnedQueue` from two (raw) head and tail (pointer, index) tuples.
    ///
    /// # Safety
    ///
    /// `head` and `tail` must form a linked list of valid/live nodes.
    pub(crate) unsafe fn from_raw_parts(head: Cursor<T>, tail: Cursor<T>) -> Self {
        Self { head, tail }
    }
}

impl<T> Drop for OwnedQueue<T> {
    fn drop(&mut self) {
        while !self.head.ptr.is_null() {
            let Cursor { ptr: curr, idx } = self.head;
            if mem::needs_drop::<T>() {
                // the highest index is either NODE_SIZE of the tail index, once the loop reaches
                // the tail node
                let hi_idx =
                    if self.head.ptr == self.tail.ptr { self.tail.idx } else { crate::NODE_SIZE };
                // SAFETY: the range (idx..hi_idx) identifies all the slots that can be safely
                // dropped, so these slots can be safely cast to `DropSlot`s
                unsafe {
                    // dropping a slice of slots using compiler generated drop glue is preferable
                    // over dropping each slot iteratively, because the compiler can automatically
                    // prevent memory leaks in case the `drop` function for some slot panics (the
                    // first panic is caught, any further panics abort the program)
                    let slice: *mut [DropSlot<T>] =
                        &mut (*curr).slots[idx..hi_idx] as *mut [Slot<T>] as *mut _;
                    ptr::drop_in_place(slice);
                }
            }

            // SAFETY: This is safe because the current node's next pointer is read before the node
            // itself is dropped
            unsafe {
                self.head = Cursor { ptr: *(*curr).next.get_mut(), idx: 0 };
                Node::dealloc(curr);
            }
        }
    }
}

impl<T> FromIterator<T> for OwnedQueue<T> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        let mut queue = Self::new();

        for elem in iter {
            let idx = queue.tail.idx;
            if idx < crate::NODE_SIZE {
                unsafe { (*queue.tail.ptr).slots[idx].write_unsync(elem) };
                queue.tail.idx += 1;
            } else {
                let next = Node::alloc_with(elem);
                unsafe { *(*queue.tail.ptr).next.get_mut() = next };
                queue.tail = Cursor { ptr: next, idx: 1 };
            }
        }

        queue
    }
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use crate::NODE_SIZE;

    use super::OwnedQueue;

    #[test]
    fn test_push_pop() {
        let mut queue = OwnedQueue::new();
        queue.push_back(1);
        queue.push_back(2);
        queue.push_back(3);

        // test basic push/pop and FIFO order
        assert_eq!(queue.pop_front(), Some(1));
        assert_eq!(queue.pop_front(), Some(2));
        assert_eq!(queue.pop_front(), Some(3));
        assert_eq!(queue.pop_front(), None);
        assert_eq!(queue.pop_front(), None);

        // validate internal state
        assert_eq!(queue.head.ptr, queue.tail.ptr, "head and tail should be equal");
        assert_eq!(queue.head.idx, 3);
        assert_eq!(queue.tail.idx, 3);
    }

    #[test]
    fn test_is_empty() {
        let mut queue = OwnedQueue::new();
        assert!(queue.is_empty());

        queue.push_back(1);
        queue.push_back(2);
        assert!(!queue.is_empty());

        assert_eq!(queue.pop_front(), Some(1));
        assert_eq!(queue.pop_front(), Some(2));
        assert!(queue.is_empty());

        assert_eq!(queue.pop_front(), None);
    }

    #[test]
    fn test_len() {
        struct BulkQueue(OwnedQueue<i32>);

        impl BulkQueue {
            fn push_n(&mut self, n: usize) {
                for _ in 0..n {
                    self.0.push_back(0);
                }
            }

            fn pop_n(&mut self, n: usize) {
                for _ in 0..n {
                    let _ = self.0.pop_front();
                }
            }
        }

        let mut queue = BulkQueue(OwnedQueue::new());
        assert_eq!(queue.0.len(), 0);

        queue.push_n(123);
        assert_eq!(queue.0.len(), 123);

        queue.pop_n(77);
        assert_eq!(queue.0.len(), 46);

        queue.push_n(7511);
        assert_eq!(queue.0.len(), 7557);

        queue.pop_n(2987);
        assert_eq!(queue.0.len(), 4570);

        queue.pop_n(4569);
        assert_eq!(queue.0.len(), 1);
    }

    #[test]
    fn test_from_iter() {
        let mut queue = OwnedQueue::from_iter((0..=100).step_by(10));
        for i in 0..=10 {
            assert_eq!(queue.pop_front(), Some(10 * i));
        }

        assert_eq!(queue.pop_front(), None);
    }

    #[test]
    fn test_drop() {
        use std::cell::Cell;

        struct Canary<'a>(&'a Cell<u32>);
        impl Drop for Canary<'_> {
            fn drop(&mut self) {
                let count = self.0.get();
                self.0.set(count + 1);
            }
        }

        let counter = Cell::new(0);

        let mut queue = OwnedQueue::new();
        for _ in 0..(crate::NODE_SIZE * 3) {
            queue.push_back(Canary(&counter));
        }

        std::mem::drop(queue);

        assert_eq!(counter.get() as usize, NODE_SIZE * 3);
    }
}
