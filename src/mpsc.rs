//! The multi-producer, single-consumer (MPSC) variant of the *looqueue* algorithm.

use std::{
    cell::Cell,
    iter::FromIterator,
    mem::{self, ManuallyDrop},
    ptr::NonNull,
    sync::atomic::{AtomicPtr, Ordering},
};

use crate::{
    refcount::RefCounts,
    slot::{ConsumeResult, WriteResult},
    AtomicTagPtr, ControlBlock, Cursor, Node, NotInserted, OwnedQueue, TagPtr, MAX_PRODUCERS,
    NODE_SIZE,
};

/// Creates a new concurrent multi-producer, single-consumer (MPSC) queue and returns a (cloneable)
/// [`Producer`] handle and a unique [`Consumer`] handle to that queue.
pub fn queue<T>() -> (Producer<T>, Consumer<T>) {
    // allocate the reference-counted queue handle
    let ptr = NonNull::from(Box::leak(Box::new(ArcQueue {
        counts: RefCounts::default(),
        raw: RawQueue::new(),
    })));

    (Producer { ptr }, Consumer { ptr })
}

/// Creates a new multi-producer, single-consumer (MPSC) queue from an [`Iterator`] and returns a
/// (cloneable) [`Producer`] handle and a unique [`Consumer`] handle to that queue.
pub fn from_iter<T>(iter: impl Iterator<Item = T>) -> (Producer<T>, Consumer<T>) {
    // collect the iterator (single-threaded) into a owned queue
    let (head, tail) = OwnedQueue::from_iter(iter).into_raw_parts();
    // allocate the reference-counted queue handle
    let ptr = NonNull::from(Box::leak(Box::new(ArcQueue {
        counts: RefCounts::default(),
        raw: RawQueue {
            head: Cell::new(head),
            tail: AtomicTagPtr::new(TagPtr::compose(tail.ptr, tail.idx)),
            tail_cached: AtomicPtr::new(tail.ptr),
        },
    })));

    (Producer { ptr }, Consumer { ptr })
}

/// A producer handle to a [`mpsc`](crate::mpsc) queue.
///
/// Producer handles may be cloned, allowing multiple threads to push elements in safe manner, but
/// at most [`MAX_PRODUCERS`](crate::MAX_PRODUCERS) may exist at the same time
pub struct Producer<T> {
    ptr: NonNull<ArcQueue<T>>,
}

// SAFETY: Producers can be sent (Send) across threads but not shared (!Sync)
unsafe impl<T: Send> Send for Producer<T> {}
// unsafe impl<T> !Sync for Producer<T> {}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        unsafe { self.ptr.as_ref().counts.increase_producer_count(MAX_PRODUCERS) };
        Self { ptr: self.ptr }
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        let is_last = unsafe { self.ptr.as_ref().counts.decrease_producer_count() };
        if is_last {
            // SAFETY: there are now other live handles and none can be created anymore at this
            // point, so the handle can be safely deallocated
            let _ = unsafe { Box::from_raw(self.ptr.as_ptr()) };
        }
    }
}

impl<T> Producer<T> {
    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        let (is_empty, _) = unsafe { self.ptr.as_ref().raw.check_empty() };
        is_empty
    }

    /// Returns the current count of live producer handles.
    pub fn producer_count(&self) -> usize {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        unsafe { self.ptr.as_ref().counts.producer_count() }
    }

    /// Pushes `elem` to the back of the queue.
    pub fn push_back(&self, elem: T) {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        unsafe { self.ptr.as_ref().raw.push_back(elem) }
    }
}

/// A unique consumer handle to a [`mpsc`](crate::mpsc) queue.
pub struct Consumer<T> {
    ptr: NonNull<ArcQueue<T>>,
}

// SAFETY: Consumers can be sent (Send) across threads but not shared (!Sync), but must not be
// allowed to "smuggle" !Send types between threads (thus requiring `T: Send`)
unsafe impl<T: Send> Send for Consumer<T> {}
// unsafe impl<T> !Sync for Consumer<T> {}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        let is_last = unsafe { self.ptr.as_ref().counts.decrease_consumer_count() };
        if is_last {
            // SAFETY: there are now other live handles and none can be created anymore at this
            // point, so the handle can be safely deallocated
            mem::drop(unsafe { Box::from_raw(self.ptr.as_ptr()) });
        }
    }
}

impl<T> Consumer<T> {
    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        let (is_empty, _) = unsafe { self.ptr.as_ref().raw.check_empty() };
        is_empty
    }

    /// Returns the current count of live producer handles.
    pub fn consumer_count(&self) -> usize {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        unsafe { self.ptr.as_ref().counts.consumer_count() }
    }

    /// Pops the element at the front of the queue or returns `None` if it is empty.
    pub fn pop_front(&self) -> Option<T> {
        // SAFETY: pointer deref is sound, since at least one live handle exists, since `self` is
        // a unique handle, there can be no concurrent calls to `pop_front()`
        unsafe { self.ptr.as_ref().raw.pop_front() }
    }
}

/// A wrapper containing both the raw queue and its reference counters to which all producers and
/// consumers must hold a handle.
struct ArcQueue<T> {
    counts: RefCounts,
    raw: RawQueue<T>,
}

struct RawQueue<T> {
    /// The (pointer, index) tuple for the queue's head node, which must only ever be accessed by
    /// the single consumer.
    head: Cell<Cursor<T>>,
    /// The atomic (pointer, index) tuple for the queue's tail node, which may be accessed by all
    /// producers concurrently.
    tail: AtomicTagPtr<Node<T>>,
    /// A cached copy of the pointer to the tail node (may lag behind).
    tail_cached: AtomicPtr<Node<T>>,
}

impl<T> RawQueue<T> {
    /// Returns a new `RawQueue`.
    fn new() -> Self {
        let node = Node::alloc();
        Self {
            head: Cell::new(Cursor { ptr: node, idx: 0 }),
            tail: AtomicTagPtr::new(TagPtr::compose(node, 0)),
            tail_cached: AtomicPtr::new(node),
        }
    }

    /// Returns `true` if the queue is empty.
    fn check_empty(&self) -> (bool, Cursor<T>) {
        // read the head and tail (pointer, index) tuples to check, whether the queue is empty
        let head = self.head.get();
        let tail_cached = self.tail_cached.load(Ordering::Acquire);

        if head.ptr == tail_cached {
            let (tail, tail_idx) = self.tail.load(Ordering::Relaxed).decompose();
            if tail != tail_cached {
                let _ = self.tail_cached.compare_exchange(
                    tail_cached,
                    tail,
                    Ordering::Release,
                    Ordering::Relaxed,
                );

                if head.ptr == tail && tail_idx <= head.idx {
                    return (true, head);
                }
            }
        }

        (false, head)
    }

    /// Pushes `elem` to the back of the queue.
    ///
    /// # Safety
    ///
    /// Must not be called concurrently by more than [`MAX_PRODUCERS`](crate::MAX_PRODUCERS) threads.
    unsafe fn push_back(&self, elem: T) {
        // the ownership of `elem` becomes fuzzy during the subsequent loop and must not be dropped
        let elem = ManuallyDrop::new(elem);
        loop {
            // increment the current tail's associated index
            let (tail, idx) = self.tail.fetch_add(1, Ordering::Acquire).decompose();
            if idx < NODE_SIZE {
                // a valid index into the node's slot array was exclusively reserved by this thread,
                // so we tentatively write `elem` into the reserved slot and assess the success of
                // that operation
                match (*tail).slots[idx].write_tentative(&elem) {
                    // the write was successful and ownership of `elem` was transfered to the queue
                    WriteResult::Success => return,
                    // the write was unsuccessful and the slot has to be abandoned
                    WriteResult::Abandon { resume_check } => {
                        if resume_check {
                            Node::check_slots_and_try_reclaim::<true>(tail, idx + 1);
                        }

                        continue;
                    }
                }
            }

            // try to append a new node with elem already inserted
            match self.try_advance_tail(&elem, tail) {
                Ok(_) => return,
                Err(_) => continue,
            }
        }
    }

    /// Pops the element at the front of the queue or returns `None` if it is empty.
    ///
    /// # Safety
    ///
    /// Must not be called concurrently, i.e. only by one single consumer.
    unsafe fn pop_front(&self) -> Option<T> {
        loop {
            let (is_empty, Cursor { ptr: head, idx }) = self.check_empty();
            if is_empty {
                return None;
            }

            if idx < NODE_SIZE {
                // a valid index into the node's slot array was exclusively reserved by this thread,
                // so we attempt to consume the slot's element, which SHOULD be initialized by now,
                // but this can not be guaranteed; since there is only one consumer, having to
                // resume any previously halted check procedures is no concern
                let res = (*head).slots[idx].try_consume();
                self.head.set(Cursor { ptr: head, idx: idx + 1 });

                match res {
                    // the slot was successfully consumed and the retrieved element can be returned
                    ConsumeResult::Success { elem, .. } => return Some(elem),
                    // the slot could not be consumed, since the corresponding write had not been
                    // completed in time
                    ConsumeResult::Abandon { .. } => continue,
                }
            }

            // the first "slow path" call initiates the check-slots procedure
            if idx == NODE_SIZE {
                // increment the idx for a final time to ensure this branch is only called once
                // and function can be called again, if the there is no next node yet
                self.head.set(Cursor { ptr: head, idx: idx + 1 });
                // the check can never succeede to reclaim the node, because the pop operation
                // itself is not yet accounted for (i.e., the HEAD_ADVANCED bit is not yet set),
                // hence the method is called with `RECLAIM = false`
                unsafe { Node::check_slots_and_try_reclaim::<false>(head, 0) };
            }

            // read tail again to ensure that `None` is never returned after a linearized push
            if head == self.tail.load(Ordering::Acquire).decompose_ptr() {
                return None;
            }

            // next does not have to be checked for null, since it was already determined, that
            // head != tail, which is sufficient as next is always set before updating tail
            unsafe {
                let next = (*head).next.load(Ordering::Acquire);
                self.head.set(Cursor { ptr: next, idx: 0 });
                Node::set_flag_and_try_reclaim::<{ ControlBlock::HEAD_ADVANCED }, true>(head);
            }
        }
    }

    /// Allocates a new node with `elem` pre-inserted in its first slot and attempts to append this
    /// node to the queue's tail.
    ///
    /// If appending succeeds, an `Ok` result is returned and ownership of `elem` is transferred to
    /// the queue.
    unsafe fn try_advance_tail(
        &self,
        elem: &ManuallyDrop<T>,
        tail: *mut Node<T>,
    ) -> Result<(), NotInserted> {
        unsafe { crate::try_advance_tail(&self.tail, &self.tail_cached, elem, tail) }
    }
}

impl<T> Drop for RawQueue<T> {
    fn drop(&mut self) {
        // converts the queue into an `OwnedQueue` and drops it
        let head = self.head.get();
        let (ptr, idx) = self.tail.load(Ordering::Relaxed).decompose();
        let tail = Cursor { ptr, idx };
        mem::drop(unsafe { OwnedQueue::from_raw_parts(head, tail) });
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, mem};

    #[test]
    fn test_push_pop() {
        let (tx, rx) = super::queue();
        tx.push_back(1);
        tx.push_back(2);
        tx.push_back(3);

        assert_eq!(rx.pop_front(), Some(1));
        assert_eq!(rx.pop_front(), Some(2));
        assert_eq!(rx.pop_front(), Some(3));
        assert_eq!(rx.pop_front(), None);
    }

    #[test]
    fn test_from_iter() {
        let (_, rx) = super::from_iter((0..).skip(10).take(10));
        for i in 10..20 {
            assert_eq!(rx.pop_front(), Some(i));
        }

        assert_eq!(rx.pop_front(), None);
    }

    #[test]
    fn test_clone() {
        struct Canary<'a>(&'a Cell<bool>);
        impl Drop for Canary<'_> {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }

        let flag = Cell::new(false);
        let (tx, _) = super::queue();
        tx.push_back(Canary(&flag));
        assert_eq!(tx.producer_count(), 1);

        let (tx2, tx3, tx4) = (tx.clone(), tx.clone(), tx.clone());
        assert_eq!(tx.producer_count(), 4);

        // dropping all handles must lead to the queue to be dropped as well
        mem::drop(tx);
        mem::drop(tx2);
        mem::drop(tx3);
        mem::drop(tx4);

        assert!(flag.get());
    }
}
