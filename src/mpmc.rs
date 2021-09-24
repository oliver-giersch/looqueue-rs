//! The multi-producer, multi-consumer (MPMC) variant of the *looqueue* algorithm.

use std::{
    mem::{self, ManuallyDrop},
    ptr::NonNull,
    sync::atomic::{AtomicPtr, Ordering},
};

use crate::{
    refcount::RefCounts,
    slot::{ConsumeResult, WriteResult},
    AtomicTagPtr, Cursor, Node, NotInserted, OwnedQueue, TagPtr, MAX_CONSUMERS, MAX_PRODUCERS,
    NODE_SIZE,
};

/// Creates a new concurrent multi-producer, multi-consumer (MPMC) queue and returns (cloneable)
/// [`Producer`] and [`Consumer`] handles to that queue.
#[must_use]
pub fn queue<T>() -> (Producer<T>, Consumer<T>) {
    // allocate the reference-counted queue handle
    let ptr = NonNull::from(Box::leak(Box::new(ArcQueue {
        counts: RefCounts::default(),
        raw: RawQueue::new(),
    })));

    (Producer { ptr }, Consumer { ptr })
}

/// Creates a new multi-producer, multi-consumer (MPMC) queue from an [`Iterator`] and returns
/// (cloneable) [`Producer`] and [`Consumer`] handles to that queue.
pub fn from_iter<T>(iter: impl Iterator<Item = T>) -> (Producer<T>, Consumer<T>) {
    // collect the iterator (single-threaded) into a owned queue
    let (head, tail) = iter.collect::<OwnedQueue<_>>().into_raw_parts();

    // allocate the reference-counted queue handle
    let ptr = NonNull::from(Box::leak(Box::new(ArcQueue {
        counts: RefCounts::default(),
        raw: RawQueue {
            head: AtomicTagPtr::new(TagPtr::compose(head.ptr, head.idx)),
            tail: AtomicTagPtr::new(TagPtr::compose(tail.ptr, tail.idx)),
            tail_cached: AtomicPtr::new(tail.ptr),
        },
    })));

    (Producer { ptr }, Consumer { ptr })
}

/// A producer handle to a [`mpmc`](crate::mpmc) queue.
///
/// Producer handles may be cloned, allowing multiple threads to push elements in safe manner, but
/// at most [`MAX_PRODUCERS`](crate::MAX_PRODUCERS) may exist at the same time.
/// Attempting to create additional handles causes calls to [`clone`](Clone::clone) to panic.
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
            mem::drop(unsafe { Box::from_raw(self.ptr.as_ptr()) });
        }
    }
}

impl<T> Producer<T> {
    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        unsafe { self.ptr.as_ref().raw.is_empty() }
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

/// A consumer handle to a [`mpmc`](crate::mpmc) queue.
///
/// Consumer handles may be cloned, allowing multiple threads to pop elements in safe manner, but
/// at most [`MAX_CONSUMERS`](crate::MAX_CONSUMERS) may exist at the same time.
/// Attempting to create additional handles causes calls to [`clone`](Clone::clone) to panic.
pub struct Consumer<T> {
    ptr: NonNull<ArcQueue<T>>,
}

// SAFETY: Consumer can be sent (Send) across threads but not shared (!Sync)
unsafe impl<T: Send> Send for Consumer<T> {}
// unsafe impl<T> !Sync for Consumer<T> {}

impl<T> Clone for Consumer<T> {
    fn clone(&self) -> Self {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        unsafe { self.ptr.as_ref().counts.increase_consumer_count(MAX_CONSUMERS) };
        Self { ptr: self.ptr }
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        let is_last = unsafe { self.ptr.as_ref().counts.decrease_consumer_count() };
        if is_last {
            mem::drop(unsafe { Box::from_raw(self.ptr.as_ptr()) });
        }
    }
}

impl<T> Consumer<T> {
    /// Pops the element at the front of the queue or returns `None` if it is empty.
    pub fn pop_front(&self) -> Option<T> {
        // SAFETY: pointer deref is sound, since at least one live handle exists
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
    /// The atomic (pointer, index) tuple for the queue's head node, which may be accessed by all
    /// consumers concurrently.
    head: AtomicTagPtr<Node<T>>,
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
            head: AtomicTagPtr::new(TagPtr::compose(node, 0)),
            tail: AtomicTagPtr::new(TagPtr::compose(node, 0)),
            tail_cached: AtomicPtr::new(node),
        }
    }

    /// Returns `true` if the queue is empty.
    fn is_empty(&self) -> bool {
        let (head, idx) = self.head.fetch_add(0, Ordering::Relaxed).decompose();
        let tail_cached = self.tail_cached.load(Ordering::Acquire);

        if head == tail_cached {
            let (tail, tail_idx) = self.tail.load(Ordering::Relaxed).decompose();
            // if cached tail is lagging behind, update it
            if tail_cached != tail {
                let _ = self.tail_cached.compare_exchange(
                    tail_cached,
                    tail,
                    Ordering::Release,
                    Ordering::Relaxed,
                );

                if head == tail && (idx >= NODE_SIZE || tail_idx <= idx) {
                    return true;
                }
            }
        }

        false
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
            // atomically increment the current tail's associated index
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
            if self.is_empty() {
                return None;
            }

            let current = self.head.fetch_add(1, Ordering::AcqRel);
            let (head, idx) = current.decompose();

            if idx < NODE_SIZE {
                // a valid index into the node's slot array was exclusively reserved by this thread,
                // so we attempt to consume the slot's element, which SHOULD be initialized by now,
                // but this can not be guaranteed; since there is only one consumer, having to
                // resume any previously halted check procedures is no concern
                match (*head).slots[idx].try_consume() {
                    // the slot was successfully consumed and the retrieved element can be returned
                    ConsumeResult::Success { elem, resume_check } => {
                        if resume_check {
                            unsafe { Node::check_slots_and_try_reclaim::<true>(head, 0) };
                        }

                        return Some(elem);
                    }
                    // the slot could not be consumed, since the corresponding write had not been
                    // completed in time
                    ConsumeResult::Abandon { resume_check } => {
                        if resume_check {
                            unsafe { Node::check_slots_and_try_reclaim::<true>(head, idx + 1) };
                        }

                        continue;
                    }
                };
            }

            // the first "slow path" call initiates the check-slots procedure
            if idx == NODE_SIZE {
                unsafe { Node::check_slots_and_try_reclaim::<false>(head, 0) };
            }

            // attempt to advance head to its successor node and retry, if there is one
            match self.try_advance_head(current, head) {
                Ok(_) => continue,
                Err(_) => return None,
            }
        }
    }

    unsafe fn try_advance_head(
        &self,
        mut current: TagPtr<Node<T>>,
        head: *mut Node<T>,
    ) -> Result<(), NoNextNode> {
        // read tail again to ensure that `None` is never returned after a linearized push
        if head == self.tail.load(Ordering::Acquire).decompose_ptr() {
            Node::count_pop_and_try_reclaim(head, None);
            return Err(NoNextNode);
        }

        // next does not have to be checked for null, since it was already determined, that
        // head != tail, which is sufficient as next is always set before updating tail
        let next = (*head).next.load(Ordering::Acquire);
        current = current.add_tag(1);

        // loop until the queue's head pointer is exchanged, then mark the operation as finished
        let final_count = self.cas_head_loop(current, TagPtr::compose(next, 0), head);
        Node::count_pop_and_try_reclaim(head, final_count);

        Ok(())
    }

    unsafe fn try_advance_tail(
        &self,
        elem: &ManuallyDrop<T>,
        tail: *mut Node<T>,
    ) -> Result<(), NotInserted> {
        unsafe { crate::try_advance_tail(&self.tail, &self.tail_cached, elem, tail) }
    }

    #[inline(always)]
    fn cas_head_loop(
        &self,
        current: TagPtr<Node<T>>,
        new: TagPtr<Node<T>>,
        old: *mut Node<T>,
    ) -> Option<u32> {
        unsafe { crate::cas_atomic_tag_ptr_loop(&self.head, current, new, old) }
    }
}

impl<T> Drop for RawQueue<T> {
    fn drop(&mut self) {
        let (ptr, idx) = self.head.load(Ordering::Relaxed).decompose();
        let head = Cursor { ptr, idx };
        let (ptr, idx) = self.tail.load(Ordering::Relaxed).decompose();
        let tail = Cursor { ptr, idx };

        mem::drop(unsafe { OwnedQueue::from_raw_parts(head, tail) });
    }
}

struct NoNextNode;
