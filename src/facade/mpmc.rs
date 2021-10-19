//! The multi-producer, multi-consumer (MPMC) variant of the *looqueue* algorithm.

use std::{
    alloc, fmt,
    mem::{self, ManuallyDrop},
    ptr::NonNull,
    sync::atomic::{AtomicPtr, Ordering},
};

use crate::facade::{
    owned::OwnedQueue,
    refcount::{DropResult, RefCounts},
    slot::{ConsumeResult, WriteResult},
    AtomicTagPtr, Cursor, NoNextNode, Node, NotInserted, TagPtr, NODE_SIZE,
};
use crate::{MAX_CONSUMERS, MAX_PRODUCERS};

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
    /// The pointer to the reference counted queue.
    ptr: NonNull<ArcQueue<T>>,
}

// SAFETY: Producers can be sent (Send) across threads but not shared (!Sync), since it must be
// guaranteed, that there are never more than MAX_PRODUCERS threads concurrently pushing values
unsafe impl<T: Send> Send for Producer<T> {}
// unsafe impl<T> !Sync for Producer<T> {}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        unsafe { self.ptr.as_ref().counts.increase_producer_count(MAX_PRODUCERS) };
        Self { ptr: self.ptr }
    }
}

impl<T> fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Producer {{ ... }}")
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        // SAFETY: can never be called more than once
        let _ = unsafe { self.drop_explicit() };
    }
}

impl<T> Producer<T> {
    /// Attempts to unwrap and convert the queue backing this handle into an [`OwnedQueue`].
    ///
    /// # Errors
    ///
    /// Unwrapping will fail, if this handle is not the **only** live handle to the queue, in which
    /// case the original handle is returned unaltered.
    pub fn try_unwrap(self) -> Result<OwnedQueue<T>, Self> {
        // SAFETY: pointer deref is sound, since at least one live handle exists; the reference does
        // not live beyond this unsafe block
        let is_last = unsafe {
            let arc = self.ptr.as_ref();
            arc.counts.consumer_count() == 0 && arc.counts.producer_count() == 1
        };

        if is_last {
            // extract the pointer to the queue
            let queue = self.ptr.as_ptr();
            // forget the handle to prevent its `drop` method to run
            mem::forget(self);
            // SAFETY: unwrapping is safe, since the handle is gone and `queue` is the final pointer
            Ok(unsafe { ArcQueue::unwrap_owned(queue) })
        } else {
            Err(self)
        }
    }

    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        unsafe { self.ptr.as_ref().raw.is_empty() }
    }

    /// Returns the current count of live producer handles.
    pub fn producer_count(&self) -> usize {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        unsafe { self.ptr.as_ref().counts.producer_count() }
    }

    /// Returns the current count of live consumer handles.
    pub fn consumer_count(&self) -> usize {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        unsafe { self.ptr.as_ref().counts.consumer_count() }
    }

    /// Pushes `elem` to the back of the queue.
    pub fn push_back(&self, elem: T) {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        unsafe { self.ptr.as_ref().raw.push_back(elem) }
    }

    /// Drops this handle without consuming it and returns a result indicating how many other
    /// handles still remain
    ///
    /// # Safety
    ///
    /// This method must be called at most once and the handle must not be used again after.
    pub unsafe fn drop_explicit(&mut self) -> Option<DropResult> {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        let res = unsafe { self.ptr.as_ref().counts.decrease_producer_count() };
        if let Some(DropResult::LastOfAny) = &res {
            // SAFETY: there are no other live handles and none can be created anymore at this
            // point, so the handle can be safely deallocated
            mem::drop(unsafe { Box::from_raw(self.ptr.as_ptr()) });
        }

        res
    }
}

/// A consumer handle to a [`mpmc`](crate::mpmc) queue.
///
/// Consumer handles may be cloned, allowing multiple threads to pop elements in safe manner, but
/// at most [`MAX_CONSUMERS`](crate::MAX_CONSUMERS) may exist at the same time.
/// Attempting to create additional handles causes calls to [`clone`](Clone::clone) to panic.
pub struct Consumer<T> {
    /// The pointer to the reference counted queue.
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

impl<T> fmt::Debug for Consumer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Consumer {{ ... }}")
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        // SAFETY: can never be called more than once
        let _ = unsafe { self.drop_explicit() };
    }
}

impl<T> Consumer<T> {
    /// Attempts to unwrap and convert the queue backing this handle into an [`OwnedQueue`].
    ///
    /// # Errors
    ///
    /// Unwrapping will fail, if this handle is not the **only** live handle to the queue, in which
    /// case the original handle is returned unaltered.
    pub fn try_unwrap(self) -> Result<OwnedQueue<T>, Self> {
        // SAFETY: pointer deref is sound, since at least one live handle exists; the reference does
        // not live beyond this unsafe block
        let is_last = unsafe {
            let arc = self.ptr.as_ref();
            arc.counts.producer_count() == 0 && arc.counts.consumer_count() == 1
        };

        if is_last {
            // extract the pointer to the queue
            let queue = self.ptr.as_ptr();
            // forget the handle to prevent its `drop` method to run
            mem::forget(self);
            // SAFETY: unwrapping is safe, since the handle is gone and `queue` is the final pointer
            Ok(unsafe { ArcQueue::unwrap_owned(queue) })
        } else {
            Err(self)
        }
    }

    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        unsafe { self.ptr.as_ref().raw.is_empty() }
    }

    /// Returns the current count of live producer handles.
    pub fn producer_count(&self) -> usize {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        unsafe { self.ptr.as_ref().counts.producer_count() }
    }

    /// Returns the current count of live consumer handles.
    pub fn consumer_count(&self) -> usize {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        unsafe { self.ptr.as_ref().counts.consumer_count() }
    }

    /// Pops the element at the front of the queue or returns `None` if it is empty.
    pub fn pop_front(&self) -> Option<T> {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        unsafe { self.ptr.as_ref().raw.pop_front() }
    }

    /// Returns an iterator consuming each element in the queue.
    pub fn drain(&self) -> impl Iterator<Item = T> + '_ {
        std::iter::from_fn(move || self.pop_front())
    }

    /// Drops this handle without consuming it and returns a result indicating how many other
    /// handles still remain
    ///
    /// # Safety
    ///
    /// This method must be called at most once and the handle must not be used again after.
    pub unsafe fn drop_explicit(&mut self) -> Option<DropResult> {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        let res = unsafe { self.ptr.as_ref().counts.decrease_producer_count() };
        if let Some(DropResult::LastOfAny) = &res {
            // SAFETY: there are no other live handles and none can be created anymore at this
            // point, so the handle can be safely deallocated
            mem::drop(unsafe { Box::from_raw(self.ptr.as_ptr()) });
        }

        res
    }
}

/// A wrapper containing both the raw queue and its reference counters to which all producers and
/// consumers must hold a handle.
struct ArcQueue<T> {
    /// The queue's producer/consumer reference counts.
    counts: RefCounts,
    /// The raw queue itself.
    raw: RawQueue<T>,
}

impl<T> ArcQueue<T> {
    /// Extracts `queue` into an [`OwnedQueue`] and de-allocates the memory at the address of the
    /// pointer.
    ///
    /// # Safety
    ///
    /// The `queue` pointer must be non-null, live and there must be no other references to the
    /// queue other than this pointer.
    unsafe fn unwrap_owned(queue: *mut Self) -> OwnedQueue<T> {
        unsafe {
            // "extract" the contents from the heap (but keep the source memory intact)
            let ArcQueue { raw, .. } = queue.read();
            // allocate the memory without dropping anything!
            alloc::dealloc(queue.cast(), alloc::Layout::new::<Self>());
            // convert the extracted queue into an owned queue
            let (head, tail) = raw.into_raw_parts();
            OwnedQueue::from_raw_parts(head, tail)
        }
    }
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
        // allocate an empty, aligned initial node
        let node = Node::aligned_alloc();
        Self {
            head: AtomicTagPtr::new(TagPtr::compose(node, 0)),
            tail: AtomicTagPtr::new(TagPtr::compose(node, 0)),
            tail_cached: AtomicPtr::new(node),
        }
    }

    /// Returns `true` if the queue is empty.
    fn is_empty(&self) -> bool {
        // on x86-64 and as long as LLVM isn't trying to be overly smart and "optimize" this by
        // compiling it as `mov` instruction, this should be compiled down to a `lock; xadd`
        // instruction, which can *dramatically* speed up pop operations under contention
        // (this is because the `lock; xadd` places the cache line in which `head` resides into
        // an "owned" state for this CPU, which means the subsequent it will likely still be in that
        // state when the subsequent `fetch_add(1)` is executed a few lines later.
        // Also, since `head` is never read by an ordinary `mov` instruction by any thread, the
        // cache line is never in the "shared" state, which reduces the amount of work required by
        // the cache-coherence protocol, in effect the cache line ping-pongs from one CPUs ownership
        // to another's)
        let head: Cursor<_> = self.head.0.fetch_add(0, Ordering::Relaxed).decompose().into();
        let tail_cached = self.tail_cached.load(Ordering::Acquire);

        // if the cached tail points to a different node, head and tail CAN NOT point to the same
        // node, even if the cached tail is lagging behind and the queue must be non-empty
        if head.ptr != tail_cached {
            return false;
        }

        // head and tail (potentially) point to the same node, so we need to compare their
        // indices, which is undesirable because it requires loading the potentially highly
        // contended tail pointer
        let tail: Cursor<_> = self.tail.0.load(Ordering::Relaxed).decompose().into();
        // check if the cached tail is lagging behind and help updating it, if it is
        if tail.ptr != tail_cached {
            let _ = self.tail_cached.compare_exchange(
                tail_cached,
                tail.ptr,
                Ordering::Release,
                Ordering::Relaxed,
            );

            // since the "real" tail is already ahead, the queue must be non-empty as well
            return false;
        }

        // check, if the tail index is ahead of the head index
        tail.idx <= head.idx
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
            let (tail, idx) = self.tail.0.fetch_add(1, Ordering::Acquire).decompose();
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
                            // SAFETY: the check can be safely resumed, since the consume has
                            // detected the CONTINUE bit in the current slot
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
            // exit early and without incrementing the pop index if the queue is empty
            if self.is_empty() {
                return None;
            }

            let current = self.head.0.fetch_add(1, Ordering::AcqRel);
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
                            // SAFETY: the check can be safely resumed, since the consume has
                            // detected the CONTINUE bit in the current slot
                            unsafe { Node::check_slots_and_try_reclaim::<true>(head, idx + 1) };
                        }

                        return Some(elem);
                    }
                    // the slot could not be consumed, since the corresponding write had not been
                    // completed in time
                    ConsumeResult::Abandon { resume_check } => {
                        if resume_check {
                            // SAFETY: the check can be safely resumed, since the consume has
                            // detected the CONTINUE bit in the current slot
                            unsafe { Node::check_slots_and_try_reclaim::<true>(head, idx + 1) };
                        }

                        continue;
                    }
                };
            }

            // attempt to advance head to its successor node and retry, if there is one
            match self.try_advance_head(current, head, idx) {
                Ok(_) => continue,
                Err(_) => return None,
            }
        }
    }

    /// Attempts to advance the queue's head to its successor node.
    ///
    /// This method is marked as **cold** exempted from inlining to keep this relatively rarely
    /// executed code out of the fast path and reduce the fast path's instruction cache impact.
    ///
    /// # Errors
    ///
    /// Fails, if there is no next node yet.
    #[inline(never)]
    #[cold]
    unsafe fn try_advance_head(
        &self,
        mut current: TagPtr<Node<T>>,
        head: *mut Node<T>,
        idx: usize,
    ) -> Result<(), NoNextNode> {
        // the first "slow path" call initiates the check-slots procedure
        if idx == NODE_SIZE {
            // SAFETY: since each idx is unique, it's safe to initiate (start_idx = 0) the check
            unsafe { Node::check_slots_and_try_reclaim::<false>(head, 0) };
        }

        // read tail again to ensure that `None` is never returned after a linearized push
        if head == self.tail.0.load(Ordering::Acquire).decompose_ptr() {
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

    #[inline(never)]
    #[cold]
    unsafe fn try_advance_tail(
        &self,
        elem: &ManuallyDrop<T>,
        tail: *mut Node<T>,
    ) -> Result<(), NotInserted> {
        unsafe { crate::facade::try_advance_tail(&self.tail, &self.tail_cached, elem, tail) }
    }

    #[inline(always)]
    fn cas_head_loop(
        &self,
        current: TagPtr<Node<T>>,
        new: TagPtr<Node<T>>,
        old: *mut Node<T>,
    ) -> Option<u32> {
        unsafe { crate::facade::cas_atomic_tag_ptr_loop(&self.head, current, new, old) }
    }

    fn into_raw_parts(self) -> (Cursor<T>, Cursor<T>) {
        let cursors = self.cursors_unsync();
        mem::forget(self);
        cursors
    }

    fn cursors_unsync(&self) -> (Cursor<T>, Cursor<T>) {
        let (ptr, idx) = self.head.0.load(Ordering::Relaxed).decompose();
        let head = Cursor { ptr, idx };
        let (ptr, idx) = self.tail.0.load(Ordering::Relaxed).decompose();
        let tail = Cursor { ptr, idx };

        (head, tail)
    }
}

impl<T> Drop for RawQueue<T> {
    fn drop(&mut self) {
        // converts the queue into an `OwnedQueue` and drops it
        let (head, tail) = self.cursors_unsync();
        mem::drop(unsafe { OwnedQueue::from_raw_parts(head, tail) });
    }
}

#[cfg(test)]
mod tests {
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
    fn test_drain() {
        let (tx, rx) = super::queue();
        tx.push_back(1);
        tx.push_back(2);
        tx.push_back(3);

        let res: Vec<_> = rx.drain().collect();
        assert_eq!(res, &[1, 2, 3]);

        tx.push_back(4);
        tx.push_back(5);
        tx.push_back(6);

        let res: Vec<_> = rx.drain().collect();
        // sanity/internal consistency check
        unsafe {
            let raw = &rx.ptr.as_ref().raw;
            let idx = raw.head.0.load(std::sync::atomic::Ordering::Relaxed).decompose_tag();
            assert_eq!(idx, 6)
        }
        assert_eq!(res, &[4, 5, 6]);
    }

    #[test]
    fn test_unwrap_producer() {
        let (tx, rx) = super::queue();
        tx.push_back(1);

        // both handles still alive, rx is dropped afterwards
        assert!(tx.try_unwrap().is_err(), "unwrapping must fail");
        // unwrapping must succeed, since tx is already gone
        let mut owned = rx.try_unwrap().unwrap();
        assert_eq!(owned.pop_front(), Some(1));
        assert_eq!(owned.pop_front(), None);
    }

    #[test]
    fn test_unwrap_consumer() {
        let (tx, rx) = super::queue();
        tx.push_back(1);

        // both handles still alive, rx is dropped afterwards
        assert!(rx.try_unwrap().is_err(), "unwrapping must fail");
        // unwrapping must succeed, since rx is already gone
        let mut owned = tx.try_unwrap().unwrap();
        assert_eq!(owned.pop_front(), Some(1));
        assert_eq!(owned.pop_front(), None);
    }
}
