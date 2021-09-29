//! The multi-producer, single-consumer (MPSC) variant of the *looqueue* algorithm.

use std::{
    alloc,
    cell::Cell,
    fmt,
    iter::FromIterator,
    mem::{self, ManuallyDrop},
    ptr::NonNull,
    sync::atomic::{AtomicPtr, Ordering},
};

use crate::{
    refcount::RefCounts,
    slot::{ConsumeResult, WriteResult},
    AtomicTagPtr, ControlBlock, Cursor, NoNextNode, Node, NotInserted, OwnedQueue, TagPtr,
    MAX_PRODUCERS, NODE_SIZE,
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
    /// The pointer to the reference counted queue.
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

impl<T> fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Producer {{ ... }}")
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        let is_last = unsafe { self.ptr.as_ref().counts.decrease_producer_count() };
        if is_last {
            // SAFETY: there are no other live handles and none can be created anymore at this
            // point, so the handle can be safely deallocated
            mem::drop(unsafe { Box::from_raw(self.ptr.as_ptr()) });
        }
    }
}

impl<T> Producer<T> {
    /// Attempts to unwrap and convert the queue backing this handle into an [`OwnedQueue`].
    ///
    /// # Errors
    ///
    /// Unwrapping will fail, if this handle is not the **only** live handle to the queue, in which
    /// case the original handle is returned unaltered.
    ///
    /// # Example
    ///
    /// ```
    /// # fn main() -> Result<(), loo::mpsc::Producer<usize>> {
    /// let (tx, _) = loo::mpsc::from_iter(0..10); // consumer is dropped immediately
    /// let queue = tx.try_unwrap()?;
    /// assert_eq!(queue.iter().count(), 10);
    /// # Ok(())
    /// # }
    /// ```
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
        let (is_empty, _) = unsafe { self.ptr.as_ref().raw.check_empty() };
        is_empty
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
}

/// A (unique) consumer handle to a [`mpsc`](crate::mpsc) queue.
pub struct Consumer<T> {
    /// The pointer to the reference counted queue.
    ptr: NonNull<ArcQueue<T>>,
}

// SAFETY: Consumers can be sent (Send) across threads but not shared (!Sync), but must not be
// allowed to "smuggle" !Send types between threads (thus requiring `T: Send`)
unsafe impl<T: Send> Send for Consumer<T> {}
// unsafe impl<T> !Sync for Consumer<T> {}

impl<T> fmt::Debug for Consumer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Consumer {{ ... }}")
    }
}

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
    /// Attempts to unwrap and convert the queue backing this handle into an [`OwnedQueue`].
    ///
    /// # Errors
    ///
    /// Unwrapping will fail, if this handle is not the **only** live handle to the queue, in which
    /// case the original handle is returned unaltered.
    ///
    /// # Example
    ///
    /// ```
    /// # fn main() -> Result<(), loo::mpsc::Consumer<usize>> {
    /// let (_, rx) = loo::mpsc::from_iter(0..10); // producer is dropped immediately
    /// let queue = rx.try_unwrap()?;
    /// assert_eq!(queue.iter().count(), 10);
    /// # Ok(())
    /// # }
    /// ```    
    pub fn try_unwrap(self) -> Result<OwnedQueue<T>, Self> {
        // SAFETY: pointer deref is sound, since at least one live handle exists
        if unsafe { self.ptr.as_ref().counts.producer_count() == 0 } {
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
        // an unique handle, there can be no concurrent calls to `pop_front()`
        unsafe { self.ptr.as_ref().raw.pop_front() }
    }

    /// Returns an iterator consuming each element in the queue.
    pub fn drain(&self) -> impl Iterator<Item = T> + '_ {
        std::iter::from_fn(move || self.pop_front())
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
            // "extract" the contents from the heap (but keep the source intact)
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
        // allocate an empty, aligned initial node
        let node = Node::aligned_alloc();
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

        // if the cached tail points to a different node, head and tail CAN NOT point to the same
        // node, even if the cached tail is lagging behind and the queue must be non-empty
        if head.ptr != tail_cached {
            return (false, head);
        }

        // head and tail (potentially) point to the same node, so we need to compare their
        // indices, which is undesirable because it requires loading the potentially highly
        // contended tail pointer
        let tail: Cursor<_> = self.tail.load(Ordering::Relaxed).decompose().into();
        // check if the cached tail is lagging behind and help updating it, if it is
        if tail.ptr != tail_cached {
            let _ = self.tail_cached.compare_exchange(
                tail_cached,
                tail.ptr,
                Ordering::Release,
                Ordering::Relaxed,
            );

            // since the "real" tail is already ahead, the queue must be non-empty as well
            return (false, head);
        }

        // check, if the tail index is ahead of the head index
        let is_empty = tail.idx <= head.idx;
        (is_empty, head)
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
            // exit early and without incrementing the pop index if the queue is empty
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

            match self.try_advance_head(head, idx) {
                Ok(_) => continue,
                Err(_) => return None,
            }
        }
    }

    #[inline(never)]
    #[cold]
    unsafe fn try_advance_head(&self, head: *mut Node<T>, idx: usize) -> Result<(), NoNextNode> {
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
        // (the head node must only be reclaimed after it has been advanced)
        if head == self.tail.load(Ordering::Acquire).decompose_ptr() {
            return Err(NoNextNode);
        }

        // next does not have to be checked for null, since it was already determined, that
        // head != tail, which is sufficient as next is always set before updating tail
        unsafe {
            let next = (*head).next.load(Ordering::Acquire);
            self.head.set(Cursor { ptr: next, idx: 0 });
            Node::set_flag_and_try_reclaim::<{ ControlBlock::HEAD_ADVANCED }, true>(head);
        }

        Ok(())
    }

    /// Allocates a new node with `elem` pre-inserted in its first slot and attempts to append this
    /// node to the queue's tail.
    ///
    /// If appending succeeds, an `Ok` result is returned and ownership of `elem` is transferred to
    /// the queue.
    #[inline(never)]
    #[cold]
    unsafe fn try_advance_tail(
        &self,
        elem: &ManuallyDrop<T>,
        tail: *mut Node<T>,
    ) -> Result<(), NotInserted> {
        unsafe { crate::try_advance_tail(&self.tail, &self.tail_cached, elem, tail) }
    }

    /// Leaks the queue and returns it's head and tail (pointer, index) tuples in their raw
    /// representation.
    fn into_raw_parts(self) -> (Cursor<T>, Cursor<T>) {
        let cursors = self.cursors_unsync();
        mem::forget(self);
        cursors
    }

    fn cursors_unsync(&self) -> (Cursor<T>, Cursor<T>) {
        let head = self.head.get();
        let (ptr, idx) = self.tail.load(Ordering::Relaxed).decompose();
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

    #[test]
    fn test_iter() {
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
            assert_eq!(raw.head.get().idx, 6);
        }
        assert_eq!(res, &[4, 5, 6]);
    }

    #[test]
    fn test_full_node() {
        const N: usize = crate::NODE_SIZE + 1;

        let (tx, rx) = super::queue();
        for i in 0..N {
            tx.push_back(i);
        }

        let res: Vec<_> = rx.drain().collect();
        assert_eq!(res.len(), N);

        // sanity/internal consistency check
        unsafe {
            let raw = &rx.ptr.as_ref().raw;
            assert_eq!(raw.head.get().idx, 1);
        }
    }

    #[test]
    fn test_unwrap() {
        let (tx, _) = super::queue();
        tx.push_back(1);

        let mut iter = tx.try_unwrap().unwrap().into_iter();
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.next(), None);
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

    #[test]
    fn test_multi_nodes() {
        const N: usize = crate::NODE_SIZE * 100;

        let (_, rx) = super::from_iter(0..N);
        for i in 0..N {
            assert_eq!(rx.pop_front(), Some(i));
        }

        assert_eq!(rx.pop_front(), None);

        // sanity/internal consistency check
        unsafe {
            let raw = &rx.ptr.as_ref().raw;
            assert_eq!(raw.head.get().idx, crate::NODE_SIZE);
        }
    }
}
