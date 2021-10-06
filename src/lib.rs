//! An implementation of a thread-safe, lock-free and scalable FIFO queue algorithm.
//! Two variants of the algorithm are provided:
//!
//! 1. MPMC (Multi-Producer/Multi-Consumer)
//! 2. MPSC (Multi-Producer/Single-Consumer)
//!
//! The API is similar to that of [`std::sync::mpsc`] with separate types for producer
//! and consumer handles.
//! There is also [`OwnedQueue`], which is a single-threaded queue with an identical
//! internal structure for cheap conversion from/to either of the thread-safe variants
//! or initialization from iterators.
//!
//! The MPMC queue implementation is based on the algorithm in [\[1\]][1] and the MPSC
//! variant is derived from the MPMC algorithm described therein but optimized for the
//! additional constraint of at most a single consumer.
//!
//! One key aspect of the implemented algorithm is, that it does not rely on a dedicated
//! memory reclamation mechanism but comes with its own memory reclamation strategy,
//! that is lock-free, deterministic (rather than lazy/deferred) like reference counting
//! and has very little performance impact.
//! It does come with a limitation, however, in that there is a limit on how many
//! producers and consumers can safely interact with the same queue.
//! These limits are enforced by the implementation and they are encoded in the
//! two constants [`MAX_PRODUCERS`] and [`MAX_CONSUMERS`].
//! When the number of live producer/consumer handles would exceed these safe limits,
//! further attempts to [`clone`](Clone::clone) handles will panic!
//!
//! In the current implementation, there can be **961** producer and **480** consumer
//! handles.
//!
//! [1]: https://ieeexplore.ieee.org/document/9490347
//!
//! # Examples
//!
//! ```
//! # #[cfg(not(miri))]
//! # {
//! use std::sync::{Arc, Barrier, Mutex};
//! use std::thread;
//!
//! const N: usize = 100;
//! const THREADS: usize = 4;
//! const EXP_SUM: usize = (N * (N - 1)) / 2; // Gauss sum [1, 99]
//!
//! let (tx, rx) = loo::mpmc::queue();
//!
//! let barrier = Arc::new(Barrier::new(THREADS * 2));
//! let total_sum = Arc::new(Mutex::new(0));
//!
//! let mut handles = Vec::with_capacity(THREADS * 2);
//!
//! // spawn 4 producer threads, each pushing 100 elements in FIFO order
//! for _ in 0..THREADS {
//!     let tx = tx.clone();
//!     let barrier = Arc::clone(&barrier);
//!
//!     handles.push(thread::spawn(move || {
//!         barrier.wait();
//!
//!         for i in 0..100 {
//!             tx.push_back(i);
//!         }
//!     }));
//! }
//!
//! // spawn 4 consumer threads, each popping exactly 100 elements from the queue
//! // and summing them up
//! for _ in 0..THREADS {
//!     let rx = rx.clone();
//!     let barrier = Arc::clone(&barrier);
//!     let total_sum = Arc::clone(&total_sum);
//!     handles.push(thread::spawn(move || {
//!         let mut count = 0;
//!         let mut sum = 0;
//!
//!         barrier.wait();
//!
//!         while count < N { // retrieve exactly N elements
//!             if let Some(e) = rx.pop_front() {
//!                 count += 1;
//!                 sum += e;
//!             }    
//!         }
//!
//!         // add this thread's result to the total sum
//!         let mut lock = total_sum.lock().unwrap();
//!         *lock += sum;
//!     }));
//! }
//!
//! for handle in handles {
//!     handle.join().unwrap();
//! }
//!
//! let lock = total_sum.lock().unwrap();
//! let mut count = *lock;
//!
//! assert!(rx.is_empty(), "queue must be empty after all threads are done");
//! assert_eq!(count, THREADS * EXP_SUM);
//! # }
//! ```

#![allow(unused_unsafe)]

pub mod mpmc;
pub mod mpsc;

mod owned;
mod refcount;
mod slot;

use std::{
    alloc::{self, Layout},
    mem::ManuallyDrop,
    ptr,
    sync::atomic::{AtomicPtr, AtomicU32, AtomicU8, Ordering},
};

pub use crate::owned::OwnedQueue;
pub use crate::refcount::DropResult;

use crate::slot::Slot;

/// The maximum number of producer handles that may exist at the same time for a single
/// [`mpsc`](crate::mpsc) or [`mpmpc`](crate::mpmc) queue.
pub const MAX_PRODUCERS: usize = (1 << TAG_BITS) - NODE_SIZE + 1;
/// The maximum number of consumer handles that may exist at the same time for a single
/// [`mpmpc`](crate::mpmc) queue.
pub const MAX_CONSUMERS: usize = ((1 << TAG_BITS) - NODE_SIZE + 1) / 2;

/// The number of tag bits required to represent the index tag.
const TAG_BITS: usize = NodeSize::Small.properties().tag_bits;
/// The number of elements (slots) in each node.
const NODE_SIZE: usize = NodeSize::Small.properties().size;
/// The memory alignment required for each node to have sufficient `TAG_BITS` in each node pointer.
const NODE_ALIGN: usize = NodeSize::Small.properties().alignment();

#[repr(align(64))]
struct AtomicTagPtr<T>(tagptr::AtomicTagPtr<T, TAG_BITS>);

impl<T> AtomicTagPtr<T> {
    fn new(ptr: TagPtr<T>) -> Self {
        Self(tagptr::AtomicTagPtr::new(ptr))
    }
}

type TagPtr<T> = tagptr::TagPtr<T, TAG_BITS>;

// TODO: parametrize all queue/from_iter constructors with this enum
// TODO: alternatively, use at type level
#[allow(unused)]
enum NodeSize {
    Small,  // e.g. 64
    Medium, // e.g. 128
    Large,  // e.g. 1024
}

impl NodeSize {
    const fn properties(&self) -> NodeProperties {
        match self {
            Self::Small => NodeProperties { tag_bits: 12, size: 64 },
            Self::Medium => NodeProperties { tag_bits: 12, size: 128 },
            Self::Large => NodeProperties { tag_bits: 12, size: 1024 },
        }
    }
}

struct NodeProperties {
    tag_bits: usize,
    size: usize,
}

impl NodeProperties {
    const fn alignment(&self) -> usize {
        1 << self.tag_bits
    }
}

/// An array-node which forms the building block for the linked-list based queues.
// TODO: Node<T, const S: NodeSize = NodeSize::Small>
struct Node<T> {
    /// The array of elements (slots) and their respective state.
    slots: [Slot<T>; NODE_SIZE],
    /// The control block keeping track of the operations referencing this node.
    control: ControlBlock,
    /// The pointer to the next node in the linked-list.
    next: AtomicPtr<Node<T>>,
}

impl<T> Node<T> {
    /// The number of bits to right-shift in order to extract the observed final operations count.
    const SHIFT: u32 = 16;
    /// The bitmask to extract the current count (lower bits) of concluded operations.
    const MASK: u32 = 0xFFFF;

    const SLOT: Slot<T> = Slot::new(); // FIXME: use inline const once stable

    /// Creates a new node of uninitialized slots.
    const fn new() -> Self {
        Self {
            slots: [Self::SLOT; NODE_SIZE],
            control: ControlBlock::new(),
            next: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Creates a node with `elem` in the first slot and all remaining slots uninitialized.
    const fn with_first(elem: T) -> Self {
        let mut slots = [Self::SLOT; NODE_SIZE];
        slots[0] = Slot::with(elem);

        Self { slots, control: ControlBlock::new(), next: AtomicPtr::new(ptr::null_mut()) }
    }

    /// Creates a node with `elem` *tentatively* in the first slot and all remaining slots
    /// uninitialized.
    ///
    /// # Safety
    ///
    /// The caller must subsequently either decide to either finalize the write and discard
    /// (w/o dropping) `elem` or to discard the returned node.
    unsafe fn with_tentative_first(elem: &ManuallyDrop<T>) -> Self {
        let mut slots = [Self::SLOT; NODE_SIZE];
        slots[0] = unsafe { Slot::with_tentative(elem) };

        Self { slots, control: ControlBlock::new(), next: AtomicPtr::new(ptr::null_mut()) }
    }

    /// Allocates memory for storing a [`Node`] aligned to [`NODE_ALIGN`] and initializes it with
    /// [`new`](Node::new).
    fn aligned_alloc() -> *mut Self {
        let ptr = Self::aligned_alloc_uninit();
        unsafe {
            ptr.write(Self::new());
            ptr
        }
    }

    /// Allocates memory for storing a [`Node`] aligned to [`NODE_ALIGN`] and initializes it with
    /// [`with_first`](Node::with_first).
    fn aligned_alloc_with(elem: T) -> *mut Self {
        let ptr = Self::aligned_alloc_uninit();
        unsafe {
            ptr.write(Self::with_first(elem));
            ptr
        }
    }

    /// Allocates memory for storing a [`Node`] aligned to [`NODE_ALIGN`] and initializes it with
    /// [`with_tentative_first`](Node::with_tentative_first).
    ///
    /// # Safety
    ///
    /// See [`with_tentative_first`](Node::with_tentative_first).
    unsafe fn aligned_alloc_with_tentative(elem: &ManuallyDrop<T>) -> *mut Self {
        let ptr = Self::aligned_alloc_uninit();
        unsafe {
            ptr.write(Self::with_tentative_first(elem));
            ptr
        }
    }

    /// Allocates memory for storing a [`Node`] aligned to [`NODE_ALIGN`], leaving it uninitialized.
    fn aligned_alloc_uninit() -> *mut Self {
        let layout = Layout::new::<Self>().align_to(NODE_ALIGN).unwrap();
        let ptr: *mut Self = unsafe { alloc::alloc(layout) }.cast();
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }

        ptr
    }

    /// Iterates all slots in `node` starting from `start_idx` and checks, if each slot has been
    /// [consumed](CONSUMED) yet.
    ///
    /// If any slot is found to have **not** yet been consumed, i.e., there is either a pending
    /// produce or consume operation, the function atomically sets the [`CONTINUE`] bit in the
    /// slot's state mask and returns without checking the remaining slots.
    ///
    /// If the const generic `RECLAIM` parameter is set to `true` and the function succeeds in
    /// checking up to the last slot, the function atomically sets the appropriate bit in the node's
    /// control block and de-allocates the block, if it determines all reclamation conditions to be
    /// met.
    ///
    /// The const generic `RECLAIM` parameter should only be set to `false`, if it is certain, that
    /// the final check can not possibly succeed.
    ///
    /// # Safety
    ///
    /// - must only be called by a single initiator thread or by threads, that detect the
    ///   [`CONTINUE`] bit in their currently processed slot's state mask (with follow on index)
    /// - must never be called concurrently with other threads or repeatedly
    /// - `node` must be non-null and live
    // FIXME: RECLAIM should default to `true` (requires const generic default parameters)
    unsafe fn check_slots_and_try_reclaim<const RECLAIM: bool>(node: *mut Self, start_idx: usize) {
        // iterate all slots from `start_idx` on and check if they have been consumed
        for slot in &(*node).slots[start_idx..] {
            if !slot.is_consumed() && !slot.set_continue_bit() {
                return;
            }
        }

        // SAFETY: after all slots have been checked exactly once and were determined to be consumed
        // it is sound to set the appropriate bit and potentially reclaim the node
        unsafe { Self::set_flag_and_try_reclaim::<{ ControlBlock::DRAINED_SLOTS }, RECLAIM>(node) };
    }

    /// Increases the current count (low 16 bit) of concluded (slow-path/advance tail) push
    /// operations and sets the final count (high 16 bit), if `final_count` is not `None`.
    ///
    /// If the operation determines, that the respective push operation is the final operation
    /// to access the node it is subsequently de-allocated.
    ///
    /// # Safety
    ///
    /// The given `node` pointer must be..
    ///
    /// 1. allocated by the same allocator used to de-allocate it (currently the global allocator)
    /// 2. live & non-null
    /// 3. correctly aligned to [`NODE_ALIGN`](crate::NODE_ALIGN)
    unsafe fn count_push_and_try_reclaim(node: *mut Self, final_count: Option<u32>) {
        let mask = match final_count {
            // set the final count AND increment the current count
            Some(final_count) => (final_count << Self::SHIFT) + 1,
            // only increment the current count
            None => 1,
        };

        // SAFETY: node deref is required to be safe by fn safety invariants
        let prev_mask = unsafe { (*node).control.push_count.fetch_add(mask, Ordering::Relaxed) };
        let curr_count = (prev_mask & Self::MASK) + 1;

        // use either provided final count or the final count extracted from the loaded mask
        if curr_count == final_count.unwrap_or_else(|| prev_mask >> Self::SHIFT) {
            // SAFETY: curr_count is never zero, it can only be equal to the final count once that
            // has been set, i.e. when all counted operations are finished
            unsafe {
                Self::set_flag_and_try_reclaim::<{ ControlBlock::TAIL_ADVANCED }, true>(node)
            };
        }
    }

    /// Increases the current count (low 16 bit) of concluded (slow-path/advance head) pop
    /// operations and sets the final count (high 16 bit), if `final_count` is not `None`.
    ///
    /// If the operation determines, that the respective pop operation is the final operation
    /// to access the node it is subsequently de-allocated.
    ///
    /// # Safety
    ///
    /// The given `node` pointer must be..
    ///
    /// 1. allocated by the same allocator used to de-allocate it (currently the global allocator)
    /// 2. live & non-null
    /// 3. correctly aligned to [`NODE_ALIGN`]
    unsafe fn count_pop_and_try_reclaim(node: *mut Self, final_count: Option<u32>) {
        let mask = match final_count {
            // set the final count AND increment the current count
            Some(final_count) => (final_count << Self::SHIFT) + 1,
            // only increment the current count
            None => 1,
        };

        // SAFETY: node deref is required to be safe by fn safety invariants
        let prev_mask = unsafe { (*node).control.pop_count.fetch_add(mask, Ordering::Relaxed) };
        let curr_count = (prev_mask & Self::MASK) + 1;

        // use either provided final count or the final count extracted from the loaded mask
        if curr_count == final_count.unwrap_or_else(|| prev_mask >> Self::SHIFT) {
            unsafe {
                Self::set_flag_and_try_reclaim::<{ ControlBlock::HEAD_ADVANCED }, true>(node)
            };
        }
    }

    /// Sets the const generic `BIT` flag in the `node`'s control block.
    ///
    /// When `RECLAIM` is `true` it then de-allocates the node, if it has become reclaimable after
    /// setting the flag.
    // FIXME: RECLAIM should default to `true` (requires stable const generic default parameters)
    unsafe fn set_flag_and_try_reclaim<const BIT: u8, const RECLAIM: bool>(node: *mut Self) {
        let flags = (*node).control.reclaim_flags.fetch_add(BIT, Ordering::AcqRel);
        if RECLAIM && ControlBlock::is_reclaimable::<BIT>(flags) {
            // SAFETY: when all three bits are set there can be no further operations that may
            // access the node
            unsafe { Self::dealloc(node) };
        }
    }

    /// De-allocates the memory of `node`.
    ///
    /// # Safety
    ///
    /// The same safety requirements as for [`dealloc`](alloc::dealloc) apply.
    /// In addition, `node` must point to a node that is aligned to [`NODE_ALIGN`].
    unsafe fn dealloc(node: *mut Self) {
        let layout = Layout::new::<Self>().align_to(NODE_ALIGN).unwrap();
        alloc::dealloc(node.cast(), layout);
    }
}

/// The control block of a node keeping track of finished push & pop operations.
struct ControlBlock {
    /// The bitmask storing the observed final count of push operations (high 16 bit) as well as the
    /// current count (low 16 bit).
    push_count: AtomicU32,
    /// The bitmask storing the observed final count of pop operations (high 16 bit) as well as the
    /// current count (low 16 bit).
    pop_count: AtomicU32,
    //// The bitmask storing the flags indicating the progress of reclamation steps and conditions.
    reclaim_flags: AtomicU8,
}

impl ControlBlock {
    /// The bit indicating that all slots have been checked exactly once and were determined to have
    /// been consumed.
    const DRAINED_SLOTS: u8 = 0b001;
    /// The bit indicating that all operations attempting to advance the queue's tail node from a
    /// specific node have concluded.
    const TAIL_ADVANCED: u8 = 0b010;
    /// The bit indicating that all operations attempting to advance the queue's head node from a
    /// specific node have concluded.
    const HEAD_ADVANCED: u8 = 0b100;

    /// Creates a new control block.
    const fn new() -> Self {
        Self {
            push_count: AtomicU32::new(0),
            pop_count: AtomicU32::new(0),
            reclaim_flags: AtomicU8::new(0),
        }
    }

    /// Returns the bit mask returned by a `fetch_add` of `bit` (i.e. the previous value),
    /// indicating that a node can be reclaimed.
    const fn reclaimable_mask(bit: u8) -> u8 {
        match bit {
            Self::DRAINED_SLOTS => Self::TAIL_ADVANCED | Self::HEAD_ADVANCED,
            Self::TAIL_ADVANCED => Self::DRAINED_SLOTS | Self::HEAD_ADVANCED,
            Self::HEAD_ADVANCED => Self::DRAINED_SLOTS | Self::TAIL_ADVANCED,
            _ => u8::MAX, // FIXME: const panic
        }
    }

    /// Returns `true` if a node can be reclaimed after a `fetch_add` of `BIT` on the node's reclaim
    /// flags has previously returned `flags`.
    fn is_reclaimable<const BIT: u8>(flags: u8) -> bool {
        flags == Self::reclaimable_mask(BIT)
    }

    fn mark_tail_advanced(&mut self) {
        self.reclaim_flags.store(Self::TAIL_ADVANCED, Ordering::Relaxed)
    }
}

/// A cursor to a [`Slot`] in a linked-list (queue) node.
struct Cursor<T> {
    ptr: *mut Node<T>,
    idx: usize,
}

impl<T> From<(*mut Node<T>, usize)> for Cursor<T> {
    fn from((ptr, idx): (*mut Node<T>, usize)) -> Self {
        Cursor { ptr, idx }
    }
}

impl<T> Clone for Cursor<T> {
    fn clone(&self) -> Self {
        Self { ptr: self.ptr, idx: self.idx }
    }
}

impl<T> Copy for Cursor<T> {}

impl<T> Cursor<T> {
    /// Iterates over the pointers to each slot as if all nodes were laid out continously in memory.
    unsafe fn next_unchecked(&mut self, end: &Self, prev: &mut Option<*mut Node<T>>) -> *mut T {
        if self.ptr.is_null() || (self.ptr == end.ptr && self.idx >= end.idx) {
            return ptr::null_mut();
        }

        // SAFETY: as long as self and end are valid cursors, `self.ptr` can be de-referenced
        let elem = unsafe { (*self.ptr).slots[self.idx].as_mut_ptr() };

        if self.idx < NODE_SIZE - 1 {
            self.idx += 1;
            elem
        } else {
            let curr = self.ptr;
            let next = (*curr).next.load(Ordering::Relaxed);

            *self = Cursor { ptr: next, idx: 0 };
            *prev = Some(curr);

            elem
        }
    }
}

#[inline(always)]
fn cas_atomic_tag_ptr_loop<T>(
    ptr: &AtomicTagPtr<Node<T>>,
    mut current: TagPtr<Node<T>>,
    new: TagPtr<Node<T>>,
    old: *mut Node<T>,
) -> Option<u32> {
    const REL_RLX_CAS: (Ordering, Ordering) = (Ordering::Release, Ordering::Relaxed);
    // loop & try to CAS the ptr until the CAS succeeds
    while let Err(read) = ptr.0.compare_exchange(current, new, REL_RLX_CAS) {
        // the CAS failed due to a competing CAS or FAA from another thread, but the loaded value
        // shows, that the pointer itself has been changed (instead of only the tag), so another
        // thread must have been successfull in exchanging the pointer
        if read.decompose_ptr() != old {
            return None;
        }

        // update the expected value and repeat
        current = read;
    }

    // since tag values can not exceed the tag bit limit, this cast will never truncate any bits
    Some((current.decompose_tag() - NODE_SIZE) as u32)
}

unsafe fn try_advance_tail<T>(
    ptr: &AtomicTagPtr<Node<T>>,
    ptr_cached: &AtomicPtr<Node<T>>,
    elem: &ManuallyDrop<T>,
    tail: *mut Node<T>,
) -> Result<(), NotInserted> {
    // read an up-to-date snapshot of the tail pointer
    let current = ptr.0.load(Ordering::Relaxed);
    // check, if the tail as already been updated by another thread
    if tail != current.decompose_ptr() {
        Node::count_push_and_try_reclaim(tail, None);
        return Err(NotInserted);
    }

    // read the current tail node's next pointer
    let next = (*tail).next.load(Ordering::Relaxed);
    if next.is_null() {
        // the next pointer is still `null`, attempt to append a newly allocated node
        let node = Node::aligned_alloc_with_tentative(elem);
        // try to exchange (CAS) the tail node's next pointer
        let (res, new_tail) =
            match (*tail).next.compare_exchange(next, node, Ordering::Release, Ordering::Relaxed) {
                // the node allocated by this thread was successfully appended
                Ok(_) => (Ok(()), node),
                // another thread has instead appended a different node
                Err(read) => (Err(NotInserted), read),
            };

        // update the queue's tail pointer
        let final_count = cas_atomic_tag_ptr_loop(ptr, current, TagPtr::compose(new_tail, 1), tail);

        // .. then, help update the queue's cached tail pointer
        let _ = ptr_cached.compare_exchange(tail, new_tail, Ordering::Release, Ordering::Relaxed);

        // mark the operation as concluded, since it does not access the node any more
        Node::count_push_and_try_reclaim(tail, final_count);

        // if the node was not appended, de-allocate it again
        if res.is_err() {
            Node::dealloc(node);
        }

        res
    } else {
        // another thread has already appended a next node, help updating the queue's tail and
        // cached tail pointer, the cached may lag behind since it is updated later, but this
        // can be detected
        let final_count = cas_atomic_tag_ptr_loop(ptr, current, TagPtr::compose(next, 1), tail);

        // ..after the tail has been updated, help updating the cached tail
        let _ = ptr_cached.compare_exchange(tail, next, Ordering::Release, Ordering::Relaxed);

        // mark the operation as concluded, since it does not access the node any more
        Node::count_push_and_try_reclaim(tail, final_count);

        Err(NotInserted)
    }
}

/// A type indicating that a append-tail operation has failed to insert the desired element.
struct NotInserted;
/// A type indicating that a advance-head operation has failed due to there not being a next node.
struct NoNextNode;
