#![allow(unused_unsafe)]

pub mod mpmc;
pub mod mpsc;

mod owned;
mod refcount;
mod slot;

use std::{
    alloc::{self, Layout},
    mem::ManuallyDrop,
    ptr::{self},
    sync::atomic::{AtomicPtr, AtomicU32, AtomicU8, Ordering},
};

pub use crate::owned::OwnedQueue;

use crate::slot::Slot;

/// The maximum number of producer handles that max exist at the same time for a single
/// [`mpsc`][crate::mpsc] or [`mpmpc`][crate::mpmc] queue.
pub const MAX_PRODUCERS: usize = (1 << TAG_BITS) - NODE_SIZE + 1;
/// The maximum number of consumer handles that max exist at the same time for a single
/// [`mpmpc`][crate::mpmc] queue.
pub const MAX_CONSUMERS: usize = ((1 << TAG_BITS) - NODE_SIZE + 1) / 2;

/// The number of tag bits required to represent the index tag.
const TAG_BITS: usize = 10;
/// The number of elements (slots) in each node.
const NODE_SIZE: usize = 64;
/// The memory alignment required for each node to have sufficient `TAG_BITS` in each node pointer.
const NODE_ALIGN: usize = 1 << TAG_BITS;

type AtomicTagPtr<T> = tagptr::AtomicTagPtr<T, TAG_BITS>;
type TagPtr<T> = tagptr::TagPtr<T, TAG_BITS>;

/// An array-node which forms the building block for the linked-list based queues.
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

    const SLOT: Slot<T> = Slot::new();

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
    /// [`new`][Node::new].
    fn alloc() -> *mut Self {
        let ptr = Self::aligned_alloc_uninit();
        unsafe {
            ptr.write(Self::new());
            ptr
        }
    }

    /// Allocates memory for storing a [`Node`] aligned to [`NODE_ALIGN`] and initializes it with
    /// [`with_first`][Node::with_first].
    fn alloc_with(elem: T) -> *mut Self {
        let ptr = Self::aligned_alloc_uninit();
        unsafe {
            ptr.write(Self::with_first(elem));
            ptr
        }
    }

    unsafe fn alloc_with_tentative(elem: &ManuallyDrop<T>) -> *mut Self {
        let ptr = Self::aligned_alloc_uninit();
        unsafe {
            ptr.write(Self::with_tentative_first(elem));
            ptr
        }
    }

    fn aligned_alloc_uninit() -> *mut Self {
        let layout = Layout::new::<Self>().align_to(NODE_ALIGN).unwrap();
        let ptr = unsafe { alloc::alloc(layout) } as *mut Self;
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }

        ptr
    }

    unsafe fn check_slots_and_try_reclaim<const RECLAIM: bool>(node: *mut Self, start_idx: usize) {
        // iterate all slots from `start_idx` on and check if they have been consumed
        for slot in &(*node).slots[start_idx..] {
            if !slot.is_consumed() && !slot.set_resume_bit() {
                return;
            }
        }

        if RECLAIM {
            unsafe { Self::set_flag_and_try_reclaim(node, ControlBlock::DRAINED_SLOTS) };
        }
    }

    unsafe fn count_push_and_try_reclaim(node: *mut Self, final_count: Option<u32>) {
        let add = match final_count {
            Some(final_count) => (final_count << Self::SHIFT) + 1,
            None => 1,
        };

        let prev_mask = unsafe { (*node).control.push_count.fetch_add(add, Ordering::Relaxed) };
        let curr_count = (prev_mask & Self::MASK) + 1;

        if curr_count == final_count.unwrap_or(0) {
            unsafe { Self::set_flag_and_try_reclaim(node, ControlBlock::TAIL_ADVANCED) };
        }
    }

    unsafe fn count_pop_and_try_reclaim(node: *mut Self, final_count: Option<u32>) {
        let add = match final_count {
            Some(final_count) => (final_count << Self::SHIFT) + 1,
            None => 1,
        };

        let prev_mask = unsafe { (*node).control.pop_count.fetch_add(add, Ordering::Relaxed) };
        let curr_count = (prev_mask & Self::MASK) + 1;

        if curr_count == final_count.unwrap_or(0) {
            unsafe { Self::set_flag_and_try_reclaim(node, ControlBlock::HEAD_ADVANCED) };
        }
    }

    unsafe fn set_flag_and_try_reclaim(node: *mut Self, flag: u8) {
        let flags = (*node).control.reclaim_flags.fetch_add(flag, Ordering::AcqRel);
        if flags == ControlBlock::RECLAIMABLE {
            Self::dealloc(node);
        }
    }

    /// De-allocates the memory of `node`.
    ///
    /// # Safety
    ///
    /// The same safety requirements as for [`dealloc`][alloc::dealloc] apply.
    /// In addition, `node` must point to a node that is aligned to [`NODE_ALIGN`][NODE_ALIGN].
    unsafe fn dealloc(node: *mut Self) {
        let layout = Layout::new::<Self>().align_to(NODE_ALIGN).unwrap();
        alloc::dealloc(node as *mut u8, layout);
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
    const DRAINED_SLOTS: u8 = 0b001;
    const TAIL_ADVANCED: u8 = 0b010;
    const HEAD_ADVANCED: u8 = 0b100;
    const RECLAIMABLE: u8 = Self::DRAINED_SLOTS | Self::TAIL_ADVANCED | Self::HEAD_ADVANCED;

    const fn new() -> Self {
        Self {
            push_count: AtomicU32::new(0),
            pop_count: AtomicU32::new(0),
            reclaim_flags: AtomicU8::new(0),
        }
    }
}

/// A cursor to a [`Slot`] in a linked-list (queue) node.
struct Cursor<T> {
    ptr: *mut Node<T>,
    idx: usize,
}

impl<T> Clone for Cursor<T> {
    fn clone(&self) -> Self {
        Self { ptr: self.ptr, idx: self.idx }
    }
}

impl<T> Copy for Cursor<T> {}

#[inline(always)]
fn cas_atomic_tag_ptr_loop<T>(
    ptr: &AtomicTagPtr<Node<T>>,
    mut current: TagPtr<Node<T>>,
    new: TagPtr<Node<T>>,
    old: *mut Node<T>,
) -> Option<u32> {
    const REL_RLX_CAS: (Ordering, Ordering) = (Ordering::Release, Ordering::Relaxed);
    while let Err(read) = ptr.compare_exchange(current, new, REL_RLX_CAS) {
        if read.decompose_ptr() != old {
            return None;
        }

        current = read
    }

    Some((current.decompose_tag() - NODE_SIZE) as u32)
}

unsafe fn try_advance_tail<T>(
    ptr: &AtomicTagPtr<Node<T>>,
    ptr_cached: &AtomicPtr<Node<T>>,
    elem: &ManuallyDrop<T>,
    tail: *mut Node<T>,
) -> Result<(), NotInserted> {
    // read an up-to-date snapshot of the tail pointer
    let current = ptr.load(Ordering::Relaxed);
    // check, if the tail as already been updated by another thread
    if tail != current.decompose_ptr() {
        Node::count_push_and_try_reclaim(tail, None);
        return Err(NotInserted);
    }

    // read the current tail node's next pointer
    let next = (*tail).next.load(Ordering::Relaxed);
    if next.is_null() {
        // the next pointer is still `null`, attempt to append a newly allocated node
        let node = Node::alloc_with_tentative(elem);
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
