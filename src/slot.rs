use std::{
    cell::UnsafeCell,
    mem::{ManuallyDrop, MaybeUninit},
    ptr,
    sync::atomic::{AtomicU8, Ordering},
};

/// A slot containing an element.
pub(crate) struct Slot<T> {
    /// The element stored in the slot, the initialization state being defined the slot's state mask.
    inner: UnsafeCell<MaybeUninit<T>>,
    /// The flags indicating the slot's initialization state.
    state: AtomicU8,
}

/// The state bits of a completely uninitialized slot.
const UNINIT: u8 = 0;

/// The state bit indicating a producer/consumer detecting it should call
/// `Node::check_slots_and_try_reclaim` (resume it) from the next slot on.
const CONTINUE_CHECK: u8 = 0b0001;
/// The state bit set by a producer AFTER writing an element into the slot, marking it as ready.
const PRODUCER: u8 = 0b0010;
/// The state bit set by a consumer AFTER having invalidated or consumed the slot.
const CONSUMED_OR_INVALIDATED: u8 = 0b0100;
/// The state bit set by a consumer BEFORE attempting to invalidate the slot.
const NO_PRODUCER_YET: u8 = 0b1000;

/// The bit mask indicating a slot has been successfully consumed.
const CONSUMED: u8 = PRODUCER | CONSUMED_OR_INVALIDATED;

impl<T> Slot<T> {
    /// Creates a new uninitialized slot.
    pub(crate) const fn new() -> Self {
        Self { inner: UnsafeCell::new(MaybeUninit::uninit()), state: AtomicU8::new(UNINIT) }
    }

    /// Creates a new slot initialized with `elem`.
    pub(crate) const fn with(elem: T) -> Self {
        Self { inner: UnsafeCell::new(MaybeUninit::new(elem)), state: AtomicU8::new(PRODUCER) }
    }

    /// Creates a new slot tentatively initialized with `elem`.
    ///
    /// # Safety
    ///
    /// The write must subsequently be either affirmed or denied, in which case the stored element
    /// must not be accessed any further.
    pub(crate) unsafe fn with_tentative(elem: &ManuallyDrop<T>) -> Self {
        Self {
            inner: UnsafeCell::new(MaybeUninit::new(ptr::read(&**elem))),
            state: AtomicU8::new(PRODUCER),
        }
    }

    /// Returns a raw pointer to the slot's element.
    pub(crate) fn as_mut_ptr(&self) -> *mut T {
        unsafe { (*self.inner.get()).as_mut_ptr() }
    }

    /// Returns `true` if the slot is consumed.
    pub(crate) fn is_consumed(&self) -> bool {
        self.state.load(Ordering::Acquire) & CONSUMED == CONSUMED
    }

    /// Atomically sets the [`CONTINUE_CHECK`] bit in the slots state mask.
    ///
    /// # Safety
    ///
    /// Must only be called during the *check slots* procedure and after determining, the slot has
    /// not yet been consumed.
    pub(crate) unsafe fn set_continue_bit(&self) -> bool {
        self.state.fetch_add(CONTINUE_CHECK, Ordering::Relaxed) & CONSUMED == CONSUMED
    }

    pub(crate) unsafe fn try_consume(&self) -> ConsumeResult<T> {
        const CONTINUE_OR_PRODUCER: u8 = CONTINUE_CHECK | PRODUCER;

        // loop a bounded number of steps in order to allow the corresponding producer to complete
        // its corresponding call to `write_tentative`
        for _ in 0..16 {
            // (slot:x) this acquire load syncs-with the release FAA (slot:y)
            if self.state.load(Ordering::Acquire) & PRODUCER == PRODUCER {
                // SAFETY: Since the PRODUCER bit is already set, the slot can be safely read (no
                // data race is possible) and the CONSUMED_OR_INVALIDATED bit can be set right away,
                // as no 2-step invalidation is necessary
                let elem = unsafe { self.read_volatile() };
                return match self.state.fetch_add(CONSUMED_OR_INVALIDATED, Ordering::Release) {
                    // the expected/likely case
                    PRODUCER => ConsumeResult::Success { elem, resume_check: false },
                    // RESUME can only be set if there are multiple consumers
                    CONTINUE_OR_PRODUCER => ConsumeResult::Success { elem, resume_check: true },
                    // SAFETY: no other combination of state bits is possible at this point
                    _ => unsafe { std::hint::unreachable_unchecked() },
                };
            }
        }

        // after an unsuccessful bounded wait, try one final time or invalidate (abandon) the slot
        // if this fails as well due to the producer still not having finished its operation
        unsafe { self.try_consume_unlikely() }
    }

    /// Consumes the element from this slot without performing any checks.
    ///
    /// # Safety
    ///
    /// The slot must have been written to before.
    pub(crate) unsafe fn consume_unsync_unchecked(&mut self) -> T {
        self.state.store(CONSUMED, Ordering::Relaxed);
        (*self.inner.get()).as_ptr().read()
    }

    /// Writes `elem` into the slot (used only by `OwnedQueue`).
    ///
    /// Should only be called once, since already written elements will be silently overwritten and
    /// leaked.
    pub(crate) fn write_unsync(&mut self, elem: T) {
        *self = Self::with(elem);
    }

    pub(crate) unsafe fn write_tentative(&self, elem: &ManuallyDrop<T>) -> WriteResult {
        // if a slot has already been visited and marked for abandonment AND halted at in an attempt
        // to check if all slots have yet been consumed, a producer must abandon that slot and
        // resume the slot check procedure
        const PRODUCER_RESUMES: u8 = NO_PRODUCER_YET | CONSUMED_OR_INVALIDATED | CONTINUE_CHECK;

        // write the element's bits tentatively into the slot, i.e. the write may yet be revoked, in
        // which case the source must remain valid
        self.write_volative(elem);
        // after the slot is initialized, set the WRITER bit in the slot's state field and assess,
        // if any other bits had been set by other (consumer) threads
        match self.state.fetch_add(PRODUCER, Ordering::Release) {
            UNINIT | CONTINUE_CHECK => WriteResult::Success,
            PRODUCER_RESUMES => WriteResult::Abandon { resume_check: true },
            _ => WriteResult::Abandon { resume_check: false },
        }
    }

    #[cold]
    unsafe fn try_consume_unlikely(&self) -> ConsumeResult<T> {
        // FIXME: could be replaced with inline const
        const CONSUMER_RESUMES_A: u8 = PRODUCER | CONTINUE_CHECK;
        const CONSUMER_RESUMES_B: u8 = NO_PRODUCER_YET | CONSUMER_RESUMES_A;

        // set the NO_PRODUCER_YET bit, which leads to all subsequent write attempts to fail, but
        // check, if the PRODUCER bit has been set before by now
        let (res, mut resume_check) = match self.state.fetch_add(NO_PRODUCER_YET, Ordering::Acquire)
        {
            // the slot has now been initialized, so the slot can now be consumed
            PRODUCER => (Some(self.read_volatile()), false),
            // the slot has now been initialized, but the slot check must be resumed
            CONSUMER_RESUMES_A => (Some(self.read_volatile()), true),
            // the slot has still not been initialized, so it must truly be abandoned now
            _ => (None, false),
        };

        // set the CONSUMED_OR_INVALIDATED bit to mark the consume operation as completed; whether
        // a write has occurred or not is no longer relevant at this point
        let state = self.state.fetch_add(CONSUMED_OR_INVALIDATED, Ordering::Release);
        // if the CONTINUE_CHECK bit was not previously set but is now, the slot check must now be resumed
        if !resume_check && state == CONSUMER_RESUMES_B {
            resume_check = true;
        }

        match res {
            Some(elem) => ConsumeResult::Success { elem, resume_check },
            None => ConsumeResult::Abandon { resume_check },
        }
    }

    /// Reads the bytes of this slot without performing any checks.
    unsafe fn read_volatile(&self) -> T {
        unsafe { (*self.inner.get()).as_ptr().read_volatile() }
    }

    /// Writes the bytes of `elem` into this slit without performing any checks.
    unsafe fn write_volative(&self, elem: &ManuallyDrop<T>) {
        unsafe { (*self.inner.get()).as_mut_ptr().write_volatile(ptr::read(&**elem)) };
    }
}

/// An (unsafe) transparent wrapper for slot that automatically drops the
/// contained type.
///
/// N.B: There is deliberately no (safe) way to create a `DropSlot` outside of this module
#[repr(transparent)]
pub(crate) struct DropSlot<T>(Slot<T>);

impl<T> Drop for DropSlot<T> {
    fn drop(&mut self) {
        // SAFETY: this in fact NOT safe in general, because it neither checks if the wrapped slot
        // has ever been initialized, nor if it has not yet been consumed, but `drop` can not be an
        // `unsafe` function; yet:
        // - the type can not be safely constructed at all, only using unsafe pointer casts
        // - the type is only used internally and only in one place (`OwnedQueue::drop`)
        // - the drop code carefully identifies the slots that are safe to drop and casts only
        //   those into `DropSlot`s
        unsafe { self.0.as_mut_ptr().drop_in_place() };
    }
}

/// The result of a consume operation on a slot.
pub(crate) enum ConsumeResult<T> {
    /// The slot was successfully consumed and the element can be returned.
    Success { resume_check: bool, elem: T },
    /// The slot was not consumed because it was not yet initialized and has to be abandoned.
    Abandon { resume_check: bool },
}

/// The result of a write (produce) operation on a slot.
pub(crate) enum WriteResult {
    /// The operation was successful and the slot was initialized.
    Success,
    /// The operation failed, because a concurrent consumer had previously invalidated the slot,
    /// marking it for abandonment.
    Abandon { resume_check: bool },
}
