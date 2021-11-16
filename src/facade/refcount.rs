use core::sync::atomic::{AtomicU32, Ordering};

/// A result indicating how many handles to a queue remain after dropping a producer or consumer.
///
/// The `LastOfKind` result indicates that, e.g., the dropped producer handle was the final producer
/// but there are still one or more remaining consumer handles.
/// The `LastOfAny` result indicates that the dropped handle was the final one and that the queue
/// has been de-allocated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum DropResult {
    LastOfKind,
    LastOfAny,
}

/// Atomic reference counters for consumers and producers.
pub(crate) struct RefCounts {
    /// The consumer thread count.
    consumers: AtomicU32,
    /// The producer thread count.
    producers: AtomicU32,
}

impl Default for RefCounts {
    fn default() -> Self {
        Self { consumers: AtomicU32::new(1), producers: AtomicU32::new(1) }
    }
}

impl RefCounts {
    /// Returns the current number of consumer threads.
    pub(crate) fn consumer_count(&self) -> usize {
        self.consumers.load(Ordering::Acquire) as usize
    }

    /// Increases the consumer thread counter.
    ///
    /// # Panics
    ///
    /// Panics, if the new count would exceed `max_consumers`.
    pub(crate) fn increase_consumer_count(&self, max_consumers: usize) {
        let prev = self.consumers.fetch_add(1, Ordering::Relaxed);
        if prev as usize >= max_consumers {
            self.consumers.fetch_sub(1, Ordering::Relaxed);
            panic!(
                "attempted to increase consumer count beyond safe limit of {} threads",
                max_consumers
            );
        }
    }

    /// Decreases the consumer thread counter and returns a result indicating how many producers
    /// and consumers still exist.
    ///
    /// If `None` is returned, there are still live producers **and** consumers.
    pub(crate) fn decrease_consumer_count(&self) -> Option<DropResult> {
        let prev = self.consumers.fetch_sub(1, Ordering::AcqRel);
        let is_last_consumer = prev == 1;
        is_last_consumer.then(|| match self.producers.load(Ordering::Acquire) == 0 {
            true => DropResult::LastOfAny,
            false => DropResult::LastOfKind,
        })
    }

    /// Returns the current number of producer threads.
    pub(crate) fn producer_count(&self) -> usize {
        self.producers.load(Ordering::Acquire) as usize
    }

    /// Increases the producer thread counter.
    ///
    /// # Panics
    ///
    /// Panics, if the new count would exceed `max_producers`.
    pub(crate) fn increase_producer_count(&self, max_producers: usize) {
        let prev = self.producers.fetch_add(1, Ordering::Relaxed);
        if prev as usize >= max_producers {
            self.producers.fetch_sub(1, Ordering::Relaxed);
            panic!(
                "attempted to increase producer count beyond safe limit of {} threads",
                max_producers
            );
        }
    }

    /// Decreases the producer thread counter and returns a result indicating how many producers
    /// and consumers still exist.
    ///
    /// If `None` is returned, there are still live producers **and** consumers.
    pub(crate) fn decrease_producer_count(&self) -> Option<DropResult> {
        let prev = self.producers.fetch_sub(1, Ordering::AcqRel);
        let is_last_producer = prev == 1;
        is_last_producer.then(|| match self.consumers.load(Ordering::Acquire) == 0 {
            true => DropResult::LastOfAny,
            false => DropResult::LastOfKind,
        })
    }
}
