use std::sync::atomic::{AtomicU32, Ordering};

/// Atomic reference counters for consumers and producers.
pub(crate) struct RefCounts {
    consumers: AtomicU32,
    producers: AtomicU32,
}

impl Default for RefCounts {
    fn default() -> Self {
        Self { consumers: AtomicU32::new(1), producers: AtomicU32::new(1) }
    }
}

impl RefCounts {
    pub(crate) fn consumer_count(&self) -> usize {
        self.consumers.load(Ordering::Acquire) as usize
    }

    pub(crate) fn increase_consumer_count(&self, max_consumers: usize) {
        let prev = self.consumers.fetch_add(1, Ordering::AcqRel);
        if prev as usize >= max_consumers {
            self.consumers.fetch_sub(1, Ordering::AcqRel);
            panic!("...");
        }
    }

    pub(crate) fn decrease_consumer_count(&self) -> bool {
        let prev = self.consumers.fetch_sub(1, Ordering::AcqRel);
        prev == 1 && (self.producers.load(Ordering::Acquire) == 0)
    }

    pub(crate) fn producer_count(&self) -> usize {
        self.producers.load(Ordering::Acquire) as usize
    }

    pub(crate) fn increase_producer_count(&self, max_producers: usize) {
        let prev = self.producers.fetch_add(1, Ordering::AcqRel);
        if prev as usize >= max_producers {
            self.producers.fetch_sub(1, Ordering::AcqRel);
            panic!("...");
        }
    }

    pub(crate) fn decrease_producer_count(&self) -> bool {
        let prev = self.producers.fetch_sub(1, Ordering::AcqRel);
        prev == 1 && (self.consumers.load(Ordering::Acquire) == 0)
    }
}
