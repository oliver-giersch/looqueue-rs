An implementation of a thread-safe, lock-free and scalable FIFO queue algorithm.
Two variants of the algorithm are provided:

1. MPMC (Multi-Producer/Multi-Consumer)
2. MPSC (Multi-Producer/Single-Consumer)

The API is similar to that of [`std::sync::mpsc`] with separate types for producer
and consumer handles.
There is also [`OwnedQueue`], which is a single-threaded queue with an identical
internal structure for cheap conversion from/to either of the thread-safe variants
or initialization from iterators.

The MPMC queue implementation is based on the algorithm in [\[1\]][1] and the MPSC
variant is derived from the MPMC algorithm described therein but optimized for the
additional constraint of at most a single consumer.

One key aspect of the implemented algorithm is, that it does not rely on a dedicated
memory reclamation mechanism but comes with its own memory reclamation strategy,
that is lock-free, deterministic (rather than lazy/deferred) like reference counting
and has very little performance impact.
It does come with a limitation, however, in that there is a limit on how many
producers and consumers can safely interact with the same queue.
These limits are enforced by the implementation and they are encoded in the
two constants [`MAX_PRODUCERS`] and [`MAX_CONSUMERS`].
When the number of live producer/consumer handles would exceed these safe limits,
further attempts to [`clone`](Clone::clone) handles will panic!

In the current implementation, there can be **961** producer and **480** consumer
handles.

[1]: https://ieeexplore.ieee.org/document/9490347

# Examples

```rust
# #[cfg(not(miri))]
# {
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

const N: usize = 100;
const THREADS: usize = 4;
const EXP_SUM: usize = (N * (N - 1)) / 2; // Gauss sum [1, 99]

let (tx, rx) = loo::mpmc::queue();

let barrier = Arc::new(Barrier::new(THREADS * 2));
let total_sum = Arc::new(Mutex::new(0));

let mut handles = Vec::with_capacity(THREADS * 2);

// spawn 4 producer threads, each pushing 100 elements in FIFO order
for _ in 0..THREADS {
    let tx = tx.clone();
    let barrier = Arc::clone(&barrier);

    handles.push(thread::spawn(move || {
        barrier.wait();

        for i in 0..100 {
            tx.push_back(i);
        }
    }));
}

// spawn 4 consumer threads, each popping exactly 100 elements from the queue
// and summing them up
for _ in 0..THREADS {
    let rx = rx.clone();
    let barrier = Arc::clone(&barrier);
    let total_sum = Arc::clone(&total_sum);
    handles.push(thread::spawn(move || {
        let mut count = 0;
        let mut sum = 0;

        barrier.wait();

        while count < N { // retrieve exactly N elements
            if let Some(e) = rx.pop_front() {
                count += 1;
                sum += e;
            }
        }

        // add this thread's result to the total sum
        let mut lock = total_sum.lock().unwrap();
        *lock += sum;
    }));
}

for handle in handles {
    handle.join().unwrap();
}

let lock = total_sum.lock().unwrap();
let mut count = *lock;

assert!(rx.is_empty(), "queue must be empty after all threads are done");
assert_eq!(count, THREADS * EXP_SUM);
# }
```
