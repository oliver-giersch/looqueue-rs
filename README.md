# looqueue-rs

A pure-rust, fully safe implementation of the lock-free FIFO queue algorithm described in [1].
The paper describes the algorithm for a multi-producer, multi-consumer queue with a custom memory reclamation mechanism and this crate exports an implementation of that algorithm in the `mpmc` module as well as an adaptation for single consumers in the `mpsc` module.

[1] _O. Giersch and J. Nolte, "Fast and Portable Concurrent FIFO Queues With Deterministic Memory Reclamation", in IEEE Transactions on Parallel and Distributed Systems, vol. 33, no. 3, pp. 604-616, 1 March 2022, doi: 10.1109/TPDS.2021.3097901_

# Usage

Add the following to your `Cargo.toml`

```toml
[dependencies]
loo = 0.1.0
```

# Example

The API of this crate is similar to that of `std::mpsc`.
Creating new queue instances is achieved by calling either `mpmp::queue()` or `mpsc::queue()`, which return reference-counted `(producer, consumer)` handle tuples.
These handles can be cloned (with the exception of `mpsc::Consumer`) and send to threads as required.

```rust
use std::thread;

use loo::mpmc;

let (tx, rx) = mpmc::queue();

let handle = thread::spawn(move || {
    tx.push_back(1);
    tx.push_back(2);
    tx.push_back(3);
});

handle.join().unwrap();

assert_eq!(rx.pop_front(), Some(1));
assert_eq!(rx.pop_front(), Some(2));
assert_eq!(rx.pop_front(), Some(3));
assert_eq!(rx.pop_front(), None);
```

# License
