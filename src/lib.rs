#![no_std]
#![cfg_attr(target_pointer_width = "64", doc = include_str!("../CRATE_DOCS.md"))]
#![cfg_attr(
    not(target_pointer_width = "64"),
    doc = "**WARNING**: The algorithm implemented in this crate is only valid for 64-bit CPU architectures. On other architectures, this crate exports nothing at all."
)]
#![allow(unused_unsafe)]

extern crate alloc;
#[cfg(test)]
extern crate std;

// since 64 bit CPU arch is required, nothing is exported on other architectures
cfg_if::cfg_if! {
    if #[cfg(target_pointer_width = "64")] {
        mod facade;

        pub use facade::{mpsc, mpmc, owned::OwnedQueue, refcount::DropResult, MAX_CONSUMERS, MAX_PRODUCERS};
    }
}
