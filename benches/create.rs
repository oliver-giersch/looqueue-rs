#![feature(test)]

extern crate test;

use test::Bencher;

use loo::mpsc;

#[bench]
fn create_1_tiny(b: &mut Bencher) {
    b.iter(|| {
        test::black_box(&mpsc::queue::<usize>());
    });
}

#[bench]
fn create_1_small(b: &mut Bencher) {
    b.iter(|| {
        test::black_box(&mpsc::queue::<[usize; 8]>());
    });
}

#[bench]
fn create_1_medium(b: &mut Bencher) {
    b.iter(|| {
        test::black_box(&mpsc::queue::<[usize; 64]>());
    });
}
