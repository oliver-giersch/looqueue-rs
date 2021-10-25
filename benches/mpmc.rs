#![feature(test)]

extern crate test;

use test::Bencher;

use loo::mpmc;

#[bench]
fn push_small(b: &mut Bencher) {
    let (tx, _) = mpmc::queue();
    b.iter(|| {
        tx.push_back(0usize);
    });
}

#[bench]
fn pop_small(b: &mut Bencher) {
    let (_, rx) = mpmc::from_iter(0..1000usize);
    b.iter(|| {
        let _res = test::black_box(rx.pop_front());
    });
}

#[bench]
fn send_small(b: &mut Bencher) {
    let (tx, rx) = mpmc::queue::<usize>();
    b.iter(|| {
        tx.push_back(0);
        let _ = rx.pop_front();
    });
}

#[bench]
fn send_medium(b: &mut Bencher) {
    let (tx, rx) = mpmc::queue();
    b.iter(|| {
        tx.push_back([0usize; 64]);
        let _ = rx.pop_front().unwrap();
    });
}

#[bench]
fn send_medium_boxed(b: &mut Bencher) {
    let (tx, rx) = mpmc::queue();
    b.iter(|| {
        tx.push_back(Box::new([0usize; 64]));
        let _ = *rx.pop_front().unwrap();
    });
}
