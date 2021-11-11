use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};

use loo::mpmc;

fn create(c: &mut Criterion) {
    c.bench_function("create tiny", |b| {
        b.iter(|| {
            black_box(&mpmc::queue::<usize>());
        });
    });

    c.bench_function("create small", |b| {
        b.iter(|| {
            black_box(&mpmc::queue::<[usize; 8]>());
        });
    });

    c.bench_function("create medium", |b| {
        b.iter(|| {
            black_box(&mpmc::queue::<[usize; 64]>());
        });
    });
}

fn push_pop_small(c: &mut Criterion) {
    c.bench_function("push small", |b| {
        b.iter_batched_ref(
            || {
                let (tx, _) = mpmc::queue();
                tx
            },
            |tx| tx.push_back(0),
            BatchSize::SmallInput,
        )
    });

    c.bench_function("pop small", |b| {
        b.iter_batched_ref(
            || {
                let (_, rx) = mpmc::from_iter(0..1);
                rx
            },
            |rx| std::mem::drop(rx.pop_front()),
            BatchSize::SmallInput,
        );
    });
}
/*
#[bench]
fn push_small(b: &mut Bencher) {
    let (tx, _) = mpmc::queue();
    b.iter(|| {
        tx.push_back(0);
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
}*/

criterion_group!(mpmc, create, push_pop_small);
criterion_main!(mpmc);
