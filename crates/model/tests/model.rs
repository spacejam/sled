use model::{linearizable, model};

#[test]
fn test_model() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    model! {
        Model => let m = AtomicUsize::new(0),
        Implementation => let mut i: usize = 0,
        Add(usize)(v in 0usize..4) => {
            let expected = m.fetch_add(v, Ordering::SeqCst) + v;
            i += v;
            assert_eq!(expected, i);
        },
        Set(usize)(v in 0usize..4) => {
            m.store(v, Ordering::SeqCst);
            i = v;
        },
        Eq(usize)(v in 0usize..4) => {
            let expected = m.load(Ordering::SeqCst) == v;
            let actual = i == v;
            assert_eq!(expected, actual);
        },
        Cas((usize, usize))((old, new) in (0usize..4, 0usize..4)) => {
            let expected =
                m.compare_and_swap(old, new, Ordering::SeqCst);
            let actual = if i == old {
                i = new;
                old
            } else {
                i
            };
            assert_eq!(expected, actual);
        }
    }
}

#[test]
#[should_panic]
fn test_linearizability_of_bad_impl() {
    use model::Shared;
    use std::sync::atomic::{AtomicUsize, Ordering};

    linearizable! {
        Implementation => let i = Shared::new(AtomicUsize::new(0)),
        BuggyAdd(usize)(v in 0usize..4) -> usize {
            let current = i.load(Ordering::SeqCst);
            std::thread::yield_now();
            i.store(current + v, Ordering::SeqCst);
            current + v
        },
        Set(usize)(v in 0usize..4) -> () {
            i.store(v, Ordering::SeqCst)
        },
        BuggyCas((usize, usize))((old, new) in (0usize..4, 0usize..4)) -> usize {
            let current = i.load(Ordering::SeqCst);
            std::thread::yield_now();
            if current == old {
                i.store(new, Ordering::SeqCst);
                new
            } else {
                current
            }
        }
    }
}

#[test]
fn test_linearizability_of_good_impl() {
    use model::Shared;
    use std::sync::atomic::{AtomicUsize, Ordering};

    linearizable! {
        Implementation => let i = Shared::new(AtomicUsize::new(0)),
        Add(usize)(v in 0usize..4) -> usize {
            i.fetch_add(v, Ordering::SeqCst)
        },
        Set(usize)(v in 0usize..4) -> () {
            i.store(v, Ordering::SeqCst)
        },
        Cas((usize, usize))((old, new) in (0usize..4, 0usize..4)) -> usize {
            i.compare_and_swap(old, new, Ordering::SeqCst)
        }
    }
}
