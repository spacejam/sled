#[macro_use]
extern crate lazy_static;
extern crate crossbeam_epoch;
extern crate pagetable;

use std::{
    cell::RefCell,
    collections::HashMap,
    fmt,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::atomic::{
        AtomicUsize,
        Ordering::{Acquire, Relaxed, SeqCst},
    },
};

use crossbeam_epoch::{pin, Atomic, Guard, Owned, Shared};
use pagetable::PageTable;

#[macro_export]
macro_rules! tx {
    ($($item:expr;)*) => {
        tx! {
            $($item);*
        }
    };
    ($($item:expr);*) => {
        loop {
            $crate::begin_tx();

            $($item);*;

            if $crate::try_commit() {
                break;
            }
        }
    };
}

static TX: AtomicUsize = AtomicUsize::new(0);
static TVARS: AtomicUsize = AtomicUsize::new(0);

lazy_static! {
    static ref G: PageTable<(AtomicUsize, Atomic<Vsn>)> =
        PageTable::default();
}

thread_local! {
    static L: RefCell<HashMap<usize, LocalView>> = RefCell::new(HashMap::new());
    static TS: RefCell<usize> = RefCell::new(0);
    static GUARD: RefCell<Option<Guard>> = RefCell::new(None);
}

struct DelayedDropPtr<T>(*mut T);

unsafe impl<T> Send for DelayedDropPtr<T> {}

/*
    1. generate new timestamp
    1. Deref pulls versioned ref into local cache, if not present
    1. DerefMut pulls versioned clone into local cache, if not present
    1. stage writes
    1. verify for all reads, no w_ts > observed ts
    1. verify for all writes, no r/w_ts > ts
    1. commit
*/

#[derive(Debug)]
enum LocalView {
    Read { ptr: *mut usize, read_wts: usize },
    Write { ptr: *mut usize, read_wts: usize },
}

impl LocalView {
    fn ptr<T>(&self) -> *mut T {
        match self {
            LocalView::Read { ref ptr, .. }
            | LocalView::Write { ref ptr, .. } => *ptr as *mut T,
        }
    }

    fn read_wts(&self) -> usize {
        match self {
            LocalView::Read { read_wts, .. }
            | LocalView::Write { read_wts, .. } => *read_wts,
        }
    }

    fn maybe_upgrade_to_write<T: Clone>(&mut self) {
        let needs_upgrade = self.is_read();
        if needs_upgrade {
            let old_ptr = self.ptr::<T>();
            let new = unsafe { (*old_ptr).clone() };
            let ptr = Box::into_raw(Box::new(new)) as *mut usize;
            let read_wts = self.read_wts();
            *self = LocalView::Write { ptr, read_wts };
        }
    }

    fn is_read(&self) -> bool {
        if let LocalView::Read { .. } = self {
            true
        } else {
            false
        }
    }

    fn is_write(&self) -> bool {
        if let LocalView::Write { .. } = self {
            true
        } else {
            false
        }
    }
}

struct Vsn {
    stable_wts: usize,
    stable: *mut usize,
    pending_wts: usize,
    pending: *mut usize,
}

unsafe impl Send for Vsn {}
unsafe impl Sync for Vsn {}

#[derive(Copy, Clone)]
struct TVar<T: Clone> {
    _pd: PhantomData<T>,
    id: usize,
}

impl<T> fmt::Debug for TVar<T>
where
    T: 'static + Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TVar({:?})", read_from_l::<T>(self.id))
    }
}

impl<T: Clone> TVar<T> {
    fn new(val: T) -> TVar<T> {
        let id = TVARS.fetch_add(1, Relaxed);
        let ptr = Box::into_raw(Box::new(val));
        let guard = pin();
        let gv = Owned::new((
            AtomicUsize::new(0),
            Vsn {
                stable_wts: 0,
                stable: ptr as *mut usize,
                pending_wts: 0,
                pending: std::ptr::null_mut(),
            },
        )).into_shared(&guard);

        G.compare_and_set(id, Shared::null(), gv, &guard);

        TVar {
            _pd: PhantomData,
            id,
        }
    }
}

fn read_from_g(id: usize, guard: &Guard) -> LocalView {
    let gv = G.get(&id, guard).unwrap();
    LocalView::Read {
        ptr: gv.stable,
        read_wts: gv.stable_wts,
    }
}

fn read_from_l<T>(id: usize) -> &'static T {
    L.with(|l| {
        let mut hm = l.borrow_mut();
        let lk = hm.entry(id).or_insert_with(|| read_from_g(id));
        unsafe { &mut *(lk.ptr() as *mut T) }
    })
}

fn write_into_l<T: Clone>(
    id: usize,
    guard: &Guard,
) -> &'static mut T {
    L.with(|l| {
        let mut hm = l.borrow_mut();
        let mut lk = hm.entry(id).or_insert_with(|| read_from_g(id));
        lk.maybe_upgrade_to_write::<T>();
        unsafe { &mut *(lk.ptr() as *mut T) }
    })
}

impl<T: 'static + Clone> Deref for TVar<T> {
    type Target = T;

    fn deref(&self) -> &T {
        read_from_l(self.id)
    }
}

impl<T: 'static + Clone> DerefMut for TVar<T> {
    fn deref_mut(&mut self) -> &mut T {
        write_into_l(self.id)
    }
}

struct Ref<T: Clone>(T, bool);

pub fn begin_tx() {
    TS.with(|ts| {
        let mut ts = ts.borrow_mut();
        if *ts == 0 {
            *ts = TX.fetch_add(1, SeqCst) + 1;
            GUARD.with(|g| {
                let mut g = g.borrow_mut();
                assert!(g.is_none(), "we expect that no local transaction is already happening");
                *g = Some(pin());
            });
        }
    });
}

// fn remove_pending(ts: usize,

pub fn try_commit() -> bool {
    let ts = TS.with(|ts| *ts.borrow());
    let guard = pin();

    // clear out local cache
    let transacted = L.with(|l| {
        let lr: &mut HashMap<_, _> = &mut *l.borrow_mut();
        std::mem::replace(lr, HashMap::new())
    });

    // install pending
    for (tvar, local) in
        transacted.iter().filter(|(_tvar, local)| local.is_write())
    {
        println!("installing write for tv {}", tvar);
        let (_rts, vsn_ptr) = G
            .get(*tvar, &guard)
            .expect("should find TVar in global lookup table")
            .deref();

        let current_ptr = vsn_ptr.load(SeqCst, &guard);
        let current = current_ptr.deref();

        if !current.pending.is_null() || current.pending_wts != 0 {
            // write conflict
            cleanup(transacted);
            return false;
        }

        let mut new = *current.clone();
        new.pending_wts = ts;
        new.pending = local.ptr();

        match vsn_ptr.compare_and_set(
            current_ptr,
            Owned::new(new).into_shared(&guard),
            SeqCst,
            &guard,
        ) {
            Ok(old) => guard.defer(|| drop(old)),
            Err(_) => {
                // write conflict
                cleanup(transacted);
                return false;
            }
        }
    }

    // update rts
    for (tvar, local) in
        transacted.iter().filter(|(_tvar, local)| local.is_read())
    {
        let (rts, _vsn_ptr) = G
            .get(*tvar, &guard)
            .expect("should find TVar in global lookup table")
            .deref();

        bump_gte(&rts, ts);
    }

    // check version consistency
    for (tvar, local) in
        transacted.iter().filter(|(_tvar, local)| local.is_write())
    {
        let (rts, vsn_ptr) = G
            .get(*tvar, &guard)
            .expect("should find TVar in global lookup table")
            .deref();

        let rts = rts.load(Acquire);

        if rts > ts {
            // read conflict
            cleanup(transacted);
            return false;
        }

        let current = vsn_ptr.load(SeqCst, &guard).deref();

        assert_eq!(
            current.pending_wts, ts,
            "somehow our pending write got lost"
        );

        if current.stable_wts > ts {
            // write conflict
            cleanup(transacted);
            return false;
        }
    }

    // commit
    for (tvar, local) in
        transacted.iter().filter(|(_tvar, local)| local.is_write())
    {
        let (_rts, vsn_ptr) = G
            .get(*tvar, &guard)
            .expect("should find TVar in global lookup table")
            .deref();

        let current_ptr = vsn_ptr.load(SeqCst, &guard);
        let current = current_ptr.deref();

        assert_eq!(
            current.pending_wts, ts,
            "somehow our pending write got lost"
        );

        if !current.pending.is_null() {
            cleanup(transacted);
            return false;
        }

        let new = Owned::new(Vsn {
            stable: current.pending,
            stable_wts: current.pending_wts,
            pending: std::ptr::null_mut(),
            pending_wts: 0,
        });

        match vsn_ptr.compare_and_set(
            current_ptr,
            new,
            SeqCst,
            &guard,
        ) {
            Ok(old) => guard.defer(|| drop(old)),
            Err(_) => {
                // write conflict
                cleanup(transacted);
                return false;
            }
        }
    }

    TS.with(|ts| *ts.borrow_mut() = 0);
    GUARD.with(|g| g.borrow_mut().take());
    true
}

fn cleanup(transacted: HashMap<usize, LocalView>) {
    for (tvar, local) in
        transacted.iter().filter(|(_tvar, local)| local.is_write())
    {
        let mut global = g
            .get_mut(&tvar)
            .expect("should find TVar in global lookup table");

        global.pending = std::ptr::null_mut();
        global.pending_wts = 0;
    }
}

fn bump_gte(a: &AtomicUsize, to: usize) {
    let mut current = a.load(Acquire);
    while current < to as usize {
        let last = a.compare_and_swap(current, to, SeqCst);
        if last == current {
            // we succeeded.
            return;
        }
        current = last;
    }
}

#[test]
fn basic() {
    let mut a = TVar::new(5);
    let mut b = TVar::new(String::from("ok"));

    let mut first_ts = 0;
    tx! {
        *a += 5;
        println!("a is now {:?}", a);
        *a -= 3;
        println!("a is now {:?}", a);
        b.push_str("ayo");
        println!("b is now {:?}", b);
        first_ts = TS.with(|ts| *ts.borrow());
    }
    tx! {
        *a += 5;
        println!("a is now {:?}", a);
        *a -= 3;
        println!("a is now {:?}", a);
        b.push_str("ayo");
        println!("b is now {:?}", b);
        first_ts = TS.with(|ts| *ts.borrow());
    }

    let mut second_ts = 0;
    tx! {
        second_ts = TS.with(|ts| *ts.borrow());
    }

    assert!(
        second_ts > first_ts,
        "expected second timestamp ({}) to be above the first ({})",
        second_ts,
        first_ts
    );
}
