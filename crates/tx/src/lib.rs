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
    sync::atomic::{AtomicUsize, Ordering::SeqCst},
};

#[cfg(test)]
use std::{
    sync::mpsc::{sync_channel, Receiver, SyncSender},
    thread::{self, JoinHandle},
};

use crossbeam_epoch::{pin, Atomic, Guard, Owned, Shared};
use pagetable::PageTable;

static TX: AtomicUsize = AtomicUsize::new(0);
static TVARS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
static TICKS: AtomicUsize = AtomicUsize::new(0);

// TODO add GC stack to entries here
lazy_static! {
    static ref G: PageTable<(AtomicUsize, Atomic<Vsn>)> =
        PageTable::default();
}

thread_local! {
    static L: RefCell<HashMap<usize, LocalView>> = RefCell::new(HashMap::new());
    static TS: RefCell<usize> = RefCell::new(0);
    static GUARD: RefCell<Option<Guard>> = RefCell::new(None);

    // a scheduler for exhaustive testing
    #[cfg(test)]
    static SCHED: RefCell<Option<Receiver<()>>> = RefCell::new(None);
}

#[cfg(test)]
fn tn() -> String {
    std::thread::current()
        .name()
        .unwrap_or("unknown")
        .to_owned()
}

macro_rules! sched_delay {
    ($note:expr) => {
        #[cfg(not(test))]
        {}
        #[cfg(test)]
        {
            SCHED.with(|ref s| {
                if let Some(s) = &*s.borrow() {
                    // prime to signal readiness to scheduler
                    s.recv().unwrap();

                    // actually go
                    s.recv().unwrap();
                    println!(
                        "{} {} {} {} {}",
                        TICKS.load(SeqCst),
                        tn(),
                        file!(),
                        line!(),
                        $note,
                    );
                }
            })
        }
    };
}

pub fn tx<F, R>(mut f: F) -> R
where
    F: FnMut() -> R,
{
    sched_delay!("begin_tx");
    let res = loop {
        begin_tx();

        let result = f();

        if try_commit() {
            break result;
        }
    };
    sched_delay!("committed tx");
    res
}

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

#[derive(Clone)]
struct Vsn {
    stable_wts: usize,
    stable: *mut usize,
    pending_wts: usize,
    pending: *mut usize,
}

unsafe impl Send for Vsn {}
unsafe impl Sync for Vsn {}

#[derive(Clone)]
pub struct TVar<T: Clone> {
    _pd: PhantomData<T>,
    id: usize,
}

impl<T> fmt::Debug for TVar<T>
where
    T: Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let guard = pin();
        write!(f, "TVar({:?})", unsafe {
            &*read_from_l::<T>(self.id, &guard)
        })
    }
}

impl<T: Clone> TVar<T> {
    pub fn new(val: T) -> TVar<T> {
        sched_delay!("creating new tvar");
        let id = TVARS.fetch_add(1, SeqCst);
        let ptr = Box::into_raw(Box::new(val));
        let guard = pin();
        let gv = Owned::new((
            AtomicUsize::new(0),
            Atomic::new(Vsn {
                stable_wts: 0,
                stable: ptr as *mut usize,
                pending_wts: 0,
                pending: std::ptr::null_mut(),
            }),
        )).into_shared(&guard);

        sched_delay!("installing tvar to global table");
        G.cas(id, Shared::null(), gv, &guard)
            .expect("installation should succeed");

        TVar {
            _pd: PhantomData,
            id,
        }
    }
}

fn read_from_g(id: usize, guard: &Guard) -> LocalView {
    sched_delay!("reading tvar from global table");
    let (_rts, vsn_ptr) =
        unsafe { G.get(id, guard).unwrap().deref() };

    sched_delay!("loading atomic ptr for global version");
    let gv = unsafe { vsn_ptr.load(SeqCst, guard).deref() };
    LocalView::Read {
        ptr: gv.stable,
        read_wts: gv.stable_wts,
    }
}

fn read_from_l<T>(id: usize, guard: &Guard) -> *mut T {
    L.with(|l| {
        let mut hm = l.borrow_mut();
        let lk =
            hm.entry(id).or_insert_with(|| read_from_g(id, guard));
        unsafe { &mut *(lk.ptr() as *mut T) }
    })
}

fn write_into_l<T: Clone>(id: usize, guard: &Guard) -> *mut T {
    L.with(|l| {
        let mut hm = l.borrow_mut();
        let lk =
            hm.entry(id).or_insert_with(|| read_from_g(id, guard));
        lk.maybe_upgrade_to_write::<T>();
        unsafe { &mut *(lk.ptr() as *mut T) }
    })
}

impl<T: Clone> Deref for TVar<T> {
    type Target = T;

    fn deref(&self) -> &T {
        let guard = pin();
        unsafe { &*read_from_l(self.id, &guard) }
    }
}

impl<T: Clone> DerefMut for TVar<T> {
    fn deref_mut(&mut self) -> &mut T {
        let guard = pin();
        unsafe { &mut *write_into_l(self.id, &guard) }
    }
}

pub fn begin_tx() {
    TS.with(|ts| {
        let mut ts = ts.borrow_mut();
        if *ts == 0 {
            sched_delay!("generating new timestamp");
            *ts = TX.fetch_add(1, SeqCst) + 1;
            GUARD.with(|g| {
                let mut g = g.borrow_mut();
                assert!(
                    g.is_none(),
                    "we do not support nested transactions yet"
                );
                *g = Some(pin());
            });
        }
    });
}

pub fn try_commit() -> bool {
    let ts = TS.with(|ts| *ts.borrow());
    let guard = pin();
    let mut abort = false;

    // clear out local cache
    let transacted = L
        .with(|l| {
            let lr: &mut HashMap<_, _> = &mut *l.borrow_mut();
            std::mem::replace(lr, HashMap::new())
        }).into_iter()
        .map(|(tvar, local)| {
            sched_delay!("re-reading global state");
            let (rts, vsn_ptr) = unsafe {
                G.get(tvar, &guard)
                    .expect("should find TVar in global lookup table")
                    .deref()
            };
            (tvar, rts, vsn_ptr, local)
        }).collect::<Vec<_>>();

    // install pending
    for (_tvar, _rts, vsn_ptr, local) in transacted
        .iter()
        .filter(|(_, _, _, local)| local.is_write())
    {
        // println!("installing write for tvar {}", tvar);
        sched_delay!("reading current ptr before installing write");
        let current_ptr = vsn_ptr.load(SeqCst, &guard);
        let current = unsafe { current_ptr.deref() };

        if !current.pending.is_null() || current.pending_wts != 0 {
            // write conflict
            abort = true;
            break;
        }

        let mut new = current.clone();
        new.pending_wts = ts;
        new.pending = local.ptr();

        sched_delay!("installing write with CAS");
        match vsn_ptr.compare_and_set(
            current_ptr,
            Owned::new(new).into_shared(&guard),
            SeqCst,
            &guard,
        ) {
            Ok(_old) => {
                // TODO delay add the ptr to the gc stack for this TVar
                // guard.defer(|| drop(ddp.0));
            }
            Err(_) => {
                // write conflict
                abort = true;
                break;
            }
        }
    }

    if abort {
        cleanup(ts, transacted, &guard);
        return false;
    }

    // update rts
    for (_tvar, rts, vsn_ptr, local) in
        transacted.iter().filter(|(_, _, _, local)| local.is_read())
    {
        bump_gte(rts, ts);

        sched_delay!("reading current stable_rts after bumping RTS");
        let current_ptr = vsn_ptr.load(SeqCst, &guard);
        let current = unsafe { current_ptr.deref() };

        if current.stable_wts != local.read_wts() {
            abort = true;
            break;
        }
    }

    if abort {
        cleanup(ts, transacted, &guard);
        return false;
    }

    // check version consistency
    for (_tvar, rts, vsn_ptr, local) in transacted
        .iter()
        .filter(|(_, _, _, local)| local.is_write())
    {
        sched_delay!("reading RTS to check consistency");
        let rts = rts.load(SeqCst);

        if rts > ts {
            // read conflict
            abort = true;
            break;
        }

        sched_delay!(
            "loading current ptr to check current stable and pending"
        );
        let current = unsafe { vsn_ptr.load(SeqCst, &guard).deref() };

        assert_eq!(
            current.pending_wts, ts,
            "somehow our pending write got lost"
        );

        if current.stable_wts != local.read_wts() {
            // write conflict
            abort = true;
            break;
        }
    }

    if abort {
        cleanup(ts, transacted, &guard);
        return false;
    }

    // commit
    for (_tvar, _rts, vsn_ptr, _local) in transacted
        .iter()
        .filter(|(_, _, _, local)| local.is_write())
    {
        sched_delay!("loading current version before committing");
        let current_ptr = vsn_ptr.load(SeqCst, &guard);
        let current = unsafe { current_ptr.deref() };

        assert_eq!(
            current.pending_wts, ts,
            "somehow our pending write got lost"
        );

        if current.pending.is_null() {
            panic!("somehow an item in our commit writeset isn't present in the Vsn anymore");
        }

        let new = Owned::new(Vsn {
            stable: current.pending,
            stable_wts: current.pending_wts,
            pending: std::ptr::null_mut(),
            pending_wts: 0,
        });

        sched_delay!("installing new version during commit");
        match vsn_ptr.compare_and_set(
            current_ptr,
            new,
            SeqCst,
            &guard,
        ) {
            Ok(_old) => {
                // TODO handle GC
                // guard.defer(|| drop(old)),
            }
            Err(_) => {
                // write conflict
                panic!("somehow we got a conflict while committing a transaction");
            }
        }
    }

    TS.with(|ts| *ts.borrow_mut() = 0);
    GUARD.with(|g| {
        g.borrow_mut()
            .take()
            .expect("should be able to end transaction")
    });
    true
}

fn cleanup(
    ts: usize,
    transacted: Vec<(usize, &AtomicUsize, &Atomic<Vsn>, LocalView)>,
    guard: &Guard,
) {
    for (_tvar, _rts, vsn_ptr, _local) in transacted
        .iter()
        .filter(|(_, _, _, local)| local.is_write())
    {
        sched_delay!("reading current version before cleaning up");
        let current_ptr = vsn_ptr.load(SeqCst, &guard);
        let current = unsafe { current_ptr.deref() };

        if current.pending_wts != ts || current.pending.is_null() {
            continue;
        }

        let new = Owned::new(Vsn {
            stable: current.pending,
            stable_wts: current.pending_wts,
            pending: std::ptr::null_mut(),
            pending_wts: 0,
        });

        sched_delay!("installing cleaned-up version");
        vsn_ptr
            .compare_and_set(current_ptr, new, SeqCst, &guard)
            .expect(
                "somehow version changed before we could clean up",
            );
    }

    TS.with(|ts| *ts.borrow_mut() = 0);
    GUARD.with(|g| {
        g.borrow_mut()
            .take()
            .expect("should be able to end transaction")
    });
}

fn bump_gte(a: &AtomicUsize, to: usize) {
    sched_delay!("loading RTS before bumping");
    let mut current = a.load(SeqCst);
    while current < to as usize {
        current = a.compare_and_swap(current, to, SeqCst);
    }
}

#[test]
fn basic() {
    let mut a = TVar::new(5);
    let mut b = TVar::new(String::from("ok"));

    let first_ts = tx(|| {
        *a += 5;
        println!("a is now {:?}", a);
        *a -= 3;
        println!("a is now {:?}", a);
        b.push_str("ayo");
        println!("b is now {:?}", b);
        TS.with(|ts| *ts.borrow())
    });
    let second_ts = tx(|| {
        *a += 5;
        println!("a is now {:?}", a);
        *a -= 3;
        println!("a is now {:?}", a);
        b.push_str("ayo");
        println!("b is now {:?}", b);
        TS.with(|ts| *ts.borrow())
    });

    assert!(
        second_ts > first_ts,
        "expected second timestamp ({}) to be above the first ({})",
        second_ts,
        first_ts
    );
}

#[cfg(test)]
fn run_interleavings(
    symmetric: bool,
    mut scheds: Vec<SyncSender<()>>,
    mut threads: Vec<JoinHandle<()>>,
    choices: &mut Vec<(usize, usize)>,
) -> bool {
    println!("-----------------------------");
    TICKS.store(0, SeqCst);

    // wait for all threads to reach sync points
    for s in &scheds {
        s.send(()).expect("should be able to prime all threads");
    }

    assert!(!scheds.is_empty());

    // replay previous choices
    // println!("replaying choices {:?}", choices);

    for c in 0..choices.len() {
        TICKS.fetch_add(1, SeqCst);
        let (choice, _len) = choices[c];

        // println!("choice: {} out of {:?}", choice, scheds);
        scheds[choice].send(()).expect("should be able to run target thread if it's still in scheds");

        // prime it or remove from scheds
        if scheds[choice].send(()).is_err() {
            scheds.remove(choice);
            let t = threads.remove(choice);
            if let Err(e) = t.join() {
                panic!(e);
            }
        }
    }

    println!("~~~~~~");

    // choose 0th choice until we are done
    while !scheds.is_empty() {
        TICKS.fetch_add(1, SeqCst);
        let choice = 0;
        // println!("choice: {} out of {:?}", choice, scheds);
        choices.push((choice, scheds.len()));

        scheds[choice].send(()).expect("should be able to run target thread if it's still in scheds");

        // prime it or remove from scheds
        if scheds[choice].send(()).is_err() {
            scheds.remove(choice);
            let t = threads.remove(choice);
            if let Err(e) = t.join() {
                panic!(e);
            }
        }
    }

    println!("took choices: {:?}", choices);

    // pop off choices until we hit one where choice < possibilities
    while !choices.is_empty() {
        let (choice, len) = choices.pop().unwrap();

        if choice + 1 >= len {
            continue;
        }

        choices.push((choice + 1, len));
        break;
    }

    // we're done when we've explored every option
    if symmetric {
        // symmetric workloads don't need to try other top-levels
        choices.len() <= 1
    } else {
        choices.is_empty()
    }
}

#[test]
#[ignore]
fn test_swap_scheduled() {
    let mut choices = vec![];

    loop {
        let tvs = [TVar::new(1), TVar::new(2), TVar::new(3)];

        let mut threads = vec![];
        let mut scheds = vec![];

        for t in 0..2 {
            let (transmit, rx) = sync_channel(0);
            let mut tvs = tvs.clone();

            let t = thread::Builder::new()
                .name(format!("t_{}", t))
                .spawn(move || {
                    // set up scheduler listener
                    SCHED.with(|s| {
                        let mut s = s.borrow_mut();
                        *s = Some(rx);
                    });

                    for i in 0..2 {
                        if (i + t) % 2 == 0 {
                            // swap tvars
                            let i1 = i % 3;
                            let i2 = (i + t) % 3;

                            tx(|| {
                                let tmp1 = *tvs[i1];
                                let tmp2 = *tvs[i2];

                                *tvs[i1] = tmp2;
                                *tvs[i2] = tmp1;
                            });
                        } else {
                            // read tvars

                            let r = tx(|| {
                                [
                                    *tvs[0],
                                    *tvs[1],
                                    *tvs[2],
                                ]
                            });

                            assert_eq!(
                                r[0] + r[1] + r[2],
                                6,
                                "expected observed values to be different, instead: {:?}",
                                r
                            );
                            assert_eq!(
                                r[0] * r[1] * r[2],
                                6,
                                "expected observed values to be different, instead: {:?}",
                                r
                            );
                        }
                    }
                }).expect("should be able to start thread");

            threads.push(t);
            scheds.push(transmit);
        }

        let finished =
            run_interleavings(true, scheds, threads, &mut choices);
        if finished {
            return;
        }
    }
}

#[test]
fn test_swap() {
    let tvs = [TVar::new(1), TVar::new(2), TVar::new(3)];

    let mut threads = vec![];

    for t in 0..2 {
        let mut tvs = tvs.clone();

        let t = thread::spawn(move || {
            for i in 0..5 {
                if (i + t) % 2 == 0 {
                    // swap tvars
                    let i1 = i % 3;
                    let i2 = (i + t) % 3;

                    tx(|| {
                        let tmp1 = *tvs[i1];
                        let tmp2 = *tvs[i2];

                        *tvs[i1] = tmp2;
                        *tvs[i2] = tmp1;
                    });
                } else {
                    // read tvars
                    let r = tx(|| [*tvs[0], *tvs[1], *tvs[2]]);

                    assert_eq!(
                        r[0] + r[1] + r[2],
                        6,
                        "expected observed values to be different, instead: {:?}",
                        r
                    );
                    assert_eq!(
                        r[0] * r[1] * r[2],
                        6,
                        "expected observed values to be different, instead: {:?}",
                        r
                    );
                }
            }
        });

        threads.push(t);
    }

    for t in threads.into_iter() {
        t.join().unwrap();
    }
}
