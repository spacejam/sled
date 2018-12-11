use lazy_static::lazy_static;

use std::{
    cell::RefCell,
    fmt,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

// we use BTreeMap during testing for deterministic iteration
#[cfg(test)]
use std::collections::BTreeMap as Map;

#[cfg(not(test))]
use std::collections::HashMap as Map;

#[cfg(test)]
use {
    env_logger,
    log::{debug, info, trace},
    sled_sync,
    std::{
        sync::{
            atomic::AtomicIsize,
            mpsc::{sync_channel, Receiver, SyncSender},
        },
        thread::{self, JoinHandle},
    },
};

use crossbeam::sync::TreiberStack;
use crossbeam_epoch::{pin, Atomic, Guard, Owned, Shared};
use pagetable::PageTable;

const CONTENTION_THRESHOLD: usize = 1;

static TX: AtomicUsize = AtomicUsize::new(0);
static TVARS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
static TICKS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
#[derive(Debug)]
enum ConflictState {
    Init = 0_isize,
    Pending = 1_isize,
    Aborted = 2_isize,
}

#[cfg(test)]
static CONFLICT_TRACKER: AtomicIsize =
    AtomicIsize::new(ConflictState::Init as isize);

lazy_static! {
    static ref G: PageTable<(AtomicUsize, Atomic<Vsn>)> =
        PageTable::default();
    static ref FREE: TreiberStack<usize> = TreiberStack::new();
}

thread_local! {
    static L: RefCell<Map<usize, LocalView>> = RefCell::new(Map::new());
    static TS: RefCell<usize> = RefCell::new(0);
    static GUARD: RefCell<Option<Guard>> = RefCell::new(None);
    static ABORTS: RefCell<usize> = RefCell::new(0);
    static HAS_ABORTED: RefCell<bool> = RefCell::new(false);

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
                    if let Err(e) = s.recv() {
                        eprintln!(
                            "{} scheduler should never hang up: {:?}",
                            tn(),
                            e
                        );
                        std::process::exit(1);
                    }

                    // actually go
                    if let Err(e) = s.recv() {
                        eprintln!(
                            "{} scheduler should never hang up: {:?}",
                            tn(),
                            e
                        );
                        std::process::exit(1);
                    }

                    trace!(
                        "{} {} {} {} {}",
                        TICKS.load(SeqCst),
                        tn(),
                        file!(),
                        line!(),
                        $note,
                    );
                } else {
                    sled_sync::debug_delay();
                }
            })
        }
    };
}

pub fn tx<F, R>(mut f: F) -> R
where
    F: FnMut() -> R,
{
    let res = loop {
        sched_delay!("begin_tx");
        begin_tx();

        let result = f();

        if try_commit() {
            break result;
        }

        maybe_bump_aborts();

        #[cfg(test)]
        debug!("{} retrying tx", tn());
    };

    maybe_decr_aborts();

    sched_delay!("committed tx");
    res
}

fn is_contended() -> bool {
    ABORTS.with(|a| *a.borrow() >= CONTENTION_THRESHOLD)
}

fn maybe_bump_aborts() {
    HAS_ABORTED.with(|ha| {
        let mut ha = ha.borrow_mut();

        if !*ha {
            ABORTS.with(|a| {
                let mut a = a.borrow_mut();
                *a = std::cmp::min(5, *a + 1);
            });
        }

        *ha = true;
    });
}

fn maybe_decr_aborts() {
    HAS_ABORTED.with(|ha| {
        let mut ha = ha.borrow_mut();

        if !*ha {
            ABORTS.with(|a| {
                let mut a = a.borrow_mut();
                *a = std::cmp::max(1, *a) - 1;
            });
        }

        *ha = false;
    });
}

fn current_ts() -> usize {
    TS.with(|ts| *ts.borrow())
}

struct DropContainer(usize);

unsafe impl Send for DropContainer {}
unsafe impl Sync for DropContainer {}

#[derive(Clone)]
struct Dropper(usize);

unsafe impl Send for Dropper {}
unsafe impl Sync for Dropper {}

impl fmt::Debug for Dropper {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Dropper(..)")
    }
}

fn drop_from_ptr<T>(drop_container: DropContainer) {
    let ptr = drop_container.0 as *mut T;
    let owned = unsafe { Box::from_raw(ptr) };
    drop(owned);
}

#[derive(Debug)]
enum LocalView {
    Read {
        ptr: *mut usize,
        read_wts: usize,
    },
    Write {
        ptr: *mut usize,
        read_wts: usize,
        drop: Dropper,
    },
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

    fn dropper(&self) -> Dropper {
        match self {
            LocalView::Read { .. } => {
                panic!("called dropper on a LocalView::Read")
            }
            LocalView::Write { drop, .. } => drop.clone(),
        }
    }

    fn maybe_upgrade_to_write<T: 'static + Clone>(&mut self) {
        let needs_upgrade = self.is_read();
        if needs_upgrade {
            let old_ptr = self.ptr::<T>();
            let new = unsafe { (*old_ptr).clone() };
            let ptr = Box::into_raw(Box::new(new)) as *mut usize;
            let read_wts = self.read_wts();
            *self = LocalView::Write {
                ptr,
                read_wts,
                drop: Dropper(drop_from_ptr::<T> as usize),
            };
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

#[derive(Clone, Debug)]
struct Vsn {
    stable_wts: usize,
    stable: *mut usize,
    pending_wts: usize,
    pending: *mut usize,
}

unsafe impl Send for Vsn {}
unsafe impl Sync for Vsn {}

struct IdDropper {
    id: usize,
    dropper: Dropper,
}

impl Drop for IdDropper {
    fn drop(&mut self) {
        let guard = pin();
        let global_val = G.swap(self.id, Shared::null(), &guard);
        assert_ne!(global_val, Shared::null());

        // NB must happen AFTER resetting G entry to null
        FREE.push(self.id);

        // GC old stable value
        let (_rts, vsn_ptr) = unsafe { global_val.deref() };
        let current_ptr = vsn_ptr.load(SeqCst, &guard);
        let current = unsafe { current_ptr.deref() };

        assert_eq!(current.pending_wts, 0);
        assert_eq!(current.pending, std::ptr::null_mut());

        unsafe {
            guard.defer_destroy(current_ptr);
        }

        // handle GC
        let drop_container = DropContainer(current.stable as usize);
        let dropper: fn(DropContainer) =
            unsafe { std::mem::transmute(self.dropper.clone()) };
        dropper(drop_container)
    }
}

#[derive(Clone)]
pub struct TVar<T: Clone> {
    _pd: PhantomData<T>,
    id_dropper: Arc<IdDropper>,
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
        let id =
            FREE.pop().unwrap_or_else(|| TVARS.fetch_add(1, SeqCst));
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
        ))
        .into_shared(&guard);

        sched_delay!("installing tvar to global table");
        G.cas(id, Shared::null(), gv, &guard)
            .expect("installation should succeed");

        let id_dropper = IdDropper {
            id: id,
            dropper: Dropper(drop_from_ptr::<T> as usize),
        };

        TVar {
            _pd: PhantomData,
            id_dropper: Arc::new(id_dropper),
            id,
        }
    }

    pub fn tvar_id(&self) -> usize {
        self.id
    }
}

fn read_from_g(id: usize, guard: &Guard) -> LocalView {
    sched_delay!("reading tvar from global table");
    let (_rts, vsn_ptr) =
        unsafe { G.get(id, guard).unwrap().deref() };

    sched_delay!("loading atomic ptr for global version");
    let gv = unsafe { vsn_ptr.load(SeqCst, guard).deref() };

    #[cfg(test)]
    debug!("{} reading tvar from global table: {:?}", tn(), gv);

    if !is_contended()
        && gv.pending_wts != 0
        && gv.pending_wts < current_ts()
    {
        // speculative reads if we're not contended
        LocalView::Read {
            ptr: gv.pending,
            read_wts: gv.pending_wts,
        }
    } else {
        LocalView::Read {
            ptr: gv.stable,
            read_wts: gv.stable_wts,
        }
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

fn write_into_l<T: 'static + Clone>(
    id: usize,
    guard: &Guard,
) -> *mut T {
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

impl<T: 'static + Clone> DerefMut for TVar<T> {
    fn deref_mut(&mut self) -> &mut T {
        let guard = pin();
        unsafe { &mut *write_into_l(self.id, &guard) }
    }
}

pub fn begin_tx() {
    #[cfg(test)]
    CONFLICT_TRACKER.compare_and_swap(
        ConflictState::Init as isize,
        ConflictState::Pending as isize,
        SeqCst,
    );

    TS.with(|ts| {
        let mut ts = ts.borrow_mut();
        if *ts == 0 {
            sched_delay!("generating new timestamp");
            *ts = TX.fetch_add(1, SeqCst) + 1;
            #[cfg(test)]
            debug!("{} started tx {}", tn(), *ts);
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
    let ts = current_ts();
    let guard = pin();

    // clear out local cache
    let transacted = L
        .with(|l| {
            let lr: &mut Map<_, _> = &mut *l.borrow_mut();
            std::mem::replace(lr, Map::new())
        })
        .into_iter()
        .map(|(tvar, local)| {
            sched_delay!("re-reading global state");
            let (rts, vsn_ptr) = unsafe {
                G.get(tvar, &guard)
                    .expect("should find TVar in global lookup table")
                    .deref()
            };
            (tvar, rts, vsn_ptr, local)
        })
        .collect::<Vec<_>>();

    // install pending
    for (_tvar, _rts, vsn_ptr, local) in transacted
        .iter()
        .filter(|(_, _, _, local)| local.is_write())
    {
        sched_delay!("reading current ptr before installing write");
        let current_ptr = vsn_ptr.load(SeqCst, &guard);
        let current = unsafe { current_ptr.deref() };

        if !current.pending.is_null()
            || current.pending_wts != 0
            || current.stable_wts > ts
            || local.read_wts() > ts
        {
            // write conflict
            #[cfg(test)]
            debug!(
                "{} hit conflict in tx {} at line {}",
                tn(),
                ts,
                line!()
            );
            return cleanup(ts, transacted, &guard);
        }

        let mut new = current.clone();
        new.pending_wts = ts;
        new.pending = local.ptr();

        #[cfg(test)]
        debug!(
            "{} installing pending {:?} to {:?}",
            tn(),
            current,
            new
        );

        sched_delay!("installing pending write with CAS");
        match vsn_ptr.compare_and_set(
            current_ptr,
            Owned::new(new).into_shared(&guard),
            SeqCst,
            &guard,
        ) {
            Ok(_old) => unsafe {
                guard.defer_destroy(current_ptr);
            },
            Err(_) => {
                // write conflict
                #[cfg(test)]
                debug!(
                    "{} hit conflict in tx {} at line {}",
                    tn(),
                    ts,
                    line!()
                );
                return cleanup(ts, transacted, &guard);
            }
        }
    }

    // update rts
    for (_tvar, rts, vsn_ptr, local) in transacted.iter() {
        bump_gte(rts, ts);

        sched_delay!("reading current stable_rts after bumping RTS");
        let current_ptr = vsn_ptr.load(SeqCst, &guard);
        let current = unsafe { current_ptr.deref() };

        if current.stable_wts > ts {
            #[cfg(test)]
            debug!(
                "{} hit conflict in tx {} at line {}: current: {:?} local: {:?}",
                tn(),
                ts,
                line!(),
                current,
                local,
            );
            return cleanup(ts, transacted, &guard);
        }

        let pending_predecessor =
            current.pending_wts != 0 && current.pending_wts < ts;
        if current.stable_wts != local.read_wts()
            || pending_predecessor
        {
            #[cfg(test)]
            debug!(
                "{} hit conflict in tx {} at line {}: current: {:?} local: {:?}",
                tn(),
                ts,
                line!(),
                current,
                local,
            );
            return cleanup(ts, transacted, &guard);
        }
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
            #[cfg(test)]
            debug!(
                "{} hit conflict in tx {} at line {}, rts {} > our ts {}",
                tn(),
                ts,
                line!(),
                rts,
                ts,
            );
            return cleanup(ts, transacted, &guard);
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
            #[cfg(test)]
            debug!(
                "{} hit conflict in tx {} at line {}, stable_wts {} != our read wts {}",
                tn(),
                ts,
                line!(),
                current.stable_wts,
                local.read_wts(),
            );
            return cleanup(ts, transacted, &guard);
        }
    }

    // commit
    for (_tvar, _rts, vsn_ptr, local) in transacted
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

        #[cfg(test)]
        debug!(
            "{} installing committed old {:?} to new {:?}",
            tn(),
            current,
            new
        );

        sched_delay!("installing new version during commit");
        match vsn_ptr.compare_and_set(
            current_ptr,
            new,
            SeqCst,
            &guard,
        ) {
            Ok(_old) => {
                unsafe {
                    guard.defer_destroy(current_ptr);
                }

                // handle GC
                let drop_container =
                    DropContainer(current.stable as usize);
                let dropper = local.dropper().0;
                guard.defer(move || {
                    let dropper: fn(DropContainer) =
                        unsafe { std::mem::transmute(dropper) };
                    dropper(drop_container)
                });
            }
            Err(_) => {
                // write conflict
                panic!("somehow we got a conflict while committing a transaction");
            }
        }
    }

    #[cfg(test)]
    debug!("{} committed tx {}", tn(), ts);
    TS.with(|ts| *ts.borrow_mut() = 0);
    GUARD.with(|g| {
        g.borrow_mut()
            .take()
            .expect("should be able to end transaction")
    });

    #[cfg(test)]
    CONFLICT_TRACKER.compare_and_swap(
        ConflictState::Pending as isize,
        ConflictState::Init as isize,
        SeqCst,
    );
    true
}

fn cleanup(
    ts: usize,
    transacted: Vec<(usize, &AtomicUsize, &Atomic<Vsn>, LocalView)>,
    guard: &Guard,
) -> bool {
    #[cfg(test)]
    debug!("{} aborting tx {}", tn(), ts);

    for (_tvar, _rts, vsn_ptr, local) in transacted
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
            stable: current.stable,
            stable_wts: current.stable_wts,
            pending: std::ptr::null_mut(),
            pending_wts: 0,
        });

        sched_delay!("installing cleaned-up version");
        vsn_ptr
            .compare_and_set(current_ptr, new, SeqCst, &guard)
            .expect(
                "somehow version changed before we could clean up",
            );

        unsafe {
            guard.defer_destroy(current_ptr);
        }

        // handle GC
        let drop_container = DropContainer(current.pending as usize);
        let dropper = local.dropper().0;
        let dropper: fn(DropContainer) =
            unsafe { std::mem::transmute(dropper) };
        dropper(drop_container);
    }

    TS.with(|ts| *ts.borrow_mut() = 0);
    GUARD.with(|g| {
        g.borrow_mut()
            .take()
            .expect("should be able to end transaction")
    });

    #[cfg(test)]
    CONFLICT_TRACKER.compare_and_swap(
        ConflictState::Pending as isize,
        ConflictState::Aborted as isize,
        SeqCst,
    );

    false
}

fn bump_gte(a: &AtomicUsize, to: usize) {
    sched_delay!("loading RTS before bumping");

    let mut current = a.load(SeqCst);
    while current < to as usize {
        match a.compare_exchange(current, to, SeqCst, SeqCst) {
            Ok(_) => return,
            Err(c) => current = c,
        }
    }
}

#[test]
fn basic() {
    let mut a = TVar::new(5);
    let mut b = TVar::new(String::from("ok"));

    let first_ts = tx(|| {
        *a += 5;
        debug!("a is now {:?}", a);
        *a -= 3;
        debug!("a is now {:?}", a);
        b.push_str("ayo");
        debug!("b is now {:?}", b);
        current_ts()
    });
    let second_ts = tx(|| {
        *a += 5;
        debug!("a is now {:?}", a);
        *a -= 3;
        debug!("a is now {:?}", a);
        b.push_str("ayo");
        debug!("b is now {:?}", b);
        current_ts()
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
    budget: usize,
    mut scheds: Vec<SyncSender<()>>,
    mut threads: Vec<JoinHandle<()>>,
    choices: &mut Vec<(usize, usize)>,
) -> bool {
    debug!("-----------------------------");
    TICKS.store(0, SeqCst);

    // wait for all threads to reach sync points
    for s in &scheds {
        s.send(()).expect("should be able to prime all threads");
    }

    assert!(!scheds.is_empty());

    // replay previous choices
    info!(
        "replaying choices {:?}",
        choices.iter().map(|c| c.0).take(100).collect::<Vec<_>>()
    );

    for c in 0..choices.len() {
        TICKS.fetch_add(1, SeqCst);
        let (choice, _len) = choices[c];

        if choice >= scheds.len() {
            panic!("runtime scheds length mismatch, skipping buggy execution");
        }

        // debug!("choice: {} out of {:?}", choice, scheds);
        scheds[choice]
            .send(())
            .expect("should be able to run target thread if it's still in scheds");

        // prime it or remove from scheds
        if let Err(e) = scheds[choice].send(()) {
            debug!("{} removing finished thread after problem sending: {:?}", tn(), e);
            scheds.remove(choice);
            let t = threads.remove(choice);
            if let Err(e) = t.join() {
                eprintln!(
                    "child thread did not exit successfully: {:?}",
                    e
                );
                std::process::exit(1);
            }
            debug!("{} successfully joined thread", tn());
        }
    }

    debug!("~~~~~~");

    CONFLICT_TRACKER.store(ConflictState::Init as isize, SeqCst);

    // choose 0th choice until we are done
    while !scheds.is_empty() {
        TICKS.fetch_add(1, SeqCst);
        let choice = 0;
        debug!("choice: {} out of {:?}", choice, scheds);
        choices.push((choice, scheds.len()));

        scheds[choice]
            .send(())
            .expect("should be able to run target thread if it's still in scheds");

        // prime it or remove from scheds
        if let Err(e) = scheds[choice].send(()) {
            debug!("removing finished thread after problem sending: {:?}", e);
            scheds.remove(choice);
            let t = threads.remove(choice);
            if let Err(e) = t.join() {
                eprintln!(
                    "child thread did not exit successfully: {:?}",
                    e
                );
                std::process::exit(1);
            }
        } else if choices.len() > budget
            || CONFLICT_TRACKER.load(SeqCst)
                == ConflictState::Aborted as isize
        {
            debug!("detected loop or above budget, running rest out");
            // finish threads
            let mut idx = 0;
            while !scheds.is_empty() {
                idx += 1;
                let choice = idx % scheds.len();
                if scheds[choice].send(()).is_err() {
                    scheds.remove(choice);
                }
            }

            for t in threads.drain(..) {
                t.join().expect("child should not have crashed");
            }
        }
    }

    // info!("took choices: {:?}", choices);

    choices.truncate(budget);

    // pop off choices until we hit one where choice < possibilities
    while !choices.is_empty() {
        let (choice, len) = choices.pop().unwrap();

        if choice + 1 >= len {
            continue;
        }

        choices.push((choice + 1, len));
        break;
    }

    debug!("{} clearing receiving scheduler", tn());

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
    let _ = env_logger::try_init();

    let mut choices = vec![];

    let mut printed = false;
    let possibilities = 1_usize;
    let mut runs = 0;

    loop {
        // clear out FREE because its ordering would
        // cause non-deterministic iteration of items
        // during testing.
        while FREE.pop().is_some() {}

        runs += 1;
        let tvs = [TVar::new(1), TVar::new(2), TVar::new(3)];

        let mut threads = vec![];
        let mut scheds = vec![];

        for t in 0..5 {
            let (transmit, rx) = sync_channel(0);
            let mut tvs = tvs.clone();

            let thread = thread::Builder::new()
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

                            debug!("{} swapping {} and {}", tn(), i1, i2);

                            tx(|| {
                                let tmp1 = *tvs[i1];
                                let tmp2 = *tvs[i2];
                                debug!(
                                    "{} read {}@{} and {}@{} \
                                   before swapping",
                                   tn(),
                                   tmp1,
                                   i1,
                                   tmp2,
                                   i2,
                                 );

                                *tvs[i1] = tmp2;
                                *tvs[i2] = tmp1;
                            });
                        } else {
                            // read tvars

                            debug!("{} reading", tn());
                            let r = tx(|| {
                                [
                                    *tvs[0],
                                    *tvs[1],
                                    *tvs[2],
                                ]
                            });
                            // debug!("{} read {:?}", tn(), r);

                            if (r[0] + r[1] + r[2] != 6) || (r[0] * r[1] * r[2] != 6){
                                println!(
                                    "{} expected observed values to be different, instead: {:?}",
                                    tn(),
                                    r
                                );
                                std::process::exit(1);
                            }
                            assert_eq!(
                                r[0] * r[1] * r[2],
                                6,
                                "{} expected observed values to be different, instead: {:?}",
                                tn(),
                                r
                            );
                        }
                    }
                    debug!("{} clearing receiving scheduler", tn());
                    SCHED.with(|s| {
                        let mut s = s.borrow_mut();
                        drop(s.take());
                    });
                    debug!("{} done", tn());
                }).expect("should be able to start thread");

            threads.push(thread);
            scheds.push(transmit);
        }

        let finished = run_interleavings(
            false,
            5,
            scheds,
            threads,
            &mut choices,
        );

        if finished {
            info!("done, took {}", runs);
            break;
        } else if !printed {
            for (_choice, options) in &choices {
                possibilities.saturating_mul(*options);
            }
            info!("{} possibilities", possibilities);
            printed = true;
        } else {
            debug!("{} getting ready to run another schedule", tn());

            if runs % (possibilities + 1 / 10) == 0 {
                info!("{} / {}", runs, possibilities);
            }
        }
    }
}

#[test]
fn test_swap() {
    let _ = env_logger::try_init();
    for _ in 0..10 {
        let tvs = [TVar::new(1), TVar::new(2), TVar::new(3)];

        let mut threads = vec![];

        for t in 0..20 {
            let mut tvs = tvs.clone();

            let t = thread::Builder::new().name(format!("t_{}", t)).spawn(move || {
                for i in 0..1000 {
                    if (i + t) % 2 == 0 {
                        // swap tvars
                        let i1 = i % 3;
                        let i2 = (i + t) % 3;

                        tx(|| {
                            let tmp1 = *tvs[i1];
                            let tmp2 = *tvs[i2];

                            // debug!("{} tmp1: {:?} tmp2: {:?}", tn(), tmp1, tmp2);

                            *tvs[i1] = tmp2;
                            *tvs[i2] = tmp1;
                        });
                    } else {
                        // read tvars
                        let r = tx(|| [*tvs[0], *tvs[1], *tvs[2]]);
                        // debug!("{} read {:?}", tn(), r);

                        if r[0] + r[1] + r[2] != 6 {
                            println!(
                                "{} expected observed values to be different, instead: {:?}",
                                tn(),
                                r,
                            );
                            std::process::exit(1);
                        }
                        assert_eq!(
                            r[0] * r[1] * r[2],
                            6,
                            "expected observed values to be different, instead: {:?}",
                            r
                        );
                    }
                }
            }).unwrap();

            threads.push(t);
        }

        for t in threads.into_iter() {
            t.join().unwrap();
        }
    }
}

// In this test, we are trying to induce a write skew anomaly.
// Essentially, if a write depends on a read that isn't part of a later write,
// we may not be able to guarantee transactional semantics unless we
// "materialize" the conflict by looking at more than just the writes.
#[test]
fn test_bank_transfer() {
    let _ = env_logger::try_init();
    for _ in 0..10 {
        let alice_accounts =
            [TVar::new(50_isize), TVar::new(50_isize)];
        let bob_account = TVar::new(0_usize);

        let mut threads = vec![];

        for thread_no in 0..20 {
            let mut alice_accounts = alice_accounts.clone();
            let mut bob_account = bob_account.clone();

            let t = thread::Builder::new().name(format!("t_{}", thread_no)).spawn(move || {
                assert!(!is_contended());

                for i in 0..500 {
                    if (i + thread_no) % 2 == 0 {
                        // try to transfer
                        let withdrawal_account = thread_no % alice_accounts.len();

                        tx(|| {
                            let sum = *alice_accounts[0] + *alice_accounts[1];

                            if sum >= 100 {
                                *alice_accounts[withdrawal_account] -= 100;
                                *bob_account += 100;
                            }
                        });
                    } else {
                        // assert that the sum of alice's accounts
                        // never go negative
                        let r = tx(|| (*alice_accounts[0], *alice_accounts[1], *bob_account));

                        assert!(
                            r.0 + r.1 >= 0
                            "possible write skew anomaly detected! expected the \
                            sum of alice's accounts to be >= 0. observed values: {:?}"
                            r
                        );

                        assert_ne!(
                            r.2,
                            200,
                            "A double-transfer to bob was detected! \
                            read values: {:?}",
                            r
                        );

                        // reset accounts
                        tx(|| {
                            *alice_accounts[0] = 50;
                            *alice_accounts[1] = 50;
                            *bob_account = 0;
                        });
                    }
                }
            }).unwrap();

            threads.push(t);
        }

        for t in threads.into_iter() {
            t.join().unwrap();
        }
    }
}

#[test]
fn test_dll() {
    #[derive(Clone)]
    struct Node<T: Clone> {
        item: T,
        next: Option<TVar<Node<T>>>,
        prev: Option<TVar<Node<T>>>,
    }

    #[derive(Clone)]
    struct Dll<T: Clone>(TVar<DllInner<T>>);

    #[derive(Clone)]
    struct DllInner<T: Clone> {
        head: Option<TVar<Node<T>>>,
        tail: Option<TVar<Node<T>>>,
    }

    impl<T: 'static + Clone> Dll<T> {
        fn new() -> Dll<T> {
            Dll(TVar::new(DllInner {
                head: None,
                tail: None,
            }))
        }

        fn push_front(&mut self, item: T) {
            let mut node = TVar::new(Node {
                item: item,
                next: None,
                prev: None,
            });

            tx(|| {
                node.next = Some(
                    self.0
                        .head
                        .clone()
                        .unwrap_or_else(|| node.clone()),
                );
                node.prev = Some(
                    self.0
                        .tail
                        .clone()
                        .unwrap_or_else(|| node.clone()),
                );

                self.0.tail = node.prev.clone();
                self.0.head = Some(node.clone());
            })
        }

        fn push_back(&mut self, item: T) {
            let mut node = TVar::new(Node {
                item: item,
                next: None,
                prev: None,
            });

            tx(|| {
                node.next = Some(
                    self.0
                        .head
                        .clone()
                        .unwrap_or_else(|| node.clone()),
                );
                node.prev = Some(
                    self.0
                        .tail
                        .clone()
                        .unwrap_or_else(|| node.clone()),
                );

                self.0.tail = Some(node.clone());
                self.0.head = node.next.clone();
            })
        }

        fn pop_front(&mut self) -> Option<T> {
            tx(|| {
                let mut taken: TVar<Node<T>> = self.0.head.take()?;
                let id = taken.tvar_id();

                let tail = &mut self.0.tail.clone().unwrap();

                if tail.tvar_id() == id {
                    // head == tail, only one item in Dll
                    self.0.tail.take();
                } else {
                    let next = taken.next.unwrap();
                    tail.next = Some(next.clone());
                    next.prev = Some(tail.clone());
                }

                Some(taken.item.clone())
            })
        }

        fn pop_back(&mut self) -> Option<T> {
            unimplemented!()
        }
    }
}
