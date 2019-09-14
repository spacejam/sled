use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc,
    },
};

use parking_lot::RwLock;

use crate::ivec::IVec;

use crate::pagecache::{OneShot, OneShotFiller};

static ID_GEN: AtomicUsize = AtomicUsize::new(0);

/// An event that happened to a key that a subscriber is interested in.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Event {
    /// A new complete (key, value) pair
    Insert(Arc<[u8]>, IVec),
    /// A deleted key
    Remove(Arc<[u8]>),
}

impl Event {
    /// Return a reference to the key that this `Event` refers to
    pub fn key(&self) -> &[u8] {
        match self {
            Event::Insert(k, ..) | Event::Remove(k) => &*k,
        }
    }
}

impl Clone for Event {
    fn clone(&self) -> Self {
        use self::Event::*;

        match self {
            Insert(k, v) => Insert(k.clone(), v.clone()),
            Remove(k) => Remove(k.clone()),
        }
    }
}

type Senders = Vec<(usize, SyncSender<OneShot<Event>>)>;

/// A subscriber listening on a specified prefix
pub struct Subscriber {
    id: usize,
    rx: Receiver<OneShot<Event>>,
    home: Arc<RwLock<Senders>>,
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        let mut w_senders = self.home.write();
        w_senders.retain(|(id, _)| *id != self.id);
    }
}

impl Iterator for Subscriber {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        loop {
            let future_rx = self.rx.recv().ok()?;
            match future_rx.wait() {
                Some(event) => return Some(event),
                None => continue,
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct Subscriptions {
    watched: RwLock<BTreeMap<Vec<u8>, Arc<RwLock<Senders>>>>,
}

impl Subscriptions {
    pub(crate) fn register(&self, prefix: Vec<u8>) -> Subscriber {
        let r_mu = {
            let r_mu = self.watched.read();
            if r_mu.contains_key(&prefix) {
                r_mu
            } else {
                drop(r_mu);
                let mut w_mu = self.watched.write();
                if !w_mu.contains_key(&prefix) {
                    w_mu.insert(prefix.clone(), Arc::new(RwLock::new(vec![])));
                }
                drop(w_mu);
                self.watched.read()
            }
        };

        let (tx, rx) = sync_channel(1024);

        let arc_senders = &r_mu[&prefix];
        let mut w_senders = arc_senders.write();

        let id = ID_GEN.fetch_add(1, Relaxed);

        w_senders.push((id, tx));

        Subscriber {
            id,
            rx,
            home: arc_senders.clone(),
        }
    }

    pub(crate) fn reserve<R: AsRef<[u8]>>(
        &self,
        key: R,
    ) -> Option<ReservedBroadcast> {
        let r_mu = self.watched.read();
        let prefixes = r_mu.iter().filter(|(k, _)| key.as_ref().starts_with(k));

        let mut subscribers = vec![];

        for (_, subs_rwl) in prefixes {
            let subs = subs_rwl.read();

            for (_id, sender) in subs.iter() {
                let (tx, rx) = OneShot::pair();
                if sender.send(rx).is_err() {
                    continue;
                }
                subscribers.push(tx);
            }
        }

        if subscribers.is_empty() {
            None
        } else {
            Some(ReservedBroadcast { subscribers })
        }
    }
}

pub(crate) struct ReservedBroadcast {
    subscribers: Vec<OneShotFiller<Event>>,
}

impl ReservedBroadcast {
    pub fn complete(self, event: Event) {
        let len = self.subscribers.len();
        let mut iter = self.subscribers.into_iter();

        let mut sent = 0;

        while sent + 1 < len {
            sent += 1;
            let tx = iter.next().unwrap();
            tx.fill(event.clone());
        }

        if len != 0 {
            let tx = iter.next().unwrap();
            tx.fill(event);
        }
    }
}

#[test]
fn basic_subscription() {
    let subs = Subscriptions::default();

    let mut s2 = subs.register([0].to_vec());
    let mut s3 = subs.register([0, 1].to_vec());
    let mut s4 = subs.register([1, 2].to_vec());

    let r1 = subs.reserve(b"awft");
    assert!(r1.is_none());

    let mut s1 = subs.register([].to_vec());

    let k2: Arc<[u8]> = vec![].into();
    let r2 = subs.reserve(&k2).unwrap();
    r2.complete(Event::Insert(k2.clone(), IVec::from(k2.clone())));

    let k3: Arc<[u8]> = vec![0].into();
    let r3 = subs.reserve(&k3).unwrap();
    r3.complete(Event::Insert(k3.clone(), IVec::from(k3.clone())));

    let k4: Arc<[u8]> = vec![0, 1].into();
    let r4 = subs.reserve(&k4).unwrap();
    r4.complete(Event::Remove(k4.clone()));

    let k5: Arc<[u8]> = vec![0, 1, 2].into();
    let r5 = subs.reserve(&k5).unwrap();
    r5.complete(Event::Insert(k5.clone(), IVec::from(k5.clone())));

    let k6: Arc<[u8]> = vec![1, 1, 2].into();
    let r6 = subs.reserve(&k6).unwrap();
    r6.complete(Event::Remove(k6.clone()));

    let k7: Arc<[u8]> = vec![1, 1, 2].into();
    let r7 = subs.reserve(&k7).unwrap();
    drop(r7);

    let k8: Arc<[u8]> = vec![1, 2, 2].into();
    let r8 = subs.reserve(&k8).unwrap();
    r8.complete(Event::Insert(k8.clone(), IVec::from(k8.clone())));

    assert_eq!(s1.next().unwrap().key(), &*k2);
    assert_eq!(s1.next().unwrap().key(), &*k3);
    assert_eq!(s1.next().unwrap().key(), &*k4);
    assert_eq!(s1.next().unwrap().key(), &*k5);
    assert_eq!(s1.next().unwrap().key(), &*k6);
    assert_eq!(s1.next().unwrap().key(), &*k8);

    assert_eq!(s2.next().unwrap().key(), &*k3);
    assert_eq!(s2.next().unwrap().key(), &*k4);
    assert_eq!(s2.next().unwrap().key(), &*k5);

    assert_eq!(s3.next().unwrap().key(), &*k4);
    assert_eq!(s3.next().unwrap().key(), &*k5);

    assert_eq!(s4.next().unwrap().key(), &*k8);
}
