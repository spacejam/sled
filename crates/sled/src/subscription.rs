use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc, RwLock,
    },
};

use futures::{
    future::Future,
    sync::oneshot::{
        channel as future_channel, Receiver as FutureReceiver,
        Sender as FutureSender,
    },
};

static ID_GEN: AtomicUsize = AtomicUsize::new(0);

/// An event that happened to a key that a subscriber is interested in.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Event {
    /// A new complete (key, value) pair
    Set(Vec<u8>, Vec<u8>),
    /// A new partial (key, merged value) pair
    Merge(Vec<u8>, Vec<u8>),
    /// A deleted key
    Del(Vec<u8>),
}

impl Event {
    /// Return a reference to the key that this `Event` refers to
    pub fn key(&self) -> &[u8] {
        match self {
            Event::Set(k, ..)
            | Event::Merge(k, ..)
            | Event::Del(k) => &*k,
        }
    }
}

impl Clone for Event {
    fn clone(&self) -> Event {
        use self::Event::*;

        match self {
            Set(k, v) => Set(k.clone(), v.clone()),
            Merge(k, v) => Merge(k.clone(), v.clone()),
            Del(k) => Del(k.clone()),
        }
    }
}

type Senders = Vec<(usize, SyncSender<FutureReceiver<Event>>)>;

/// A subscriber listening on a specified prefix
pub struct Subscriber {
    id: usize,
    rx: Receiver<FutureReceiver<Event>>,
    home: Arc<RwLock<Senders>>,
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        let mut w_senders = self.home.write().unwrap();
        w_senders.retain(|(id, _)| *id != self.id);
    }
}

impl Iterator for Subscriber {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        loop {
            let future_rx = self.rx.recv().ok()?;
            match future_rx.wait() {
                Ok(event) => return Some(event),
                Err(_cancelled) => continue,
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
            let r_mu = self.watched.read().unwrap();
            if r_mu.contains_key(&prefix) {
                r_mu
            } else {
                drop(r_mu);
                let mut w_mu = self.watched.write().unwrap();
                if !w_mu.contains_key(&prefix) {
                    w_mu.insert(
                        prefix.clone(),
                        Arc::new(RwLock::new(vec![])),
                    );
                }
                drop(w_mu);
                self.watched.read().unwrap()
            }
        };

        let (tx, rx) = sync_channel(1024);

        let arc_senders = &r_mu[&prefix];
        let mut w_senders = arc_senders.write().unwrap();

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
        let r_mu = self.watched.read().unwrap();
        let prefixes =
            r_mu.iter().filter(|(k, _)| key.as_ref().starts_with(k));

        let mut subscribers = vec![];

        for (_, subs_rwl) in prefixes {
            let subs = subs_rwl.read().unwrap();

            for (_id, sender) in subs.iter() {
                let (tx, rx) = future_channel();
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
    subscribers: Vec<FutureSender<Event>>,
}

impl ReservedBroadcast {
    pub fn complete(self, event: Event) {
        let len = self.subscribers.len();
        let mut iter = self.subscribers.into_iter();

        let mut sent = 0;

        while sent + 1 < len {
            sent += 1;
            let tx = iter.next().unwrap();
            let _ = tx.send(event.clone());
        }

        if len != 0 {
            let tx = iter.next().unwrap();
            let _ = tx.send(event);
        }
    }
}

#[test]
fn basic_subscription() {
    let subs = Subscriptions::default();

    let mut s2 = subs.register(vec![0]);
    let mut s3 = subs.register(vec![0, 1]);
    let mut s4 = subs.register(vec![1, 2]);

    let r1 = subs.reserve(b"awft");
    assert!(r1.is_none());

    let mut s1 = subs.register(vec![]);

    let k2 = vec![];
    let r2 = subs.reserve(&k2).unwrap();
    r2.complete(Event::Set(k2.clone(), k2.clone()));

    let k3 = vec![0];
    let r3 = subs.reserve(&k3).unwrap();
    r3.complete(Event::Set(k3.clone(), k3.clone()));

    let k4 = vec![0, 1];
    let r4 = subs.reserve(&k4).unwrap();
    r4.complete(Event::Del(k4.clone()));

    let k5 = vec![0, 1, 2];
    let r5 = subs.reserve(&k5).unwrap();
    r5.complete(Event::Merge(k5.clone(), k5.clone()));

    let k6 = vec![1, 1, 2];
    let r6 = subs.reserve(&k6).unwrap();
    r6.complete(Event::Del(k6.clone()));

    let k7 = vec![1, 1, 2];
    let r7 = subs.reserve(&k7).unwrap();
    drop(r7);

    let k8 = vec![1, 2, 2];
    let r8 = subs.reserve(&k8).unwrap();
    r8.complete(Event::Set(k8.clone(), k8.clone()));

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
