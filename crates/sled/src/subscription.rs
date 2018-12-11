use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc, RwLock,
    },
};

use futures::sync::oneshot::{
    channel as future_channel, Receiver as FutureReceiver,
    Sender as FutureSender,
};

static ID_GEN: AtomicUsize = AtomicUsize::new(0);

/// An event that happened to a key that a subscriber is interested in.
#[derive(Debug)]
pub enum Event {
    Set(Vec<u8>),
    Merge(Vec<u8>),
    Del(Vec<u8>),
}

impl Clone for Event {
    fn clone(&self) -> Event {
        use self::Event::*;

        match self {
            Set(k) => Set(k.clone()), //, v.shallow_copy()),
            Merge(k) => Merge(k.clone()), //, v.shallow_copy()),
            Del(k) => Del(k.clone()),
        }
    }
}

/// A subscriber listening on a specified prefix
pub struct Subscriber {
    id: usize,
    rx: Receiver<FutureReceiver<Event>>,
    home:
        Arc<RwLock<Vec<(usize, SyncSender<FutureReceiver<Event>>)>>>,
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
            let mut future_rx = self.rx.recv().ok()?;
            match future_rx.try_recv() {
                Ok(Some(event)) => return Some(event),
                Ok(None) => panic!("stale value unexpected"),
                Err(_cancelled) => continue,
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct Subscriptions {
    watched: RwLock<
        BTreeMap<
            Vec<u8>,
            Arc<
                RwLock<
                    Vec<(usize, SyncSender<FutureReceiver<Event>>)>,
                >,
            >,
        >,
    >,
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
            id: id,
            rx: rx,
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
