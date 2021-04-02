use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering::Relaxed},
        mpsc::{sync_channel, Receiver, SyncSender, TryRecvError},
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

#[cfg(not(feature = "testing"))]
use std::collections::HashMap as Map;

// we avoid HashMap while testing because
// it makes tests non-deterministic
#[cfg(feature = "testing")]
use std::collections::BTreeMap as Map;

use crate::*;

static ID_GEN: AtomicUsize = AtomicUsize::new(0);

/// An event that happened to a key that a subscriber is interested in.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Event {
    /// A new complete (key, value) pair
    Insert {
        /// The key that has been set
        key: IVec,
        /// The value that has been set
        value: IVec,
    },
    /// A deleted key
    Remove {
        /// The key that has been removed
        key: IVec,
    },
}

impl Event {
    /// Return the key associated with the `Event`
    pub fn key(&self) -> &IVec {
        match self {
            Event::Insert { key, .. } | Event::Remove { key } => key,
        }
    }
}

type Senders = Map<usize, (Option<Waker>, SyncSender<OneShot<Option<Event>>>)>;

/// A subscriber listening on a specified prefix
///
/// `Subscriber` implements both `Iterator<Item = Event>`
/// and `Future<Output=Option<Event>>`
///
/// # Examples
///
/// Synchronous, blocking subscriber:
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use sled::{Config, Event};
/// let config = Config::new().temporary(true);
///
/// let tree = config.open()?;
///
/// // watch all events by subscribing to the empty prefix
/// let mut subscriber = tree.watch_prefix(vec![]);
///
/// let tree_2 = tree.clone();
/// let thread = std::thread::spawn(move || {
///     tree.insert(vec![0], vec![1])
/// });
///
/// // `Subscription` implements `Iterator<Item=Event>`
/// for event in subscriber.take(1) {
///     match event {
///         Event::Insert{ key, value } => assert_eq!(key.as_ref(), &[0]),
///         Event::Remove {key } => {}
///     }
/// }
///
/// # thread.join().unwrap();
/// # Ok(())
/// # }
/// ```
/// Aynchronous, non-blocking subscriber:
///
/// `Subscription` implements `Future<Output=Option<Event>>`.
///
/// `while let Some(event) = (&mut subscriber).await { /* use it */ }`
pub struct Subscriber {
    id: usize,
    rx: Receiver<OneShot<Option<Event>>>,
    existing: Option<OneShot<Option<Event>>>,
    home: Arc<RwLock<Senders>>,
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        let mut w_senders = self.home.write();
        w_senders.remove(&self.id);
    }
}

impl Subscriber {
    /// Attempts to wait for a value on this `Subscriber`, returning
    /// an error if no event arrives within the provided `Duration`
    /// or if the backing `Db` shuts down.
    pub fn next_timeout(
        &mut self,
        mut timeout: Duration,
    ) -> std::result::Result<Event, std::sync::mpsc::RecvTimeoutError> {
        loop {
            let start = Instant::now();
            let mut future_rx = if let Some(future_rx) = self.existing.take() {
                future_rx
            } else {
                self.rx.recv_timeout(timeout)?
            };
            timeout =
                if let Some(timeout) = timeout.checked_sub(start.elapsed()) {
                    timeout
                } else {
                    Duration::from_nanos(0)
                };

            let start = Instant::now();
            match future_rx.wait_timeout(timeout) {
                Ok(Some(event)) => return Ok(event),
                Ok(None) => (),
                Err(timeout_error) => {
                    self.existing = Some(future_rx);
                    return Err(timeout_error);
                }
            }
            timeout =
                if let Some(timeout) = timeout.checked_sub(start.elapsed()) {
                    timeout
                } else {
                    Duration::from_nanos(0)
                };
        }
    }
}

impl Future for Subscriber {
    type Output = Option<Event>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        loop {
            let mut future_rx = if let Some(future_rx) = self.existing.take() {
                future_rx
            } else {
                match self.rx.try_recv() {
                    Ok(future_rx) => future_rx,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        return Poll::Ready(None)
                    }
                }
            };

            match Future::poll(Pin::new(&mut future_rx), cx) {
                Poll::Ready(Some(event)) => return Poll::Ready(event),
                Poll::Ready(None) => continue,
                Poll::Pending => {
                    self.existing = Some(future_rx);
                    return Poll::Pending;
                }
            }
        }
        let mut home = self.home.write();
        let entry = home.get_mut(&self.id).unwrap();
        entry.0 = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl Iterator for Subscriber {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        loop {
            let future_rx = self.rx.recv().ok()?;
            match future_rx.wait() {
                Some(Some(event)) => return Some(event),
                Some(None) => return None,
                None => continue,
            }
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct Subscribers {
    watched: RwLock<BTreeMap<Vec<u8>, Arc<RwLock<Senders>>>>,
    ever_used: AtomicBool,
}

impl Drop for Subscribers {
    fn drop(&mut self) {
        let watched = self.watched.read();

        for senders in watched.values() {
            let senders =
                std::mem::replace(&mut *senders.write(), Map::default());
            for (_, (waker, sender)) in senders {
                drop(sender);
                if let Some(waker) = waker {
                    waker.wake();
                }
            }
        }
    }
}

impl Subscribers {
    pub(crate) fn register(&self, prefix: &[u8]) -> Subscriber {
        self.ever_used.store(true, Relaxed);
        let r_mu = {
            let r_mu = self.watched.read();
            if r_mu.contains_key(prefix) {
                r_mu
            } else {
                drop(r_mu);
                let mut w_mu = self.watched.write();
                if !w_mu.contains_key(prefix) {
                    let old = w_mu.insert(
                        prefix.to_vec(),
                        Arc::new(RwLock::new(Map::default())),
                    );
                    assert!(old.is_none());
                }
                drop(w_mu);
                self.watched.read()
            }
        };

        let (tx, rx) = sync_channel(1024);

        let arc_senders = &r_mu[prefix];
        let mut w_senders = arc_senders.write();

        let id = ID_GEN.fetch_add(1, Relaxed);

        w_senders.insert(id, (None, tx));

        Subscriber { id, rx, existing: None, home: arc_senders.clone() }
    }

    pub(crate) fn reserve<R: AsRef<[u8]>>(
        &self,
        key: R,
    ) -> Option<ReservedBroadcast> {
        if !self.ever_used.load(Relaxed) {
            return None;
        }

        let r_mu = self.watched.read();
        let prefixes = r_mu.iter().filter(|(k, _)| key.as_ref().starts_with(k));

        let mut subscribers = vec![];

        for (_, subs_rwl) in prefixes {
            let subs = subs_rwl.read();

            for (_id, (waker, sender)) in subs.iter() {
                let (tx, rx) = OneShot::pair();
                if sender.send(rx).is_err() {
                    continue;
                }
                subscribers.push((waker.clone(), tx));
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
    subscribers: Vec<(Option<Waker>, OneShotFiller<Option<Event>>)>,
}

impl ReservedBroadcast {
    pub fn complete(self, event: &Event) {
        let iter = self.subscribers.into_iter();

        for (waker, tx) in iter {
            tx.fill(Some(event.clone()));
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

#[test]
fn basic_subscriber() {
    let subs = Subscribers::default();

    let mut s2 = subs.register(&[0]);
    let mut s3 = subs.register(&[0, 1]);
    let mut s4 = subs.register(&[1, 2]);

    let r1 = subs.reserve(b"awft");
    assert!(r1.is_none());

    let mut s1 = subs.register(&[]);

    let k2: IVec = vec![].into();
    let r2 = subs.reserve(&k2).unwrap();
    r2.complete(&Event::Insert { key: k2.clone(), value: k2.clone() });

    let k3: IVec = vec![0].into();
    let r3 = subs.reserve(&k3).unwrap();
    r3.complete(&Event::Insert { key: k3.clone(), value: k3.clone() });

    let k4: IVec = vec![0, 1].into();
    let r4 = subs.reserve(&k4).unwrap();
    r4.complete(&Event::Remove { key: k4.clone() });

    let k5: IVec = vec![0, 1, 2].into();
    let r5 = subs.reserve(&k5).unwrap();
    r5.complete(&Event::Insert { key: k5.clone(), value: k5.clone() });

    let k6: IVec = vec![1, 1, 2].into();
    let r6 = subs.reserve(&k6).unwrap();
    r6.complete(&Event::Remove { key: k6.clone() });

    let k7: IVec = vec![1, 1, 2].into();
    let r7 = subs.reserve(&k7).unwrap();
    drop(r7);

    let k8: IVec = vec![1, 2, 2].into();
    let r8 = subs.reserve(&k8).unwrap();
    r8.complete(&Event::Insert { key: k8.clone(), value: k8.clone() });

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
