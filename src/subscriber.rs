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

use crate::*;

static ID_GEN: AtomicUsize = AtomicUsize::new(0);

/// An event that happened to a key that a subscriber is interested in.
#[derive(Debug, Clone)]
pub struct Event {
    /// A map of batches for each tree written to in a transaction,
    /// only one of which will be the one subscribed to.
    pub(crate) batches: Arc<[(Tree, Batch)]>,
}

impl Event {
    pub(crate) fn single_update(
        tree: Tree,
        key: IVec,
        value: Option<IVec>,
    ) -> Event {
        Event::single_batch(
            tree,
            Batch { writes: vec![(key, value)].into_iter().collect() },
        )
    }

    pub(crate) fn single_batch(tree: Tree, batch: Batch) -> Event {
        Event::from_batches(vec![(tree, batch)])
    }

    pub(crate) fn from_batches(batches: Vec<(Tree, Batch)>) -> Event {
        Event { batches: Arc::from(batches.into_boxed_slice()) }
    }

    /// Iterate over each Tree, key, and optional value in this `Event`
    pub fn iter<'a>(
        &'a self,
    ) -> Box<dyn 'a + Iterator<Item = (&'a Tree, &'a IVec, &'a Option<IVec>)>>
    {
        self.into_iter()
    }
}

impl<'a> IntoIterator for &'a Event {
    type Item = (&'a Tree, &'a IVec, &'a Option<IVec>);
    type IntoIter = Box<dyn 'a + Iterator<Item = Self::Item>>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.batches.iter().flat_map(|(ref tree, ref batch)| {
            batch.writes.iter().map(move |(k, v_opt)| (tree, k, v_opt))
        }))
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
///     // Events occur due to single key operations,
///     // batches, or transactions. The tree is included
///     // so that you may perform a new transaction or
///     // operation in response to the event.
///     for (tree, key, value_opt) in &event {
///         if let Some(value) = value_opt {
///             // key `key` was set to value `value`
///         } else {
///             // key `key` was removed
///         }
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
        &self,
        mut timeout: Duration,
    ) -> std::result::Result<Event, std::sync::mpsc::RecvTimeoutError> {
        loop {
            let start = Instant::now();
            let future_rx = self.rx.recv_timeout(timeout)?;
            timeout =
                if let Some(timeout) = timeout.checked_sub(start.elapsed()) {
                    timeout
                } else {
                    Duration::from_nanos(0)
                };

            let start = Instant::now();
            if let Some(event) = future_rx.wait_timeout(timeout)? {
                return Ok(event);
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

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.rx.try_recv() {
                Ok(mut future_rx) => {
                    #[allow(unsafe_code)]
                    let future_rx =
                        unsafe { std::pin::Pin::new_unchecked(&mut future_rx) };

                    match Future::poll(future_rx, cx) {
                        Poll::Ready(Some(event)) => {
                            return Poll::Ready(event);
                        }
                        Poll::Ready(None) => {
                            continue;
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Poll::Ready(None),
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
            let senders = std::mem::take(&mut *senders.write());
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

        Subscriber { id, rx, home: arc_senders.clone() }
    }

    pub(crate) fn reserve_batch(
        &self,
        batch: &Batch,
    ) -> Option<ReservedBroadcast> {
        if !self.ever_used.load(Relaxed) {
            return None;
        }

        let r_mu = self.watched.read();

        let mut skip_indices = std::collections::HashSet::new();
        let mut subscribers = vec![];

        for key in batch.writes.keys() {
            for (idx, (prefix, subs_rwl)) in r_mu.iter().enumerate() {
                if key.starts_with(prefix) && !skip_indices.contains(&idx) {
                    skip_indices.insert(idx);
                    let subs = subs_rwl.read();

                    for (_id, (waker, sender)) in subs.iter() {
                        let (tx, rx) = OneShot::pair();
                        if sender.send(rx).is_err() {
                            continue;
                        }
                        subscribers.push((waker.clone(), tx));
                    }
                }
            }
        }

        if subscribers.is_empty() {
            None
        } else {
            Some(ReservedBroadcast { subscribers })
        }
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
