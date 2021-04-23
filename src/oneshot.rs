use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex};

use crate::Arc;

#[derive(Debug)]
struct OneShotState<T> {
    filled: bool,
    fused: bool,
    item: Option<T>,
    waker: Option<Waker>,
}

impl<T> Default for OneShotState<T> {
    fn default() -> OneShotState<T> {
        OneShotState { filled: false, fused: false, item: None, waker: None }
    }
}

/// A Future value which may or may not be filled
#[derive(Debug)]
pub struct OneShot<T> {
    mu: Arc<Mutex<OneShotState<T>>>,
    cv: Arc<Condvar>,
}

/// The completer side of the Future
pub struct OneShotFiller<T> {
    mu: Arc<Mutex<OneShotState<T>>>,
    cv: Arc<Condvar>,
}

impl<T> OneShot<T> {
    /// Create a new `OneShotFiller` and the `OneShot`
    /// that will be filled by its completion.
    pub fn pair() -> (OneShotFiller<T>, Self) {
        let mu = Arc::new(Mutex::new(OneShotState::default()));
        let cv = Arc::new(Condvar::new());
        let future = Self { mu: mu.clone(), cv: cv.clone() };
        let filler = OneShotFiller { mu, cv };

        (filler, future)
    }

    /// Block on the `OneShot`'s completion
    /// or dropping of the `OneShotFiller`
    pub fn wait(self) -> Option<T> {
        let mut inner = self.mu.lock();
        while !inner.filled {
            self.cv.wait(&mut inner);
        }
        inner.item.take()
    }

    /// Block on the `OneShot`'s completion
    /// or dropping of the `OneShotFiller`,
    /// returning an error if not filled
    /// before a given timeout or if the
    /// system shuts down before then.
    ///
    /// Upon a successful receive, the
    /// oneshot should be dropped, as it
    /// will never yield that value again.
    pub fn wait_timeout(
        &mut self,
        mut timeout: Duration,
    ) -> Result<T, std::sync::mpsc::RecvTimeoutError> {
        let mut inner = self.mu.lock();
        while !inner.filled {
            let start = Instant::now();
            let res = self.cv.wait_for(&mut inner, timeout);
            if res.timed_out() {
                return Err(std::sync::mpsc::RecvTimeoutError::Disconnected);
            }
            timeout =
                if let Some(timeout) = timeout.checked_sub(start.elapsed()) {
                    timeout
                } else {
                    Duration::from_nanos(0)
                };
        }
        if let Some(item) = inner.item.take() {
            Ok(item)
        } else {
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
        }
    }
}

impl<T> Future for OneShot<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.mu.lock();
        if state.fused {
            return Poll::Pending;
        }
        if state.filled {
            state.fused = true;
            Poll::Ready(state.item.take())
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl<T> OneShotFiller<T> {
    /// Complete the `OneShot`
    pub fn fill(self, inner: T) {
        let mut state = self.mu.lock();

        if let Some(waker) = state.waker.take() {
            waker.wake();
        }

        state.filled = true;
        state.item = Some(inner);

        // having held the mutex makes this linearized
        // with the notify below.
        drop(state);

        let _notified = self.cv.notify_all();
    }
}

impl<T> Drop for OneShotFiller<T> {
    fn drop(&mut self) {
        let mut state = self.mu.lock();

        if state.filled {
            return;
        }

        if let Some(waker) = state.waker.take() {
            waker.wake();
        }

        state.filled = true;

        // having held the mutex makes this linearized
        // with the notify below.
        drop(state);

        let _notified = self.cv.notify_all();
    }
}
