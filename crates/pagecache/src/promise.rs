use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

/// A Future value which may or may not be filled
#[derive(Debug)]
pub struct Promise<T> {
    mu: Arc<Mutex<(bool, Option<T>)>>,
    cv: Arc<Condvar>,
}

/// The completer side of the Future
pub struct PromiseFiller<T> {
    mu: Arc<Mutex<(bool, Option<T>)>>,
    cv: Arc<Condvar>,
    completed: bool,
}

impl<T> Promise<T> {
    /// Create a new PromiseFiller and the Promise
    /// that will be filled by its completion.
    pub fn pair() -> (PromiseFiller<T>, Promise<T>) {
        let mu = Arc::new(Mutex::new((false, None)));
        let cv = Arc::new(Condvar::new());
        let future = Self {
            mu: mu.clone(),
            cv: cv.clone(),
        };
        let filler = PromiseFiller {
            mu,
            cv,
            completed: false,
        };

        (filler, future)
    }

    /// Block on the Promise's completion
    /// or dropping of the PromiseFiller
    pub fn wait(self) -> Option<T> {
        let mut inner = self.mu.lock();
        while !inner.0 {
            self.cv.wait(&mut inner);
        }
        inner.1.take()
    }

    /// Block on the Promise's completion
    /// or dropping of the PromiseFiller.
    ///
    /// # Panics
    /// panics if the PromiseFiller is dropped
    /// without completing the promise.
    pub fn unwrap(self) -> T {
        self.wait().unwrap()
    }
}

impl<T> PromiseFiller<T> {
    /// Complete the Promise
    pub fn fill(mut self, inner: T) {
        let mut mu = self.mu.lock();
        *mu = (true, Some(inner));
        self.cv.notify_all();
        self.completed = true;
    }
}

impl<T> Drop for PromiseFiller<T> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut mu = self.mu.lock();
        *mu = (true, None);
        self.cv.notify_all();
    }
}
