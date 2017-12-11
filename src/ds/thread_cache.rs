use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use std::thread::{self, ThreadId};

#[derive(Debug)]
pub struct ThreadCache<T> {
    inner: Arc<RwLock<HashMap<ThreadId, Rc<RefCell<T>>>>>,
}

impl<T> Clone for ThreadCache<T> {
    fn clone(&self) -> ThreadCache<T> {
        ThreadCache {
            inner: Arc::clone(&self.inner),
        }
    }
}

unsafe impl<T> Send for ThreadCache<T> {}
unsafe impl<T> Sync for ThreadCache<T> {}

impl<T> Default for ThreadCache<T> {
    fn default() -> ThreadCache<T> {
        ThreadCache {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl<T> ThreadCache<T> {
    pub fn get_or_else<F>(&self, mut f: F) -> Rc<RefCell<T>>
        where F: FnMut() -> T
    {
        let id = thread::current().id();

        {
            let map = self.inner.read().unwrap();
            if map.contains_key(&id) {
                return Rc::clone(map.get(&id).unwrap());
            }
        }

        let t = Rc::new(RefCell::new(f()));
        let mut map = self.inner.write().unwrap();
        map.insert(id, t);
        Rc::clone(map.get(&id).unwrap())
    }
}
