use {
    std::{
        any::TypeId,
        ptr::NonNull,
        sync::Arc,
        ffi::c_void,
    },
    super::{Error, Result},
};

trait FnBox {
    fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<F>) {
        (*self)()
    }
}

pub(crate) struct DynamicArc {
    ptr: NonNull<c_void>,
    type_id: TypeId,
    destructor: Option<Box<dyn FnBox>>,
}

impl DynamicArc {
    pub fn new<T: 'static>(item: Arc<T>) -> DynamicArc {
        let ptr: *const T = Arc::into_raw(item);
        let non_null = NonNull::new(ptr as *mut c_void).unwrap();

        let type_id = TypeId::of::<T>();

        let destructor: Option<Box<dyn FnBox>> = Some(Box::new(move || {
            let arc: Arc<T> = unsafe { Arc::from_raw(ptr) };
            drop(arc)
        }));

        DynamicArc {
            ptr: non_null,
            type_id,
            destructor,
        }
    }

    pub fn maybe_clone<T: 'static>(&self) -> Result<Arc<T>> {
        let type_id = TypeId::of::<T>();

        if type_id != self.type_id {
            Err(Error::IncorrectType)
        } else {
            let ptr = self.ptr.as_ptr() as *const T;
            let arc: Arc<T> = unsafe { Arc::from_raw(ptr) };
            let ret = Ok(arc.clone());
            std::mem::forget(arc);
            ret
        }
    }
}

impl Drop for DynamicArc {
    fn drop(&mut self) {
        if let Some(destructor) = self.destructor.take() {
            destructor.call_box()
        }
    }
}
