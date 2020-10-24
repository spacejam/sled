#![allow(unsafe_code)]
/// We use this because we never use the weak count on the std
/// `Arc`, but we use a LOT of `Arc`'s, so the extra 8 bytes
/// turn into a huge overhead.
use std::{
    alloc::{alloc, dealloc, Layout},
    convert::TryFrom,
    fmt::{self, Debug},
    mem,
    ops::Deref,
    ptr,
    sync::atomic::{AtomicUsize, Ordering},
};

// we make this repr(C) because we do a raw
// write to the beginning where we expect
// the rc to be.
#[repr(C)]
struct ArcInner<T: ?Sized> {
    rc: AtomicUsize,
    inner: T,
}

pub struct Arc<T: ?Sized> {
    ptr: *mut ArcInner<T>,
}

unsafe impl<T: Send + Sync + ?Sized> Send for Arc<T> {}
unsafe impl<T: Send + Sync + ?Sized> Sync for Arc<T> {}

impl<T: Debug + ?Sized> Debug for Arc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        Debug::fmt(&**self, f)
    }
}

impl<T> Arc<T> {
    pub fn new(inner: T) -> Arc<T> {
        let bx = Box::new(ArcInner { inner, rc: AtomicUsize::new(1) });
        let ptr = Box::into_raw(bx);
        Arc { ptr }
    }

    // See std::sync::arc::Arc::copy_from_slice,
    // "Unsafe because the caller must either take ownership or bind `T: Copy`"
    unsafe fn copy_from_slice(s: &[T]) -> Arc<[T]> {
        let align =
            std::cmp::max(mem::align_of::<T>(), mem::align_of::<AtomicUsize>());

        let rc_width = std::cmp::max(align, mem::size_of::<AtomicUsize>());
        let data_width = mem::size_of::<T>().checked_mul(s.len()).unwrap();

        let size_unpadded = rc_width.checked_add(data_width).unwrap();
        // Pad size out to alignment
        let size_padded = (size_unpadded + align - 1) & !(align - 1);

        let layout = Layout::from_size_align(size_padded, align).unwrap();

        let ptr = alloc(layout);

        assert!(!ptr.is_null(), "failed to allocate Arc");
        #[allow(clippy::cast_ptr_alignment)]
        ptr::write(ptr as _, AtomicUsize::new(1));

        let data_ptr = ptr.add(rc_width);
        ptr::copy_nonoverlapping(s.as_ptr(), data_ptr as _, s.len());

        let fat_ptr: *const ArcInner<[T]> = Arc::fatten(ptr, s.len());

        Arc { ptr: fat_ptr as *mut _ }
    }

    /// <https://users.rust-lang.org/t/construct-fat-pointer-to-struct/29198/9>
    #[allow(trivial_casts)]
    fn fatten(data: *const u8, len: usize) -> *const ArcInner<[T]> {
        // Requirements of slice::from_raw_parts.
        assert!(!data.is_null());
        assert!(isize::try_from(len).is_ok());

        let slice =
            unsafe { core::slice::from_raw_parts(data as *const (), len) };
        slice as *const [()] as *const _
    }

    pub fn into_raw(arc: Arc<T>) -> *const T {
        let ptr = unsafe { &(*arc.ptr).inner };
        #[allow(clippy::mem_forget)]
        mem::forget(arc);
        ptr
    }

    pub unsafe fn from_raw(ptr: *const T) -> Arc<T> {
        let align =
            std::cmp::max(mem::align_of::<T>(), mem::align_of::<AtomicUsize>());

        let rc_width = std::cmp::max(align, mem::size_of::<AtomicUsize>());

        let sub_ptr = (ptr as *const u8).sub(rc_width) as *mut ArcInner<T>;

        Arc { ptr: sub_ptr }
    }
}

impl<T: ?Sized> Arc<T> {
    pub fn strong_count(arc: &Arc<T>) -> usize {
        unsafe { (*arc.ptr).rc.load(Ordering::Acquire) }
    }

    pub fn get_mut(arc: &mut Arc<T>) -> Option<&mut T> {
        if Arc::strong_count(arc) == 1 {
            Some(unsafe { &mut arc.ptr.as_mut().unwrap().inner })
        } else {
            None
        }
    }
}

impl<T: ?Sized + Clone> Arc<T> {
    pub fn make_mut(arc: &mut Arc<T>) -> &mut T {
        if Arc::strong_count(arc) != 1 {
            *arc = Arc::new((**arc).clone());
            assert_eq!(Arc::strong_count(arc), 1);
        }
        Arc::get_mut(arc).unwrap()
    }
}

impl<T: Default> Default for Arc<T> {
    fn default() -> Arc<T> {
        Arc::new(T::default())
    }
}

impl<T: ?Sized> Clone for Arc<T> {
    fn clone(&self) -> Arc<T> {
        // safe to use Relaxed ordering below because
        // of the required synchronization for passing
        // any objects to another thread.
        let last_count =
            unsafe { (*self.ptr).rc.fetch_add(1, Ordering::Relaxed) };

        if last_count == usize::max_value() {
            std::process::abort();
        }

        Arc { ptr: self.ptr }
    }
}

impl<T: ?Sized> Drop for Arc<T> {
    fn drop(&mut self) {
        unsafe {
            let rc = (*self.ptr).rc.fetch_sub(1, Ordering::Release) - 1;
            if rc == 0 {
                std::sync::atomic::fence(Ordering::Acquire);
                Box::from_raw(self.ptr);
            }
        }
    }
}

impl<T: Copy> From<&[T]> for Arc<[T]> {
    #[inline]
    fn from(s: &[T]) -> Arc<[T]> {
        unsafe { Arc::copy_from_slice(s) }
    }
}

#[allow(clippy::fallible_impl_from)]
impl<T> From<Box<[T]>> for Arc<[T]> {
    #[inline]
    fn from(b: Box<[T]>) -> Arc<[T]> {
        let len = b.len();
        unsafe {
            let src = Box::into_raw(b);
            let value_layout = Layout::for_value(&*src);
            let align = std::cmp::max(
                value_layout.align(),
                mem::align_of::<AtomicUsize>(),
            );
            let rc_width = std::cmp::max(align, mem::size_of::<AtomicUsize>());
            let unpadded_size =
                rc_width.checked_add(value_layout.size()).unwrap();
            // pad the total `Arc` allocation size to the alignment of
            // `max(value, AtomicUsize)`
            let size = (unpadded_size + align - 1) & !(align - 1);
            let dst_layout = Layout::from_size_align(size, align).unwrap();
            let dst = alloc(dst_layout);
            assert!(!dst.is_null(), "failed to allocate Arc");

            #[allow(clippy::cast_ptr_alignment)]
            ptr::write(dst as _, AtomicUsize::new(1));
            let data_ptr = dst.add(rc_width);
            ptr::copy_nonoverlapping(
                src as *const u8,
                data_ptr,
                value_layout.size(),
            );

            // free the old box memory without running Drop
            if value_layout.size() != 0 {
                dealloc(src as *mut u8, value_layout);
            }

            let fat_ptr: *const ArcInner<[T]> = Arc::fatten(dst, len);

            Arc { ptr: fat_ptr as *mut _ }
        }
    }
}

#[test]
fn boxed_slice_to_arc_slice() {
    let box1: Box<[u8]> = Box::new([1, 2, 3]);
    let arc1: Arc<[u8]> = box1.into();
    assert_eq!(&*arc1, &*vec![1, 2, 3]);
    let box2: Box<[u8]> = Box::new([]);
    let arc2: Arc<[u8]> = box2.into();
    assert_eq!(&*arc2, &*vec![]);
    let box3: Box<[u64]> = Box::new([1, 2, 3]);
    let arc3: Arc<[u64]> = box3.into();
    assert_eq!(&*arc3, &*vec![1, 2, 3]);
}

impl<T> From<Vec<T>> for Arc<[T]> {
    #[inline]
    fn from(mut v: Vec<T>) -> Arc<[T]> {
        unsafe {
            let arc = Arc::copy_from_slice(&v);

            // Allow the Vec to free its memory, but not destroy its contents
            v.set_len(0);

            arc
        }
    }
}

impl<T: ?Sized> Deref for Arc<T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &(*self.ptr).inner }
    }
}

impl<T: ?Sized> std::borrow::Borrow<T> for Arc<T> {
    fn borrow(&self) -> &T {
        &**self
    }
}

impl<T: ?Sized> AsRef<T> for Arc<T> {
    fn as_ref(&self) -> &T {
        &**self
    }
}
