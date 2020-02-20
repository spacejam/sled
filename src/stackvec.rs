use std::{convert::TryFrom, mem::MaybeUninit};

use crate::pagecache::constants::PAGE_CONSOLIDATION_THRESHOLD;

#[derive(Debug, Clone, Copy)]
pub(crate) struct StackVec<T: Copy + Sized> {
    items: [MaybeUninit<T>; PAGE_CONSOLIDATION_THRESHOLD],
    len: u8,
}

impl<T: Copy> Default for StackVec<T> {
    fn default() -> StackVec<T> {
        StackVec {
            items: [MaybeUninit::uninit(); PAGE_CONSOLIDATION_THRESHOLD],
            len: 0,
        }
    }
}

impl<T: Copy + Sized> std::ops::Deref for StackVec<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        #[allow(unsafe_code)]
        unsafe {
            let ptr: *const T = self.items.as_ptr() as *const T;
            std::slice::from_raw_parts(ptr, self.len as usize)
        }
    }
}

impl<T: Copy + Sized> std::ops::DerefMut for StackVec<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        #[allow(unsafe_code)]
        unsafe {
            let ptr: *mut T = self.items.as_mut_ptr() as *mut T;
            std::slice::from_raw_parts_mut(ptr, self.len as usize)
        }
    }
}

impl<T: Copy + Sized> StackVec<T> {
    pub(crate) fn single(item: T) -> StackVec<T> {
        let mut sv = Self::default();
        sv.push(item);
        sv
    }

    pub(crate) fn extend_from_slice(&mut self, other: &[T]) {
        assert!(
            self.len as usize + other.len() <= PAGE_CONSOLIDATION_THRESHOLD,
            "tried to extend_from_slice into StackVec past max capacity"
        );

        #[allow(unsafe_code)]
        unsafe {
            let src = other.as_ptr();
            let dst = self.items.as_mut_ptr().add(self.len as usize) as *mut T;
            std::ptr::copy(src, dst, other.len());
        }

        self.len += u8::try_from(other.len()).unwrap();
    }

    pub(crate) fn _insert(&mut self, idx: usize, item: T) {
        assert_ne!(
            self.len as usize, PAGE_CONSOLIDATION_THRESHOLD,
            "tried to insert into StackVec already at max capacity"
        );

        assert!(idx <= self.len());

        if idx != self.len as usize {
            let items = self.len as usize - idx;

            #[allow(unsafe_code)]
            unsafe {
                let src = self.items.as_ptr().add(idx);
                let dst = self.items.as_mut_ptr().add(idx + 1);
                std::ptr::copy(src, dst, items);
            }
        }

        self.len += 1;
        self.items[idx] = MaybeUninit::new(item);
    }

    pub(crate) fn push(&mut self, item: T) {
        assert_ne!(
            self.len as usize, PAGE_CONSOLIDATION_THRESHOLD,
            "tried to push into StackVec already at max capacity"
        );
        self.items[self.len as usize] = MaybeUninit::new(item);
        self.len += 1;
    }

    pub(crate) fn _pop(&mut self) -> Option<T> {
        if self.len > 0 {
            self.len -= 1;

            #[allow(unsafe_code)]
            unsafe {
                Some(self.items[self.len as usize].as_ptr().read())
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod qc {
    use quickcheck::{Arbitrary, Gen};
    use rand::Rng;

    use super::{StackVec, PAGE_CONSOLIDATION_THRESHOLD};

    #[derive(Clone, Debug)]
    enum Op {
        Extend(Vec<u8>),
        Insert(usize, u8),
        Push(u8),
        Pop,
    }

    impl Arbitrary for Op {
        fn arbitrary<G: Gen>(g: &mut G) -> Op {
            match g.gen_range(0, 4) {
                0 => {
                    let len = g.gen_range(0, PAGE_CONSOLIDATION_THRESHOLD);
                    let items = vec![g.gen(); len];
                    Op::Extend(items)
                }
                1 => Op::Insert(
                    g.gen_range(0, PAGE_CONSOLIDATION_THRESHOLD),
                    g.gen(),
                ),
                2 => Op::Push(g.gen()),
                3 => Op::Pop,
                _ => unreachable!(),
            }
        }
    }

    quickcheck::quickcheck! {
        fn qc_stackvec(ops: Vec<Op>) -> bool {
            let mut sv = StackVec::default();
            let mut v = vec![];

            for op in ops {
                match op {
                    Op::Extend(items) => {
                        if items.len() + v.len() < PAGE_CONSOLIDATION_THRESHOLD {
                            sv.extend_from_slice(&items);
                            v.extend_from_slice(&items);
                        }
                    }
                    Op::Insert(at, item) => {
                        if at <= v.len() && v.len() < PAGE_CONSOLIDATION_THRESHOLD {
                            sv._insert(at, item);
                            v.insert(at, item);
                        }
                    }
                    Op::Push(item) => {
                        if v.len() < PAGE_CONSOLIDATION_THRESHOLD {
                            sv.push(item);
                            v.push(item);
                        }
                    }
                    Op::Pop => {
                        assert_eq!(sv._pop(), v.pop());
                    }
                }
                assert_eq!(&*sv, &*v);
            }

            true
        }
    }
}
