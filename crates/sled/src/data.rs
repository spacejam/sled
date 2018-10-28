use std::fmt::Debug;
use std::mem::size_of;

use super::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum Data {
    Index(Vec<(Key, PageId)>),
    Leaf(Vec<(Key, Value)>),
}

impl Data {
    #[inline]
    pub(crate) fn size_in_bytes(&self) -> usize {
        let self_sz = size_of::<Bound>();

        let inner_sz = match self {
            Data::Index(ref v) => {
                let mut sz = 0;
                for (k, _p) in v.iter() {
                    sz += k.len()
                        + size_of::<Key>()
                        + size_of::<PageId>();
                }
                sz + size_of::<Vec<(Key, PageId)>>()
            }
            Data::Leaf(ref v) => {
                let mut sz = 0;
                for (k, value) in v.iter() {
                    sz += k.len()
                        + size_of::<Key>()
                        + value.len()
                        + size_of::<Value>();
                }
                sz + size_of::<Vec<(Key, Value)>>()
            }
        };

        self_sz + inner_sz
    }

    pub(crate) fn len(&self) -> usize {
        match *self {
            Data::Index(ref ptrs) => ptrs.len(),
            Data::Leaf(ref items) => items.len(),
        }
    }

    pub(crate) fn split(&self, lhs_prefix: &[u8]) -> (Key, Data) {
        fn split_inner<T>(
            xs: &[(Key, T)],
            lhs_prefix: &[u8],
        ) -> (Key, Vec<(Key, T)>)
        where
            T: Clone + Debug + Ord,
        {
            let mut decoded_xs: Vec<_> = xs
                .iter()
                .map(|&(ref k, ref v)| {
                    let decoded_k = prefix_decode(lhs_prefix, &*k);
                    (decoded_k, v.clone())
                }).collect();
            decoded_xs.sort();

            let (_lhs, rhs) =
                decoded_xs.split_at(decoded_xs.len() / 2 + 1);
            let split = rhs
                .first()
                .expect("rhs should contain at least one element")
                .0
                .clone();
            let rhs_data: Vec<_> = rhs
                .iter()
                .map(|&(ref k, ref v)| {
                    let new_k = prefix_encode(&*split, k);
                    (new_k, v.clone())
                }).collect();

            (split, rhs_data)
        }

        match *self {
            Data::Index(ref ptrs) => {
                let (split, rhs) = split_inner(ptrs, lhs_prefix);
                (split, Data::Index(rhs))
            }
            Data::Leaf(ref items) => {
                let (split, rhs) = split_inner(items, lhs_prefix);
                (split, Data::Leaf(rhs))
            }
        }
    }

    pub(crate) fn drop_gte(&mut self, at: &Bound, prefix: &[u8]) {
        let bound = at.inner();
        match *self {
            Data::Index(ref mut ptrs) => {
                ptrs.retain(|&(ref k, _)| {
                    let decoded_k = prefix_decode(prefix, &*k);
                    &*decoded_k < bound
                })
            }
            Data::Leaf(ref mut items) => {
                items.retain(|&(ref k, _)| {
                    let decoded_k = prefix_decode(prefix, &*k);
                    &*decoded_k < bound
                })
            }
        }
    }

    pub(crate) fn leaf_ref(&self) -> Option<&Vec<(Key, Value)>> {
        match *self {
            Data::Index(_) => None,
            Data::Leaf(ref items) => Some(items),
        }
    }
}
