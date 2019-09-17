use std::sync::{atomic::AtomicUsize, Arc};

use super::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Values {
    inner: Vec<u8>,
}

impl Values {
    pub(crate) fn size_in_bytes(&self) -> usize {
        0
    }
}
