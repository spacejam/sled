use std::collections::BTreeMap;

use super::*;

/// A simple map that can be used to store metadata
/// for the pagecache tenant.
#[derive(Clone, Debug, Eq, PartialEq, Default, Serialize, Deserialize)]
pub struct Meta {
    inner: BTreeMap<Vec<u8>, PageId>,
}

impl Meta {
    /// Retrieve the PageId associated with an identifier
    pub fn get_root(&self, table: &[u8]) -> Option<PageId> {
        self.inner.get(table).cloned()
    }

    /// Set the PageId associated with an identifier
    pub fn set_root(&mut self, name: Vec<u8>, pid: PageId) {
        self.inner.insert(name, pid);
    }

    /// Remove the page mapping for a given identifier
    pub fn del_root(&mut self, name: &[u8]) -> Option<PageId> {
        self.inner.remove(name)
    }

    /// Return the current rooted tenants in Meta
    pub fn tenants(&self) -> BTreeMap<Vec<u8>, PageId> {
        self.inner.clone()
    }

    pub(crate) fn size_in_bytes(&self) -> u64 {
        self.inner
            .iter()
            .map(|(k, _pid)| {
                k.len() as u64 + std::mem::size_of::<PageId>() as u64
            })
            .sum()
    }
}
