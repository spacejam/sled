use std::collections::BTreeMap;

use super::*;

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub(crate) struct Meta {
    roots: BTreeMap<Vec<u8>, usize>,
}

impl Meta {
    pub(crate) fn root(&self, table: &[u8]) -> Option<usize> {
        self.roots.get(table).map(|r| *r)
    }

    pub(crate) fn set_root(&mut self, name: Vec<u8>, root: usize) {
        self.roots.insert(name, root);
    }
}
