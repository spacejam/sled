use super::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: PageID,
    pub data: Data,
    pub next: Option<PageID>,
    pub lo: Bound,
    pub hi: Bound,
}

impl Node {
    pub fn apply(&mut self, frag: &Frag) {
        use self::Frag::*;

        match *frag {
            Set(ref k, ref v) => {
                let decoded_k = prefix_decode(self.lo.inner(), k);
                if Bound::Inclusive(decoded_k) < self.hi {
                    self.set_leaf(k.clone(), v.clone());
                } else {
                    panic!("tried to consolidate set at key <= hi")
                }
            }
            ChildSplit(ref child_split) => {
                self.child_split(child_split);
            }
            ParentSplit(ref parent_split) => {
                self.parent_split(parent_split);
            }
            Del(ref k) => {
                let decoded_k = prefix_decode(self.lo.inner(), k);
                if Bound::Inclusive(decoded_k) < self.hi {
                    self.del_leaf(k);
                } else {
                    panic!("tried to consolidate del at key <= hi")
                }
            }
            Base(_, _) => panic!("encountered base page in middle of chain"),
        }
    }

    pub fn set_leaf(&mut self, key: Key, val: Value) {
        if let Data::Leaf(ref mut records) = self.data {
            let search = records.binary_search_by(
                |&(ref k, ref _v)| prefix_cmp(k, &*key),
            );
            if let Ok(idx) = search {
                records.push((key, val));
                records.swap_remove(idx);
            } else {
                records.push((key, val));
                records.sort_unstable_by(|a, b| prefix_cmp(&*a.0, &*b.0));
            }
        } else {
            panic!("tried to Set a value to an index");
        }
    }

    pub fn child_split(&mut self, cs: &ChildSplit) {
        self.data.drop_gte(&cs.at, self.lo.inner());
        self.hi = Bound::Exclusive(cs.at.inner().to_vec());
        self.next = Some(cs.to);
    }

    pub fn parent_split(&mut self, ps: &ParentSplit) {
        if let Data::Index(ref mut ptrs) = self.data {
            let encoded_sep = prefix_encode(self.lo.inner(), ps.at.inner());
            ptrs.push((encoded_sep, ps.to));
            ptrs.sort_unstable_by(|a, b| prefix_cmp(&*a.0, &*b.0));
        } else {
            panic!("tried to attach a ParentSplit to a Leaf chain");
        }
    }

    pub fn del_leaf(&mut self, key: KeyRef) {
        if let Data::Leaf(ref mut records) = self.data {
            let search = records.binary_search_by(
                |&(ref k, ref _v)| prefix_cmp(k, &*key),
            );
            if let Ok(idx) = search {
                records.remove(idx);
            }
        } else {
            panic!("tried to attach a Del to an Index chain");
        }
    }

    pub fn should_split(&self, fanout: usize) -> bool {
        self.data.len() > fanout
    }

    pub fn split(&self, id: PageID) -> Node {
        let (split, right_data) = self.data.split(self.lo.inner());
        Node {
            id: id,
            data: right_data,
            next: self.next,
            lo: Bound::Inclusive(split),
            hi: self.hi.clone(),
        }
    }
}
