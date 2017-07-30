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
    pub fn apply(&mut self, frag: Frag) {
        use self::Frag::*;

        match frag {
            Set(ref k, ref v) => {
                if Bound::Inc(k.clone()) < self.hi {
                    self.set_leaf(k.clone(), v.clone());
                } else {
                    panic!("tried to consolidate set at key <= hi")
                }
            }
            ChildSplit(child_split) => {
                self.child_split(&child_split);
            }
            ParentSplit(parent_split) => {
                self.parent_split(&parent_split);
            }
            Del(ref k) => {
                if Bound::Inc(k.clone()) < self.hi {
                    self.del_leaf(k);
                } else {
                    panic!("tried to consolidate del at key <= hi")
                }
            }
            Base(_) => panic!("encountered base page in middle of chain"),
        }
    }

    pub fn set_leaf(&mut self, key: Key, val: Value) {
        if let Data::Leaf(ref mut records) = self.data {
            let search = records.binary_search_by(|&(ref k, ref _v)| (**k).cmp(&*key));
            if let Ok(idx) = search {
                records.push((key, val));
                records.swap_remove(idx);
            } else {
                records.push((key, val));
                records.sort();
            }
        } else {
            panic!("tried to Set a value to an index");
        }
    }

    pub fn child_split(&mut self, cs: &ChildSplit) {
        self.data.drop_gte(&cs.at);
        self.hi = Bound::Non(cs.at.inner().unwrap());
        self.next = Some(cs.to);
    }

    pub fn parent_split(&mut self, ps: &ParentSplit) {
        // println!("splitting parent: {:?}\nwith {:?}", self, ps);
        if let Data::Index(ref mut ptrs) = self.data {
            ptrs.push((ps.at.inner().unwrap(), ps.to));
            ptrs.sort_by(|a, b| a.0.cmp(&b.0));
        } else {
            panic!("tried to attach a ParentSplit to a Leaf chain");
        }
    }

    pub fn del_leaf(&mut self, key: &Key) {
        if let Data::Leaf(ref mut records) = self.data {
            let search = records.binary_search_by(|&(ref k, ref _v)| (**k).cmp(key));
            if let Ok(idx) = search {
                records.remove(idx);
            } else {
                print!(".");
            }
        } else {
            panic!("tried to attach a Del to an Index chain");
        }
    }

    pub fn should_split(&self) -> bool {
        if self.data.len() > FANOUT {
            true
        } else {
            false
        }
    }

    pub fn split(&self, id: PageID) -> Node {
        let (split, right_data) = self.data.split();
        Node {
            id: id,
            data: right_data,
            next: self.next.clone(),
            lo: Bound::Inc(split),
            hi: self.hi.clone(),
        }
    }
}
