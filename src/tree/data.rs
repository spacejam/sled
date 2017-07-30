use super::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Data {
    Index(Vec<(Key, PageID)>),
    Leaf(Vec<(Key, Value)>),
}

impl Data {
    pub fn len(&self) -> usize {
        match *self {
            Data::Index(ref ptrs) => ptrs.len(),
            Data::Leaf(ref items) => items.len(),
        }
    }

    pub fn split(&self) -> (Key, Data) {
        fn split_inner<T>(xs: &Vec<(Key, T)>) -> (Key, Vec<(Key, T)>)
            where T: Clone + Debug
        {
            let (_lhs, rhs) = xs.split_at(xs.len() / 2 + 1);
            let split = rhs.first().unwrap().0.clone();

            (split, rhs.to_vec())
        }

        match *self {
            Data::Index(ref ptrs) => {
                let (split, rhs) = split_inner(ptrs);
                (split, Data::Index(rhs))
            }
            Data::Leaf(ref items) => {
                let (split, rhs) = split_inner(items);
                (split, Data::Leaf(rhs))
            }
        }
    }

    pub fn drop_gte(&mut self, at: &Bound) {
        let bound = at.inner().unwrap();
        match *self {
            Data::Index(ref mut ptrs) => ptrs.retain(|&(ref k, _)| k < &bound),
            Data::Leaf(ref mut items) => items.retain(|&(ref k, _)| k < &bound),
        }
    }

    // used for detecting root hoists during recovery
    pub fn is_parent_of(&self, pid: PageID) -> bool {
        match *self {
            Data::Index(ref ptrs) => {
                for &(_, ref child) in ptrs {
                    if *child == pid {
                        return true;
                    }
                }
                false
            }
            Data::Leaf(_) => false,
        }
    }

    pub fn leaf(&self) -> Option<Vec<(Key, Value)>> {
        match self {
            &Data::Index(_) => None,
            &Data::Leaf(ref items) => Some(items.clone()),
        }
    }
}
