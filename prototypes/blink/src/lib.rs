/// An in-memory b-link tree.

// notes
// * during all traversals, merges or splits may be encountered
// * if a partial SMO is encountered, complete it

use std::cmp::Ordering;
use std::collections::HashMap;

type NodeID = u64;

const ROOT: NodeID = 0;
const FANOUT: usize = 128;

#[derive(Clone, Debug, Ord, Eq, PartialEq)]
enum Bound {
    Inc(Vec<u8>),
    Non(Vec<u8>),
    Inf,
}

impl Bound {
    fn inner(&self) -> Option<Vec<u8>> {
        match self {
            &Bound::Inc(ref v) => Some(v.clone()),
            _ => None,
        }
    }
}

impl PartialOrd for Bound {
    fn partial_cmp(&self, other: &Bound) -> Option<Ordering> {
        use Bound::*;
        match *self {
            Inc(ref lhs) => {
                match *other {
                    Inf => Some(Ordering::Less),
                    Inc(ref rhs) => Some(lhs.cmp(rhs)),
                    Non(ref rhs) => {
                        if lhs < rhs {
                            Some(Ordering::Less)
                        } else {
                            Some(Ordering::Greater)
                        }
                    }
                }
            }
            Non(ref lhs) => {
                match *other {
                    Inf => Some(Ordering::Less),
                    Inc(ref rhs) => {
                        if lhs <= rhs {
                            Some(Ordering::Less)
                        } else {
                            Some(Ordering::Greater)
                        }
                    }
                    Non(ref rhs) => Some(lhs.cmp(&rhs)),
                }
            }
            Inf => {
                match *other {
                    Inf => Some(Ordering::Equal),
                    _ => Some(Ordering::Less),
                }
            }
        }
    }
}

struct Tree {
    pages: HashMap<NodeID, Node>,
    max_id: NodeID,
}

#[derive(Clone, Debug)]
struct Node {
    id: NodeID,
    data: Data,
    next: Option<NodeID>,
    lo: Bound,
    hi: Bound,
}

impl Node {
    fn split_if_necessary(&self, parent: &mut Tree) -> Option<(Node, Node)> {
        match self.data {
            Data::Index(ref ptrs) => {
                if ptrs.len() > FANOUT {
                    let id = parent.new_id();
                    let (lhs, rhs) = ptrs.split_at(ptrs.len() / 2);
                    let split = rhs.first().unwrap().0.clone();
                    let left = Node {
                        id: self.id,
                        data: Data::Index(lhs.to_vec()),
                        next: Some(id),
                        lo: self.lo.clone(),
                        hi: Bound::Non(split.clone()),
                    };
                    let right = Node {
                        id: id,
                        data: Data::Index(rhs.to_vec()),
                        next: self.next.clone(),
                        lo: Bound::Inc(split.clone()),
                        hi: self.hi.clone(),
                    };
                    Some((left, right))
                } else {
                    None
                }
            }
            Data::Leaf(ref items) => {
                if items.len() > FANOUT {
                    let id = parent.new_id();
                    let (lhs, rhs) = items.split_at(items.len() / 2);
                    let split = rhs.first().unwrap().0.clone();
                    let left = Node {
                        id: self.id,
                        data: Data::Leaf(lhs.to_vec()),
                        next: Some(id),
                        lo: self.lo.clone(),
                        hi: Bound::Non(split.clone()),
                    };
                    let right = Node {
                        id: id,
                        data: Data::Leaf(rhs.to_vec()),
                        next: self.next.clone(),
                        lo: Bound::Inc(split.clone()),
                        hi: self.hi.clone(),
                    };
                    Some((left, right))
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
enum Data {
    Index(Vec<(Vec<u8>, NodeID)>),
    Leaf(Vec<(Vec<u8>, Vec<u8>)>),
}

// general traversal hazards:
// encounter merged nodes

impl Tree {
    pub fn new() -> Tree {
        let leaf = Node {
            id: ROOT + 1,
            data: Data::Leaf(vec![]),
            next: None,
            lo: Bound::Inc(vec![]),
            hi: Bound::Inf,
        };
        let root = Node {
            id: ROOT,
            data: Data::Index(vec![(vec![], leaf.id)]),
            next: None,
            lo: Bound::Inc(vec![]),
            hi: Bound::Inf,
        };
        let mut pages = HashMap::new();
        pages.insert(root.id, root);
        pages.insert(leaf.id, leaf);
        Tree {
            pages: pages,
            max_id: ROOT + 1,
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut cursor = ROOT;
        while let Some(node) = self.pages.get(&cursor) {
            let old_cursor = cursor.clone();
            match node.data {
                Data::Index(ref ptrs) => {
                    for &(ref sep_k, ref ptr) in ptrs {
                        if &**sep_k <= key {
                            cursor = *ptr;
                        } else {
                            break;
                        }
                    }
                }
                Data::Leaf(ref items) => {
                    let search = items.binary_search_by(|&(ref k, ref v)| (**k).cmp(key));
                    if let Ok(idx) = search {
                        return Some(items[idx].1.clone());
                    } else {
                        return None;
                    }
                }
            }
            if cursor == old_cursor {
                panic!("get failed to traverse");
            }
        }
        None
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // 1. traverse indexes & insert at leaf
        let mut cursor = ROOT;
        let mut path = vec![ROOT];
        let mut needs_split = false;
        // println!("traversing, using key {:?}", key);
        while let Some(node) = self.pages.get_mut(&cursor) {
            let old_cursor = cursor.clone();
            match node.data {
                Data::Index(ref ptrs) => {
                    // println!("level: {} pointers: {:?}", path.len(), ptrs);
                    for &(ref sep_k, ref ptr) in ptrs {
                        if &**sep_k <= &*key {
                            cursor = *ptr;
                        } else {
                            if cursor == old_cursor {
                                panic!("set failed to traverse");
                            }
                            break; // we've found our next cursor
                        }
                    }
                    path.push(cursor.clone());
                }
                Data::Leaf(ref mut items) => {
                    // println!("comparing leaf!");
                    let search = items.binary_search_by(|&(ref k, ref v)| k.cmp(&key));
                    if let Ok(idx) = search {
                        items.push((key.clone(), value.clone()));
                        items.swap_remove(idx);
                    } else {
                        items.push((key.clone(), value.clone()));
                        items.sort_by(|a, b| a.0.cmp(&b.0));
                        if items.len() > FANOUT {
                            // println!("marking leaf node as in need of split");
                            needs_split = true;
                        }
                    }

                    break; // we've finished inserting
                }
            }
        }

        print!("{} ", path.len());

        // 2. recursively split if necessary
        if !needs_split {
            return;
        }

        // println!("needs split!");
        // to split, we pop the path, see if it's in need of split, recurse up
        // two-phase: (in prep for lock-free, not necessary for single threaded)
        //  1. half-split: install split on child, P
        //      a. allocate new right sibling page, Q
        //      b. locate split point
        //      c. create new consolidated pages for both sides
        //      d. add new node to pagetable
        //      e. append split delta to original page P with physical pointer to Q
        //  2. parent update: install new index term on parent
        //      a. append "index term delta record" to parent, containing:
        //          i. new bounds for P & Q
        //          ii. logical pointer to Q
        //
        //      (it's possible parent was merged in the mean-time, so if that's the
        //      case, we need to go up the path to the grandparent then down again
        //      or higher until it works)
        while let Some(cursor) = path.pop() {
            let node = self.pages.get(&cursor).unwrap().clone();

            if let Some((mut lhs, rhs)) = node.split_if_necessary(self) {
                // println!("processing split at {}", cursor);
                if let Some(parent_id) = path.last() {
                    // println!("pushing new node onto parent {}", parent_id);
                    let ref mut parent = self.pages.get_mut(parent_id).unwrap();
                    if let Data::Index(ref mut ptrs) = parent.data {
                        let search =
                            ptrs.binary_search_by(|&(ref k, ref v)| {
                                Bound::Inc(k.clone()).cmp(&lhs.lo)
                            });
                        if let Ok(idx) = search {
                            ptrs.remove(idx);
                        }
                        ptrs.push((lhs.lo.inner().unwrap(), lhs.id));
                        ptrs.push((rhs.lo.inner().unwrap(), rhs.id));
                        ptrs.sort_by(|a, b| a.0.cmp(&b.0));
                    }
                } else {
                    // need to hoist a new root
                    // println!("$#@$#%!$%#@$%#@% hoisting new root");
                    assert_eq!(lhs.id, ROOT);
                    lhs.id = self.new_id();
                    let root = Node {
                        id: ROOT,
                        data: Data::Index(vec![(vec![], lhs.id),
                                               (rhs.lo.inner().unwrap(), rhs.id)]),
                        next: None,
                        lo: Bound::Inc(vec![]),
                        hi: Bound::Inf,
                    };
                    self.pages.insert(ROOT, root);
                }
                self.pages.insert(lhs.id, lhs);
                self.pages.insert(rhs.id, rhs);
            } else {
                // println!("none necessary");
            }
        }
    }

    pub fn del(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let mut cursor = ROOT;
        let mut path = vec![ROOT];
        let mut needs_merge = false;
        let mut ret = None;
        // println!("traversing, using key {:?}", key);
        while let Some(node) = self.pages.get_mut(&cursor) {
            let old_cursor = cursor.clone();
            match node.data {
                Data::Index(ref ptrs) => {
                    // println!("level: {} pointers: {:?}", path.len(), ptrs);
                    for &(ref sep_k, ref ptr) in ptrs {
                        if &**sep_k <= &*key {
                            cursor = *ptr;
                        } else {
                            if cursor == old_cursor {
                                panic!("set failed to traverse");
                            }
                            break; // we've found our next cursor
                        }
                    }
                    path.push(cursor.clone());
                }
                Data::Leaf(ref mut items) => {
                    // println!("comparing leaf!");
                    let search = items.binary_search_by(|&(ref k, ref v)| (**k).cmp(key));
                    if let Ok(idx) = search {
                        ret = Some(items.remove(idx).1);
                        if items.len() < FANOUT / 2 {
                            needs_merge = true;
                        }
                    }

                    break; // we've finished removing
                }
            }
        }

        print!("{} ", path.len());

        // 2. recursively merge if necessary
        if !needs_merge {
            return ret;
        }

        println!("needs merge!");
        // TODO merge
        ret
    }

    fn new_id(&mut self) -> NodeID {
        self.max_id += 1;
        self.max_id
    }
}

#[test]
fn test_bounds() {
    use Bound::*;
    assert!(Inf == Inf);
    assert!(Inc(b"hi".to_vec()) == Inc(b"hi".to_vec()));
    assert!(Non(b"hi".to_vec()) == Non(b"hi".to_vec()));
    assert!(Inc(b"hi".to_vec()) > Non(b"hi".to_vec()));
    assert!(Inc(vec![]) < Inf);
    assert!(Non(vec![]) < Inf);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut tree = Tree::new();
        for i in 0..100000 {
            let k = format!("k{}", i);
            let k = k.as_bytes();
            let v = format!("v{}", i);
            let v = v.as_bytes();
            tree.set(k.to_vec(), v.to_vec());
            assert_eq!(tree.get(k), Some(v.to_vec()));
        }
    }
}
