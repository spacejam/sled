use std::ops::{Bound, Bound::{Included, Excluded, Unbounded}};
use std::iter::FusedIterator;

use pagecache::{Measure, M};

use super::*;

fn lower_bound_includes(bound: &Bound<Vec<u8>>, item: &[u8]) -> bool {
    match bound {
        Included(start) => start.as_slice() <= item,
        Excluded(start) => start.as_slice() < item,
        Unbounded => true,
    }
}

fn upper_bound_includes(bound: &Bound<Vec<u8>>, item: &[u8]) -> bool {
    match bound {
        Included(end) => item <= end.as_slice(),
        Excluded(end) => item < end.as_slice(),
        Unbounded => true,
    }
}

fn node_of_key<'a>(tree: &Tree, key: &[u8], tx: &'a Tx) -> Result<(PageId, &'a Node)> {
    let (page_id, frag, _tree_ptr) =
        tree.path_for_key(key, tx)?
            .pop()
            .expect("path should never be empty");

    let node = frag.unwrap_base();

    Ok((page_id, node))
}

fn node_of_pageid<'a>(tree: &Tree, page_id: PageId, tx: &'a Tx) -> Result<&'a Node> {
    let res = tree
        .context
        .pagecache
        .get(page_id, tx)
        .map(|page_get| page_get.unwrap());

    let frag = res.map(|(frag, _ptr)| frag)?;
    let node = frag.unwrap_base();

    Ok(node)
}

fn biggest_node<'a>(tree: &Tree, tx: &'a Tx) -> Result<(PageId, &'a Node)> {
    let (mut page_id, mut node) = node_of_key(tree, b"", tx)?;

    while let Some(next_page_id) = node.next {
        let next_node = node_of_pageid(tree, next_page_id, tx)?;
        page_id = next_page_id;
        node = next_node;
    }

    Ok((page_id, node))
}

fn possible_predecessor(s: &[u8]) -> Option<Vec<u8>> {
    let mut ret = s.to_vec();
    match ret.pop() {
        None => None,
        Some(i) if i == 0 => Some(ret),
        Some(i) => {
            ret.push(i - 1);
            for _ in 0..4 {
                ret.push(255);
            }
            Some(ret)
        }
    }
}

fn left_node<'a>(tree: &Tree, search_node: &Node, tx: &'a Tx) -> Result<Option<(PageId, &'a Node)>> {
    let pred = match possible_predecessor(&search_node.lo) {
        Some(pred) => pred,
        None => return Ok(None),
    };

    let (mut page_id, mut node) = node_of_key(tree, &pred, tx)?;
    let mut previous_node = (page_id, node);

    if node.lo == search_node.lo { return Ok(None) }

    while let Some(next_page_id) = node.next {
        let next_node = node_of_pageid(tree, next_page_id, tx)?;
        if next_node.lo >= search_node.lo { break }

        previous_node = (page_id, node);
        page_id = next_page_id;
        node = next_node;
    }

    Ok(Some(previous_node))
}

fn seek_to_node_with_key<'a>(
    tree: &Tree,
    from: (PageId, &'a Node),
    key: &[u8],
    tx: &'a Tx,
) -> Result<Option<(PageId, &'a Node)>> {
    let (mut next_id, mut next_node) = from;

    // If we detected a split, we need to seek until
    // the new node that contains our last key.
    while next_node.hi.as_ref() < key {
        let next_page_id = match next_node.next {
            Some(page_id) => page_id,
            None => return Ok(None),
        };

        let node = node_of_pageid(tree, next_page_id, tx)?;

        next_id = next_page_id;
        next_node = node;
    }

    Ok(Some((next_id, next_node)))
}

/// An iterator over keys and values in a `Tree`.
pub struct Iter<'a>(InnerIter<'a>);

enum InnerIter<'a> {
    Valid(ValidIter<'a>),
    Errorneous(Option<Error>),
}

struct ValidIter<'a> {
    tree: &'a Tree,
    hi: Bound<Vec<u8>>,
    lo: Bound<Vec<u8>>,
    last_forward_id: Option<PageId>,
    last_forward_key: Option<Key>,
    last_backward_id: Option<PageId>,
    last_backward_key: Option<Key>,
    tx: Tx,
    // TODO we have to refactor this in light of pages being deleted
}

impl<'a> Iter<'a> {
    pub(crate) fn new(
        tree: &'a Tree,
        lo: Bound<Vec<u8>>,
        hi: Bound<Vec<u8>>,
    ) -> Iter<'a>
    {
        let _measure = Measure::new(&M.tree_scan);

        let tx = match tree.context.pagecache.begin() {
            Ok(tx) => tx,
            Err(e) => return Iter(InnerIter::Errorneous(Some(e))),
        };

        let iter = ValidIter {
            tree,
            hi,
            lo,
            last_forward_id: None,
            last_forward_key: None,
            last_backward_id: None,
            last_backward_key: None,
            tx,
        };

        Iter(InnerIter::Valid(iter))
    }

    /// Iterate over the keys of this Tree
    pub fn keys(self) -> Keys<Iter<'a>> {
        Keys(self)
    }

    /// Iterate over the values of this Tree
    pub fn values(self) -> Values<Iter<'a>> {
        Values(self)
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = Result<(Vec<u8>, IVec)>;

    fn next(&mut self) -> Option<Self::Item> {
        let _measure = Measure::new(&M.tree_scan);

        let iter = match &mut self.0 {
            InnerIter::Valid(iter) => iter,
            InnerIter::Errorneous(error) => {
                match error.take() {
                    Some(error) => return Some(Err(error)),
                    None => return None,
                }
            },
        };

        if let (Some(forward), Some(backward)) = (&iter.last_forward_key, &iter.last_backward_key) {
            if forward >= backward {
                return None
            }
        }

        let start = match &iter.lo {
            Included(start) | Excluded(start) => start.as_slice(),
            Unbounded => b"",
        };

        let mut spins = 0;

        loop {
            spins += 1;
            debug_assert_ne!(
                spins, 100,
                "forward iterator stuck in loop \
                 looking for key after {:?} \
                 with start_bound {:?} \
                 on tree: \n \
                 {:?}",
                iter.last_forward_key, start, iter.tree,
            );

            let node = match iter.last_forward_id {
                Some(page_id) => match node_of_pageid(iter.tree, page_id, &iter.tx) {
                    Ok(node) => node,
                    Err(e) => {
                        error!("iteration failed: {:?}", e);
                        self.0 = InnerIter::Errorneous(None);
                        return Some(Err(e));
                    },
                },
                None => {
                    match node_of_key(iter.tree, start, &iter.tx) {
                        Ok((page_id, node)) => {
                            iter.last_forward_id = Some(page_id);
                            node
                        },
                        Err(e) => {
                            error!("iteration failed: {:?}", e);
                            self.0 = InnerIter::Errorneous(None);
                            return Some(Err(e));
                        },
                    }
                }
            };

            // TODO (when implementing merge support) this could
            // be None if the node was removed since the last
            // iteration, and we need to just get the inner
            // node again...
            let leaf = node.data.leaf_ref().expect("node should be a leaf");
            let prefix = &node.lo;
            let last_key = iter.last_forward_key.as_ref().map(|s| s.as_slice());

            let is_inclusive = match &iter.lo {
                Unbounded | Included(_) => true,
                Excluded(_) => false,
            };

            let entry = if is_inclusive && last_key.is_none() {
                let index = leaf.binary_search_by(|(ref k, _)| {
                    prefix_cmp_encoded(k, start, prefix)
                });

                match index {
                    Ok(index) => Some(&leaf[index]),
                    Err(index) => leaf.get(index),
                }

            } else {
                let search_key = last_key.unwrap_or(start);
                let index = binary_search_gt(leaf, |(ref k, _)| {
                    prefix_cmp_encoded(k, search_key, prefix)
                });

                index.map(|index| &leaf[index])
            };

            if let Some((k, v)) = entry {
                let decoded_k = prefix_decode(prefix, &k);

                if !upper_bound_includes(&iter.hi, &decoded_k) {
                    // we've overshot our bounds
                    return None;
                }

                iter.last_forward_key = Some(decoded_k.clone());

                if let Some(backward) = &iter.last_backward_key {
                    if &decoded_k >= backward {
                        return None
                    }
                }

                return Some(Ok((decoded_k, v.clone())));
            }

            if !node.hi.is_empty() && !upper_bound_includes(&iter.hi, &node.hi) {
                // we've overshot our bounds
                return None;
            }

            // we need to seek to the right sibling to find a
            // key that is greater than our last key (or
            // the start bound)
            match node.next {
                Some(id) => iter.last_forward_id = Some(id),
                None => {
                    assert!(
                        node.hi.is_empty(),
                        "if a node has no right sibling, \
                         it must be the upper-bound node"
                    );
                    return None;
                }
            }
        }
    }
}

impl<'a> DoubleEndedIterator for Iter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let _measure = Measure::new(&M.tree_reverse_scan);

        let iter = match &mut self.0 {
            InnerIter::Valid(iter) => iter,
            InnerIter::Errorneous(error) => {
                match error.take() {
                    Some(error) => return Some(Err(error)),
                    None => return None,
                }
            },
        };

        if let (Some(forward), Some(backward)) = (&iter.last_forward_key, &iter.last_backward_key) {
            if forward >= backward {
                return None
            }
        }

        let mut spins = 0;

        loop {
            spins += 1;
            debug_assert_ne!(
                spins, 100,
                "backward iterator stuck in loop \
                 looking for key before {:?} \
                 with end_bound {:?} \
                 on tree: \n \
                 {:?}",
                iter.last_backward_key, iter.hi, iter.tree,
            );

            let (last_backward_id, node) = match iter.last_backward_id {
                Some(page_id) => {
                    match node_of_pageid(iter.tree, page_id, &iter.tx) {
                        Ok(node) => (page_id, node),
                        Err(e) => {
                            error!("next_back iteration failed: {:?}", e);
                            self.0 = InnerIter::Errorneous(None);
                            return Some(Err(e));
                        },
                    }
                },
                None => {
                    match biggest_node(iter.tree, &iter.tx) {
                        Ok((page_id, node)) => {
                            iter.last_backward_id = Some(page_id);
                            (page_id, node)
                        },
                        Err(e) => {
                            error!("next_back iteration failed: {:?}", e);
                            self.0 = InnerIter::Errorneous(None);
                            return Some(Err(e));
                        },
                    }
                }
            };

            // TODO (when implementing merge support) this could
            // be None if the node was removed since the last
            // iteration, and we need to just get the inner
            // node again...
            let leaf = node.data.leaf_ref().expect("node should be a leaf");
            let prefix = &node.lo;
            let mut split_detected = None;

            let entry = match (&iter.hi, &iter.last_backward_key) {
                (Included(search_key), None) => {
                    let index = binary_search_lub(leaf, |(ref k, _)| {
                        prefix_cmp_encoded(k, search_key, prefix)
                    });

                    index.map(|index| &leaf[index])
                },
                (Unbounded, None) => {
                    leaf.last()
                },
                | (Excluded(search_key), None)
                | (Included(_), Some(search_key))
                | (Excluded(_), Some(search_key))
                | (Unbounded,   Some(search_key)) => {
                    if !node.hi.is_empty() && node.hi.as_ref() < search_key {
                        // the node has been split since we saw it,
                        // and we need to scan forward
                        split_detected = Some(search_key.as_slice());
                        None
                    } else {
                        let index = binary_search_lt(leaf, |(ref k, _)| {
                            prefix_cmp_encoded(k, search_key, prefix)
                        });

                        index.map(|index| &leaf[index])
                    }
                },
            };

            if let Some((k, v)) = entry {
                let decoded_k = prefix_decode(prefix, &k);

                if !lower_bound_includes(&iter.lo, &decoded_k) {
                    // we've overshot our bounds
                    return None;
                }

                iter.last_backward_key = Some(decoded_k.clone());

                if let Some(forward) = &iter.last_forward_key {
                    if &decoded_k <= forward {
                        return None
                    }
                }

                return Some(Ok((decoded_k, v.clone())));
            }

            if !lower_bound_includes(&iter.lo, &node.lo) {
                // we've overshot our bounds
                return None;
            }

            if let Some(split_key) = split_detected {
                // If we detected a split, we need to seek until
                // the new node that contains our last key.
                // We need to skip ahead to get to the node
                // where our last key resided.
                let from = (last_backward_id, node);
                let result = seek_to_node_with_key(iter.tree, from, split_key, &iter.tx);

                let (next_id, next_node) = match result {
                    Ok(Some((page_id, node))) => (page_id, node),
                    Ok(None) => return None,
                    Err(e) => {
                        error!("next_back iteration failed: {:?}", e);
                        self.0 = InnerIter::Errorneous(None);
                        return Some(Err(e));
                    },
                };

                if next_node.data.is_empty() {
                    // we want to mark this node's lo key as our last key
                    // to prevent infinite search loops, by enforcing
                    // reverse progress.
                    iter.last_backward_key = Some(next_node.lo.to_vec());
                }

                iter.last_backward_id = Some(next_id);

            } else {
                // we need to get the node to the left of ours.
                let next_id = match left_node(iter.tree, &node, &iter.tx) {
                    Ok(Some((page_id, _))) => page_id,
                    Ok(None) => return None,
                    Err(e) => {
                        error!("next_back iteration failed: {:?}", e);
                        self.0 = InnerIter::Errorneous(None);
                        return Some(Err(e));
                    },
                };

                iter.last_backward_key = Some(node.lo.to_vec());
                iter.last_backward_id = Some(next_id);
            }
        }
    }
}

impl<'a> FusedIterator for Iter<'a> { }

/// An iterator over keys.
pub struct Keys<I>(I);

impl<I> Iterator for Keys<I>
where I: Iterator<Item=Result<(Vec<u8>, IVec)>>,
{
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next() {
            Some(Ok((key, _))) => Some(Ok(key)),
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}

impl<I> DoubleEndedIterator for Keys<I>
where I: DoubleEndedIterator<Item=Result<(Vec<u8>, IVec)>>,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        match self.0.next_back() {
            Some(Ok((key, _))) => Some(Ok(key)),
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}

impl<I> FusedIterator for Keys<I>
where I: Iterator<Item=Result<(Vec<u8>, IVec)>> + FusedIterator,
{ }

/// An iterator over values.
pub struct Values<I>(I);

impl<I> Iterator for Values<I>
where I: Iterator<Item=Result<(Vec<u8>, IVec)>>,
{
    type Item = Result<IVec>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next() {
            Some(Ok((_, value))) => Some(Ok(value)),
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}

impl<I> DoubleEndedIterator for Values<I>
where I: DoubleEndedIterator<Item=Result<(Vec<u8>, IVec)>>,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        match self.0.next_back() {
            Some(Ok((_, value))) => Some(Ok(value)),
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}

impl<I> FusedIterator for Values<I>
where I: Iterator<Item=Result<(Vec<u8>, IVec)>> + FusedIterator,
{ }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_possible_predecessor() {
        assert_eq!(possible_predecessor(b""), None);
        assert_eq!(possible_predecessor(&[0]), Some(vec![]));
        assert_eq!(possible_predecessor(&[0, 0]), Some(vec![0]));
        assert_eq!(
            possible_predecessor(&[0, 1]),
            Some(vec![0, 0, 255, 255, 255, 255])
        );
        assert_eq!(
            possible_predecessor(&[0, 2]),
            Some(vec![0, 1, 255, 255, 255, 255])
        );
        assert_eq!(possible_predecessor(&[1, 0]), Some(vec![1]));
        assert_eq!(
            possible_predecessor(&[1, 1]),
            Some(vec![1, 0, 255, 255, 255, 255])
        );
        assert_eq!(
            possible_predecessor(&[155]),
            Some(vec![154, 255, 255, 255, 255])
        );
    }
}
