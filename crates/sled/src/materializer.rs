use std::sync::Mutex;

use super::*;

#[derive(Debug)]
pub(crate) struct BLinkMaterializer {
    pub(super) roots: Mutex<Vec<(PageId, PageId)>>,
    config: Config,
}

impl Materializer for BLinkMaterializer {
    type PageFrag = Frag;

    // a vector of (root, prev root) for deterministic recovery
    type Recovery = Vec<(PageId, PageId)>;

    fn new(
        config: Config,
        last_roots: &Option<Self::Recovery>,
    ) -> Self {
        let roots: Vec<(PageId, PageId)> =
            last_roots.clone().unwrap_or_else(|| vec![]);

        BLinkMaterializer {
            roots: Mutex::new(roots),
            config,
        }
    }

    fn merge(&self, frags: &[&Frag]) -> Frag {
        let (mut base_node, is_root) = match frags[0].clone() {
            Frag::Base(base_node, is_root) => (base_node, is_root),
            _ => panic!("non-Base in first element of frags slice"),
        };

        for &frag in &frags[1..] {
            base_node.apply(frag, self.config.merge_operator);
        }

        Frag::Base(base_node, is_root)
    }

    fn recover(&self, frag: &Frag) -> Option<Vec<(PageId, PageId)>> {
        if let Frag::Base(ref node, prev_root) = *frag {
            if let Some(prev_root) = prev_root {
                let mut roots = self.roots.lock().expect(
                    "a thread panicked and poisoned the BLinkMaterializer's
                    roots mutex.",    
                );
                if !roots.contains(&(node.id, prev_root)) {
                    roots.push((node.id, prev_root));
                    roots.sort();
                    return Some(roots.clone());
                }
            }
        }
        
        None
    }

    fn size_in_bytes(&self, frag: &Frag) -> usize {
        match *frag {
            Frag::Base(ref node, _prev_root) => {
                std::mem::size_of::<Frag>() + node.size_in_bytes()
            }
            _ => std::mem::size_of::<Frag>(),
        }
    }
}
