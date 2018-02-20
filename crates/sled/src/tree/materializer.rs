use std::sync::Mutex;

use super::*;

#[derive(Default, Debug)]
pub struct BLinkMaterializer {
    pub(super) roots: Mutex<Vec<(PageID, PageID)>>,
}

impl Materializer for BLinkMaterializer {
    type PageFrag = Frag;

    // a vector of (root, prev root) for deterministic recovery
    type Recovery = Vec<(PageID, PageID)>;

    fn new(last_roots: &Option<Self::Recovery>) -> Self {
        let roots: Vec<(PageID, PageID)> =
            last_roots.clone().unwrap_or_else(|| vec![]);

        BLinkMaterializer {
            roots: Mutex::new(roots),
        }
    }

    fn merge(&self, frags: &[&Frag]) -> Frag {
        let (mut base_node, is_root) = match frags[0].clone() {
            Frag::Base(base_node, is_root) => (base_node, is_root),
            _ => panic!("non-Base in first element of frags slice"),
        };

        for &frag in &frags[1..] {
            base_node.apply(frag);
        }

        Frag::Base(base_node, is_root)
    }

    fn recover(&self, frag: &Frag) -> Option<Vec<(PageID, PageID)>> {
        match *frag {
            Frag::Base(ref node, prev_root) => {
                if let Some(prev_root) = prev_root {
                    let mut roots = self.roots.lock().unwrap();
                    if !roots.contains(&(node.id, prev_root)) {
                        roots.push((node.id, prev_root));
                        return Some(roots.clone());
                    }
                }
            }
            _ => (),
        }
        None
    }
}
