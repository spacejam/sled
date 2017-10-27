use super::*;

#[derive(Default, Debug)]
pub struct BLinkMaterializer {
    // TODO use interval-based tracking to handle race conditions in
    // hoists where higher is not later.
    pub(super) roots: Mutex<Vec<PageID>>,
}

impl Materializer for BLinkMaterializer {
    type PageFrag = Frag;
    type Recovery = PageID;

    fn initialize_with_previous_recovery(&self, last_root: &PageID) {
        let mut roots = self.roots.lock().unwrap();
        roots.push(*last_root);
    }

    fn merge(&self, frags: &[&Frag]) -> Frag {
        let mut base_node_opt: Option<Node> = None;
        let mut root = false;

        for &frag in frags {
            if let Some(ref mut base_node) = base_node_opt {
                base_node.apply(frag);
            } else {
                let (base_node, is_root) = frag.base().unwrap();
                if is_root {
                    debug!("merged node {} is root", base_node.id);
                }
                base_node_opt = Some(base_node);
                root = is_root;
            }
        }

        Frag::Base(base_node_opt.unwrap(), root)
    }

    fn recover(&self, frag: &Frag) -> Option<PageID> {
        match *frag {
            Frag::Base(ref node, root) => {
                if root {
                    let mut roots = self.roots.lock().unwrap();
                    if roots.contains(&node.id) {
                        None
                    } else {
                        roots.push(node.id);
                        Some(node.id)
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}
