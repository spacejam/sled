use std::sync::Mutex;

use super::*;

#[derive(Debug)]
pub(crate) struct BLinkMaterializer {
    pub(super) recovery: Mutex<Recovery>,
    config: Config,
}

impl Materializer for BLinkMaterializer {
    type PageFrag = Frag;

    // a vector of (root, prev root, max counter) for deterministic recovery
    type Recovery = Recovery;

    fn new(
        config: Config,
        recovery: &Option<Self::Recovery>,
    ) -> Self {
        let recovery = recovery.clone().unwrap_or_default();

        BLinkMaterializer {
            recovery: Mutex::new(recovery),
            config,
        }
    }

    fn merge(&self, frags: &[&Frag]) -> Frag {
        match frags[0].clone() {
            Frag::Base(mut base_node, is_root) => {
                for &frag in &frags[1..] {
                    base_node.apply(frag, self.config.merge_operator);
                }

                Frag::Base(base_node, is_root)
            }
            Frag::Counter(count) => {
                let mut max = count;
                for &frag in &frags[1..] {
                    if let Frag::Counter(count) = frag {
                        max = std::cmp::max(*count, max);
                    } else {
                        panic!(
                            "got non-BumpCounter in frag chain: {:?}",
                            frag
                        );
                    }
                }

                Frag::Counter(max)
            }
            _ => panic!("non-Base in first element of frags slice"),
        }
    }

    fn recover(&self, frag: &Frag) -> Option<Recovery> {
        match *frag {
            Frag::Base(ref node, prev_root) => {
                let prev_root = prev_root?;
                let mut recovery = self.recovery.lock().expect(
                        "a thread panicked and poisoned the BLinkMaterializer's
                        roots mutex.",
                    );
                if recovery
                    .root_transitions
                    .contains(&(node.id, prev_root))
                {
                    None
                } else {
                    recovery
                        .root_transitions
                        .push((node.id, prev_root));

                    recovery.root_transitions.sort();

                    Some(recovery.clone())
                }
            }
            Frag::Counter(count) => {
                let mut recovery = self.recovery.lock().expect(
                    "a thread panicked and poisoned the BLinkMaterializer's
                    roots mutex.",
                );
                recovery.counter =
                    std::cmp::max(count, recovery.counter);

                Some(recovery.clone())
            }
            _ => None,
        }
    }

    fn size_in_bytes(&self, frag: &Frag) -> usize {
        match *frag {
            Frag::Base(ref node, _prev_root) => {
                std::mem::size_of::<Frag>()
                    .saturating_add(node.size_in_bytes() as usize)
            }
            _ => std::mem::size_of::<Frag>(),
        }
    }
}
