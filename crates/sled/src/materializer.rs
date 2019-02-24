use super::*;

#[derive(Debug)]
pub(crate) struct BLinkMaterializer {
    config: Config,
}

impl Materializer for BLinkMaterializer {
    type PageFrag = Frag;

    // a vector of (root, prev root, max counter) for deterministic recovery
    fn new(config: Config) -> Self {
        BLinkMaterializer { config }
    }

    fn merge<'a, I>(&'a self, frags: I) -> Self::PageFrag
    where
        I: IntoIterator<Item = &'a Self::PageFrag>,
    {
        let mut frag_iter = frags.into_iter();

        let possible_base = frag_iter.next().expect(
            "merge should only be called on non-empty sets of Frag's",
        );

        match possible_base {
            Frag::Base(ref base_node_ref) => {
                let mut base_node = base_node_ref.clone();
                for frag in frag_iter {
                    base_node.apply(frag, self.config.merge_operator);
                }

                Frag::Base(base_node)
            }
            _ => panic!("non-Base in first element of frags slice"),
        }
    }

    fn size_in_bytes(&self, frag: &Frag) -> usize {
        match *frag {
            Frag::Base(ref node) => std::mem::size_of::<Frag>()
                .saturating_add(node.size_in_bytes() as usize),
            _ => std::mem::size_of::<Frag>(),
        }
    }
}
