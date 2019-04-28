use super::*;

pub(crate) struct BLinkMaterializer;

impl Materializer for BLinkMaterializer {
    type PageFrag = Frag;

    fn merge<'a, I>(frags: I, config: &Config) -> Self::PageFrag
    where
        I: IntoIterator<Item = &'a Self::PageFrag>,
    {
        let mut frag_iter = frags.into_iter();

        let possible_base = frag_iter
            .next()
            .expect("merge should only be called on non-empty sets of Frag's");

        match possible_base {
            Frag::Base(ref base_node_ref) => {
                let mut base_node = base_node_ref.clone();
                for frag in frag_iter {
                    base_node.apply(frag, config.merge_operator);
                }

                Frag::Base(base_node)
            }
            _ => panic!("non-Base in first element of frags slice"),
        }
    }

    fn size_in_bytes(frag: &Frag) -> u64 {
        match *frag {
            Frag::Base(ref node) => (std::mem::size_of::<Frag>() as u64)
                .saturating_add(node.size_in_bytes()),
            _ => std::mem::size_of::<Frag>() as u64,
        }
    }
}
