use super::*;

impl Materializer for Frag {
    fn merge(&mut self, other: &Frag, config: &Config) {
        if let Frag::Base(ref mut base) = self {
            base.apply(other, config.merge_operator);
        } else {
            panic!("expected base to be the first node");
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
