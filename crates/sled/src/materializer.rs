use super::*;

impl Materializer for Frag {
    fn merge(&mut self, other: &Frag, config: &Config) {
        if let Frag::Base(ref mut base) = self {
            base.apply(other, config.merge_operator);
        } else {
            panic!("expected base to be the first node");
        }
    }
}
