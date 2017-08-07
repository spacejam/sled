use super::*;

pub struct CacheStack<M: Materializer> {
}

impl<M> CacheStack<M>
    where M: Materializer
{
    pub fn page_in(&self) -> M::MaterializedPage {}

    pub fn page_out(&self) {}
}
