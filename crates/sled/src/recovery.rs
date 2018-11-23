use super::*;

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    PartialOrd,
    Serialize,
    Deserialize,
)]
pub struct Recovery {
    pub root_transitions: Vec<(PageId, PageId)>,
    pub counter: usize,
}
