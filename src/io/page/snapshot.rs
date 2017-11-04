use super::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Snapshot<R> {
    pub max_lsn: Lsn,
    pub last_lid: LogID,
    pub max_pid: PageID,
    pub pt: BTreeMap<PageID, Vec<(Lsn, LogID)>>,
    pub segments: Vec<log::Segment>,
    pub free: Vec<PageID>,
    pub recovery: Option<R>,
}

impl<R> Default for Snapshot<R> {
    fn default() -> Snapshot<R> {
        Snapshot {
            max_lsn: 0,
            last_lid: 0,
            max_pid: 0,
            pt: BTreeMap::new(),
            segments: vec![],
            free: vec![],
            recovery: None,
        }
    }
}

impl<R> Snapshot<R> {
    pub fn _apply() {}
}
