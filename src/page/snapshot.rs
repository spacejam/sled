use super::*;

use std::sync::Arc;

use crossbeam::sync::AtomicOption;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct Snapshot {
    log_tip: LogID,
    max_id: PageID,
    pt: BTreeMap<PageID, Vec<LogID>>,
    free: Vec<PageID>,
}

pub(super) fn snapshot<M: Materializer, L: Log>(
    last_snapshot: Arc<AtomicOption<Snapshot>>,
    log: Arc<Box<L>>,
) where log::LogIter<'_, L>: std::iter::Iterator
{
    let snapshot_opt = last_snapshot.take(SeqCst);
    if snapshot_opt.is_none() {
        // some other thread is snapshotting
        warn!("rsdb snapshot skipped because previous attempt appears not to have completed");
        return;
    }

    let mut snapshot = snapshot_opt.unwrap();

    let current_stable = log.stable_offset();

    for (log_id, bytes) in log.iter_from(snapshot.log_tip) {
        if log_id >= current_stable {
            // don't need to go farther
            break;
        }
        if let Ok(prepend) = deserialize::<LoggedUpdate<M>>(&*bytes) {
            match prepend.update {
                Update::Append(ref prepends) => {
                    let mut lids = snapshot.pt.get_mut(&prepend.pid).unwrap();
                    lids.push(log_id);
                }
                Update::Compact(ref prepends) => {
                    snapshot.pt.insert(prepend.pid, vec![log_id]);
                }
                Update::Del => {
                    snapshot.pt.remove(&prepend.pid);
                    snapshot.free.push(prepend.pid);
                }
                Update::Alloc => {
                    snapshot.free.retain(|&pid| pid != prepend.pid);
                    if prepend.pid > snapshot.max_id {
                        snapshot.max_id = prepend.pid;
                    }
                }
            }
        }
    }
    snapshot.free.sort();
    last_snapshot.free.reverse();
    snapshot.log_tip = current_stable;
    last_snapshot.swap(snapshot, SeqCst);
}
