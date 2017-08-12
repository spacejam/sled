use std::sync::Arc;
use std::io::Write;

use crossbeam::sync::AtomicOption;

use super::*;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(super) struct Snapshot {
    log_tip: LogID,
    max_id: PageID,
    pt: BTreeMap<PageID, Vec<LogID>>,
    free: Vec<PageID>,
}

pub(super) fn snapshot<'a, M, L>(
    prefix: String,
    last_snapshot: Arc<AtomicOption<Snapshot>>,
    log: Arc<L>,
)
    where M: Materializer + DeserializeOwned,
          L: Log,
          L: 'a
{
    let snapshot_opt = last_snapshot.take(SeqCst);
    if snapshot_opt.is_none() {
        // some other thread is snapshotting
        warn!(
            "rsdb snapshot skipped because previous attempt \
              appears not to have completed"
        );
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
                Update::Append(_) => {
                    let mut lids = snapshot.pt.get_mut(&prepend.pid).unwrap();
                    lids.push(log_id);
                }
                Update::Compact(_) => {
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
    snapshot.free.reverse();
    snapshot.log_tip = current_stable;

    let bytes = serialize(&snapshot, Infinite).unwrap();
    let crc64: [u8; 8] = unsafe { std::mem::transmute(crc64::crc64(&*bytes)) };

    let path = format!("{}_{}.snapshot", prefix, snapshot.log_tip);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .unwrap();

    f.write_all(&*bytes).unwrap();
    f.write_all(&crc64).unwrap();

    last_snapshot.swap(snapshot, SeqCst);
}

pub(super) fn read_snapshot_or_default(_prefix: String) -> Snapshot {
    Snapshot::default()
}
