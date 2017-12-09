use std::path::Path;
use std::io::{Read, Seek, Write};

use super::*;

/// A snapshot of the state required to quickly restart
/// the `PageCache` and `SegmentAccountant`.
/// TODO consider splitting `Snapshot` into separate
/// snapshots for PC, SA, Materializer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Snapshot<R> {
    /// the last lsn included in the `Snapshot`
    pub max_lsn: Lsn,
    /// the last lid included in the `Snapshot`
    pub last_lid: LogID,
    /// the highest allocated pid
    pub max_pid: PageID,
    /// the mapping from pages to (lsn, lid)
    pub pt: BTreeMap<PageID, Vec<(Lsn, LogID)>>,
    /// the segment accounting information
    pub segments: Vec<Segment>,
    /// the free pids
    pub free: Vec<PageID>,
    /// the `Materializer`-specific recovered state
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
    // TODO move logic from PageCache::advance_snapshot to here
    fn _apply() {}
}

pub(super) fn advance_snapshot<P, R>(
    iter: LogIter,
    mut snapshot: Snapshot<R>,
    materializer: Arc<Materializer<PageFrag = P, Recovery = R>>,
    config: FinalConfig,
) -> Snapshot<R>
    where P: 'static
                 + Debug
                 + Clone
                 + Serialize
                 + DeserializeOwned
                 + Send
                 + Sync,
          R: Debug + Clone + Serialize + DeserializeOwned + Send
{
    let start = clock();

    trace!("building on top of old snapshot: {:?}", snapshot);

    let io_buf_size = config.get_io_buf_size();

    let mut recovery = snapshot.recovery.take();
    let mut max_lsn = snapshot.max_lsn;
    let mut last_lid = snapshot.last_lid;

    let mut last_segment = None;

    for (lsn, log_id, bytes) in iter {
        let segment_lsn = lsn / io_buf_size as Lsn * io_buf_size as Lsn;

        trace!(
            "in advance_snapshot looking at item: segment lsn {} lsn {} lid {}",
            segment_lsn,
            lsn,
            log_id
        );

        if lsn <= max_lsn {
            // don't process already-processed Lsn's.
            trace!(
                "continuing in advance_snapshot, lsn {} log_id {} max_lsn {}",
                lsn,
                log_id,
                max_lsn
            );
            continue;
        }

        assert!(lsn > max_lsn);
        max_lsn = lsn;
        last_lid = log_id;

        let idx = log_id as usize / io_buf_size;
        if snapshot.segments.len() < idx + 1 {
            snapshot.segments.resize(idx + 1, Segment::default());
        }

        assert_eq!(
            segment_lsn / io_buf_size as Lsn * io_buf_size as Lsn,
            segment_lsn,
            "segment lsn is unaligned! fix above lsn statement..."
        );

        // unwrapping this because it's already passed the crc check
        // in the log iterator
        trace!("trying to deserialize buf for lid {} lsn {}", log_id, lsn);
        let deserialization = deserialize::<LoggedUpdate<P>>(&*bytes);

        if let Err(e) = deserialization {
            error!(
                "failed to deserialize buffer for item in log: lsn {} \
                    lid {}: {:?}",
                lsn,
                log_id,
                e
            );
            continue;
        }

        let prepend = deserialization.unwrap();

        if prepend.pid >= snapshot.max_pid {
            snapshot.max_pid = prepend.pid + 1;
        }

        snapshot.segments[idx].recovery_ensure_initialized(segment_lsn);

        let last_idx = *last_segment.get_or_insert(idx);
        if last_idx != idx {
            // if we have moved to a new segment, mark the previous one
            // as inactive.
            trace!("PageCache recovery setting segment {} to inactive", log_id);
            snapshot.segments[last_idx].active_to_inactive(segment_lsn, true);
            if snapshot.segments[last_idx].is_empty() {
                trace!(
                    "PageCache recovery setting segment {} to draining",
                    log_id
                );
                snapshot.segments[last_idx].inactive_to_draining(segment_lsn);
            }
        }
        last_segment = Some(idx);

        match prepend.update {
            Update::Append(partial_page) => {
                // Because we rewrite pages over time, we may have relocated
                // a page's initial Compact to a later segment. We should skip
                // over pages here unless we've encountered a Compact or Alloc
                // for them.
                if let Some(lids) = snapshot.pt.get_mut(&prepend.pid) {
                    trace!(
                        "append of pid {} at lid {} lsn {}",
                        prepend.pid,
                        log_id,
                        lsn
                    );

                    snapshot.segments[idx].insert_pid(prepend.pid, segment_lsn);

                    let r = materializer.recover(&partial_page);
                    if r.is_some() {
                        recovery = r;
                    }

                    lids.push((lsn, log_id));
                }
            }
            Update::Compact(partial_page) => {
                trace!(
                    "compact of pid {} at lid {} lsn {}",
                    prepend.pid,
                    log_id,
                    lsn
                );
                if let Some(lids) = snapshot.pt.remove(&prepend.pid) {
                    for (_lsn, old_lid) in lids {
                        let old_idx = old_lid as usize / io_buf_size;
                        if old_idx == idx {
                            // don't remove pid if it's still there
                            continue;
                        }
                        let old_segment = &mut snapshot.segments[old_idx];

                        old_segment.remove_pid(prepend.pid, segment_lsn);
                    }
                }

                snapshot.segments[idx].insert_pid(prepend.pid, segment_lsn);

                let r = materializer.recover(&partial_page);
                if r.is_some() {
                    recovery = r;
                }

                snapshot.pt.insert(prepend.pid, vec![(lsn, log_id)]);
            }
            Update::Free => {
                trace!(
                    "del of pid {} at lid {} lsn {}",
                    prepend.pid,
                    log_id,
                    lsn
                );
                if let Some(lids) = snapshot.pt.remove(&prepend.pid) {
                    // this could fail if our Alloc was nuked
                    for (_lsn, old_lid) in lids {
                        let old_idx = old_lid as usize / io_buf_size;
                        if old_idx == idx {
                            // don't remove pid if it's still there
                            continue;
                        }
                        let old_segment = &mut snapshot.segments[old_idx];
                        old_segment.remove_pid(prepend.pid, segment_lsn);
                    }
                }

                snapshot.segments[idx].insert_pid(prepend.pid, segment_lsn);

                snapshot.free.push(prepend.pid);
            }
            Update::Alloc => {
                trace!(
                    "alloc of pid {} at lid {} lsn {}",
                    prepend.pid,
                    log_id,
                    lsn
                );

                snapshot.pt.insert(prepend.pid, vec![]);
                snapshot.free.retain(|&pid| pid != prepend.pid);
                snapshot.segments[idx].insert_pid(prepend.pid, segment_lsn);
            }
        }
    }

    snapshot.free.sort();
    snapshot.free.reverse();
    snapshot.max_lsn = max_lsn;
    snapshot.last_lid = last_lid;
    snapshot.recovery = recovery;

    write_snapshot(config, &snapshot);

    trace!("generated new snapshot: {:?}", snapshot);

    M.advance_snapshot.measure(clock() - start);

    snapshot
}

pub fn read_snapshot<R>(config: FinalConfig) -> Option<Snapshot<R>>
    where R: Debug + Clone + Serialize + DeserializeOwned + Send
{
    let mut candidates = config.get_snapshot_files();
    if candidates.is_empty() {
        info!("no previous snapshot found");
        return None;
    }

    candidates.sort_by_key(|path| {
        std::fs::metadata(path).unwrap().created().unwrap()
    });

    let path = candidates.pop().unwrap();

    let mut f = std::fs::OpenOptions::new().read(true).open(&path).unwrap();

    let mut buf = vec![];
    f.read_to_end(&mut buf).unwrap();
    let len = buf.len();
    buf.split_off(len - 8);

    let mut crc_expected_bytes = [0u8; 8];
    f.seek(std::io::SeekFrom::End(-8)).unwrap();
    f.read_exact(&mut crc_expected_bytes).unwrap();

    let crc_expected: u64 = unsafe { std::mem::transmute(crc_expected_bytes) };
    let crc_actual = crc64(&*buf);

    if crc_expected != crc_actual {
        panic!("crc for snapshot file {:?} failed!", path);
    }

        #[cfg(feature = "zstd")]
    let bytes = if config.get_use_compression() {
        decompress(&*buf, config.get_io_buf_size()).unwrap()
    } else {
        buf
    };

        #[cfg(not(feature = "zstd"))]
    let bytes = buf;

    let snapshot = deserialize::<Snapshot<R>>(&*bytes).unwrap();

    Some(snapshot)
}

pub fn write_snapshot<R>(config: FinalConfig, snapshot: &Snapshot<R>)
    where R: Debug + Clone + Serialize + DeserializeOwned + Send
{
    let raw_bytes = serialize(&snapshot, Infinite).unwrap();

        #[cfg(feature = "zstd")]
    let bytes = if config.get_use_compression() {
        compress(&*raw_bytes, 5).unwrap()
    } else {
        raw_bytes
    };

        #[cfg(not(feature = "zstd"))]
    let bytes = raw_bytes;

    let crc64: [u8; 8] = unsafe { std::mem::transmute(crc64(&*bytes)) };

    let prefix = config.snapshot_prefix();

    let path_1 = format!("{}.{}.in___motion", prefix, snapshot.max_lsn);
    let path_2 = format!("{}.{}", prefix, snapshot.max_lsn);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&path_1)
        .unwrap();

    // write the snapshot bytes, followed by a crc64 checksum at the end
    f.write_all(&*bytes).unwrap();
    f.write_all(&crc64).unwrap();
    f.sync_all().unwrap();
    drop(f);

    trace!("wrote snapshot to {}", path_1);

    std::fs::rename(path_1, &path_2).expect("failed to write snapshot");

    trace!("renamed snapshot to {}", path_2);

    // clean up any old snapshots
    let candidates = config.get_snapshot_files();
    for path in candidates {
        let path_str = Path::new(&path).file_name().unwrap().to_str().unwrap();
        if !path_2.ends_with(&*path_str) {
            debug!("removing old snapshot file {:?}", path);

            if let Err(_e) = std::fs::remove_file(&path) {
                warn!(
                    "failed to remove old snapshot file, maybe snapshot race? {}",
                    _e
                );
            }
        }
    }
}
