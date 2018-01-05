use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::io::{Read, Seek, Write};

#[cfg(feature = "zstd")]
use zstd::block::{compress, decompress};

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
    pub pt: HashMap<PageID, Vec<(Lsn, LogID)>>,
    /// replaced pages per segment index
    pub replacements: HashMap<usize, HashSet<(PageID, Lsn)>>,
    /// the free pids
    pub free: HashSet<PageID>,
    /// the `Materializer`-specific recovered state
    pub recovery: Option<R>,
}

impl<R> Default for Snapshot<R> {
    fn default() -> Snapshot<R> {
        Snapshot {
            max_lsn: 0,
            last_lid: 0,
            max_pid: 0,
            pt: HashMap::new(),
            replacements: HashMap::new(),
            free: HashSet::new(),
            recovery: None,
        }
    }
}

impl<R> Snapshot<R> {
    fn apply<P>(
        &mut self,
        materializer: &Arc<Materializer<PageFrag = P, Recovery = R>>,
        lsn: Lsn,
        log_id: LogID,
        bytes: &[u8],
        io_buf_size: usize,
    )
        where P: 'static
                     + Debug
                     + Clone
                     + Serialize
                     + DeserializeOwned
                     + Send
                     + Sync,
              R: Debug + Clone + Serialize + DeserializeOwned + Send
    {
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
            return;
        }

        let prepend = deserialization.unwrap();
        let pid = prepend.pid;

        if pid >= self.max_pid {
            self.max_pid = pid + 1;
        }

        let replaced_at_idx = log_id as usize / io_buf_size;

        match prepend.update {
            Update::Append(partial_page) => {
                // Because we rewrite pages over time, we may have relocated
                // a page's initial Compact to a later segment. We should skip
                // over pages here unless we've encountered a Compact for them.
                if let Some(lids) = self.pt.get_mut(&pid) {
                    trace!(
                        "append of pid {} at lid {} lsn {}",
                        pid,
                        log_id,
                        lsn
                    );

                    if let Some(r) = materializer.recover(&partial_page) {
                        self.recovery = Some(r);
                    }

                    lids.push((lsn, log_id));
                }
                self.free.remove(&pid);
            }
            Update::Compact(partial_page) => {
                trace!("compact of pid {} at lid {} lsn {}", pid, log_id, lsn);
                self.replace_pid(pid, replaced_at_idx, lsn, io_buf_size);

                if let Some(r) = materializer.recover(&partial_page) {
                    self.recovery = Some(r);
                }

                self.pt.insert(pid, vec![(lsn, log_id)]);
                self.free.remove(&pid);
            }
            Update::Free => {
                trace!("del of pid {} at lid {} lsn {}", pid, log_id, lsn);
                self.replace_pid(pid, replaced_at_idx, lsn, io_buf_size);
                self.pt.insert(pid, vec![(lsn, log_id)]);

                self.free.insert(pid);
            }
        }
    }

    fn replace_pid(
        &mut self,
        pid: PageID,
        replaced_at_idx: usize,
        replaced_at_lsn: Lsn,
        io_buf_size: usize,
    ) {
        if let Some(coords) = self.pt.remove(&pid) {
            for (_lsn, lid) in coords {
                let idx = lid as usize / io_buf_size;
                if replaced_at_idx == idx {
                    continue;
                }
                let entry =
                    self.replacements.entry(idx).or_insert(HashSet::new());
                entry.insert((pid, replaced_at_lsn));
            }
        }
    }
}

pub(super) fn advance_snapshot<P, R>(
    iter: LogIter,
    mut snapshot: Snapshot<R>,
    materializer_opt: Option<Arc<Materializer<PageFrag = P, Recovery = R>>>,
    config: &FinalConfig,
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

    for (lsn, log_id, bytes) in iter {
        let segment_idx = lsn as usize / io_buf_size;
        let segment_lsn = (segment_idx * io_buf_size) as Lsn;

        assert_eq!(
            segment_lsn / io_buf_size as Lsn * io_buf_size as Lsn,
            segment_lsn,
            "segment lsn is unaligned! fix above lsn statement..."
        );

        trace!(
            "in advance_snapshot looking at item: segment lsn {} lsn {} lid {}",
            segment_lsn,
            lsn,
            log_id
        );

        if lsn <= snapshot.max_lsn {
            // don't process already-processed Lsn's. max_lsn is for the last
            // item ALREADY INCLUDED lsn in the snapshot.
            trace!(
                "continuing in advance_snapshot, lsn {} log_id {} max_lsn {}",
                lsn,
                log_id,
                snapshot.max_lsn
            );
            continue;
        }

        assert!(lsn > snapshot.max_lsn);
        snapshot.max_lsn = lsn;
        snapshot.last_lid = log_id;

        // invalidate any removed pids
        snapshot.replacements.remove(&segment_idx);

        if let Some(ref materializer) = materializer_opt {
            snapshot.apply(materializer, lsn, log_id, &*bytes, io_buf_size);
        }
    }

    write_snapshot(config, &snapshot);

    trace!("generated new snapshot: {:?}", snapshot);

    M.advance_snapshot.measure(clock() - start);

    snapshot
}

/// Read a `Snapshot` or generate a default, then advance it to
/// the tip of the data file, if present.
pub fn read_snapshot_or_default<P, R>(
    config: &FinalConfig,
    pm_opt: Option<Arc<Materializer<PageFrag = P, Recovery = R>>>,
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
    let last_snap = read_snapshot(config).unwrap_or_else(Snapshot::default);

    let log_iter = raw_segment_iter(config);

    advance_snapshot::<P, R>(log_iter, last_snap, pm_opt, config)
}

fn read_snapshot<R>(config: &FinalConfig) -> Option<Snapshot<R>>
    where R: Debug + Clone + Serialize + DeserializeOwned + Send
{
    let mut candidates = config.get_snapshot_files();
    if candidates.is_empty() {
        info!("no previous snapshot found");
        return None;
    }

    // TODO use parsed max lsn in filename
    candidates.sort_by_key(|path| {
        std::fs::metadata(path).unwrap().created().unwrap()
    });

    let path = candidates.pop().unwrap();

    let mut f = std::fs::OpenOptions::new().read(true).open(&path).unwrap();
    if f.metadata().unwrap().len() <= 16 {
        warn!("empty/corrupt snapshot file found");
        return None;
    }

    let mut buf = vec![];
    f.read_to_end(&mut buf).unwrap();
    let len = buf.len();
    buf.split_off(len - 16);

    let mut len_expected_bytes = [0u8; 8];
    f.seek(std::io::SeekFrom::End(-16)).unwrap();
    f.read_exact(&mut len_expected_bytes).unwrap();

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
        let len_expected: u64 =
            unsafe { std::mem::transmute(len_expected_bytes) };
        decompress(&*buf, len_expected as usize).unwrap()
    } else {
        buf
    };

    #[cfg(not(feature = "zstd"))]
    let bytes = buf;

    let snapshot = deserialize::<Snapshot<R>>(&*bytes).unwrap();

    Some(snapshot)
}

pub fn write_snapshot<R>(config: &FinalConfig, snapshot: &Snapshot<R>)
    where R: Debug + Clone + Serialize + DeserializeOwned + Send
{
    let raw_bytes = serialize(&snapshot, Infinite).unwrap();
    let decompressed_len = raw_bytes.len();

    #[cfg(feature = "zstd")]
    let bytes = if config.get_use_compression() {
        compress(&*raw_bytes, config.get_zstd_compression_factor()).unwrap()
    } else {
        raw_bytes
    };

    #[cfg(not(feature = "zstd"))]
    let bytes = raw_bytes;

    let crc64: [u8; 8] = unsafe { std::mem::transmute(crc64(&*bytes)) };
    let len_bytes: [u8; 8] =
        unsafe { std::mem::transmute(decompressed_len as u64) };

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
    f.write_all(&len_bytes).unwrap();
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
