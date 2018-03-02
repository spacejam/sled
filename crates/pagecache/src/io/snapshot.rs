use std::collections::{HashMap, HashSet};
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
    pub pt: HashMap<PageID, PageState>,
    /// replaced pages per segment index
    pub replacements: HashMap<SegmentID, HashSet<(PageID, Lsn)>>,
    /// the free pids
    pub free: HashSet<PageID>,
    /// the `Materializer`-specific recovered state
    pub recovery: Option<R>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PageState {
    Present(Vec<(Lsn, LogID)>),
    Allocated(Lsn, LogID),
    Free(Lsn, LogID),
}

impl PageState {
    fn push(&mut self, item: (Lsn, LogID)) {
        match self {
            &mut PageState::Present(ref mut items) => items.push(item),
            &mut PageState::Allocated(_, _) => {
                *self = PageState::Present(vec![item])
            }
            &mut PageState::Free(_, _) => {
                panic!("pushed items to a PageState::Free")
            }
        }
    }

    /// Iterate over the (lsn, lid) pairs that hold this page's state.
    pub fn iter(&self) -> Box<Iterator<Item = (Lsn, LogID)>> {
        match self {
            &PageState::Present(ref items) => Box::new(
                items.clone().into_iter(),
            ),
            &PageState::Allocated(lsn, lid) |
            &PageState::Free(lsn, lid) => {
                Box::new(vec![(lsn, lid)].into_iter())
            }
        }
    }
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
        materializer: &Materializer<PageFrag = P, Recovery = R>,
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

        let replaced_at_idx = log_id as SegmentID / io_buf_size;

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
                if let Some(r) = materializer.recover(&partial_page) {
                    self.recovery = Some(r);
                }

                self.replace_pid(pid, replaced_at_idx, lsn, io_buf_size);
                self.pt.insert(pid, PageState::Present(vec![(lsn, log_id)]));
                self.free.remove(&pid);
            }
            Update::Allocate => {
                trace!(
                    "allocate  of pid {} at lid {} lsn {}",
                    pid,
                    log_id,
                    lsn
                );
                self.replace_pid(pid, replaced_at_idx, lsn, io_buf_size);
                self.pt.insert(pid, PageState::Allocated(lsn, log_id));
                self.free.remove(&pid);
            }
            Update::Free => {
                trace!("free of pid {} at lid {} lsn {}", pid, log_id, lsn);
                self.replace_pid(pid, replaced_at_idx, lsn, io_buf_size);
                self.pt.insert(pid, PageState::Free(lsn, log_id));
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
        match self.pt.remove(&pid) {
            Some(PageState::Present(coords)) => {
                for (_lsn, lid) in coords {
                    let idx = lid as SegmentID / io_buf_size;
                    if replaced_at_idx == idx {
                        return;
                    }
                    let entry =
                        self.replacements.entry(idx).or_insert(HashSet::new());
                    entry.insert((pid, replaced_at_lsn));
                }
            }
            Some(PageState::Allocated(_lsn, lid)) |
            Some(PageState::Free(_lsn, lid)) => {

                let idx = lid as SegmentID / io_buf_size;
                if replaced_at_idx == idx {
                    return;
                }
                let entry =
                    self.replacements.entry(idx).or_insert(HashSet::new());
                entry.insert((pid, replaced_at_lsn));
            }
            None => {}
        }
    }
}

pub(super) fn advance_snapshot<PM, P, R>(
    iter: LogIter,
    mut snapshot: Snapshot<R>,
    config: &Config,
) -> CacheResult<Snapshot<R>, ()>
    where PM: Materializer<Recovery = R, PageFrag = P>,
          P: 'static
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

    let materializer = PM::new(config.clone(), &snapshot.recovery);

    let io_buf_size = config.io_buf_size;

    for (lsn, log_id, bytes) in iter {
        let segment_idx = log_id as SegmentID / io_buf_size;

        trace!(
            "in advance_snapshot looking at item with lsn {} lid {}",
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

        if !PM::is_null() {
            snapshot.apply(&materializer, lsn, log_id, &*bytes, io_buf_size);
        }
    }

    write_snapshot(config, &snapshot)?;

    trace!("generated new snapshot: {:?}", snapshot);

    M.advance_snapshot.measure(clock() - start);

    Ok(snapshot)
}

/// Read a `Snapshot` or generate a default, then advance it to
/// the tip of the data file, if present.
pub fn read_snapshot_or_default<PM, P, R>(
    config: &Config,
) -> CacheResult<Snapshot<R>, ()>
    where PM: Materializer<Recovery = R, PageFrag = P>,
          P: 'static
                 + Debug
                 + Clone
                 + Serialize
                 + DeserializeOwned
                 + Send
                 + Sync,
          R: Debug + Clone + Serialize + DeserializeOwned + Send
{
    let last_snap = read_snapshot(config)?.unwrap_or_else(Snapshot::default);

    let log_iter = raw_segment_iter_from(last_snap.max_lsn, config)?;

    advance_snapshot::<PM, P, R>(log_iter, last_snap, config)
}

/// Read a `Snapshot` from disk.
fn read_snapshot<R>(config: &Config) -> std::io::Result<Option<Snapshot<R>>>
    where R: Debug + Clone + Serialize + DeserializeOwned + Send
{
    let mut candidates = config.get_snapshot_files()?;
    if candidates.is_empty() {
        info!("no previous snapshot found");
        return Ok(None);
    }

    candidates.sort();

    let path = candidates.pop().unwrap();

    let mut f = std::fs::OpenOptions::new().read(true).open(&path)?;
    if f.metadata()?.len() <= 16 {
        warn!("empty/corrupt snapshot file found");
        return Ok(None);
    }

    let mut buf = vec![];
    f.read_to_end(&mut buf)?;
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
        error!("crc for snapshot file {:?} failed!", path);
        return Ok(None);
    }

    #[cfg(feature = "zstd")]
    let bytes = if config.use_compression {
        let len_expected: u64 =
            unsafe { std::mem::transmute(len_expected_bytes) };
        decompress(&*buf, len_expected as usize).unwrap()
    } else {
        buf
    };

    #[cfg(not(feature = "zstd"))]
    let bytes = buf;

    Ok(deserialize::<Snapshot<R>>(&*bytes).ok())
}

pub fn write_snapshot<R>(
    config: &Config,
    snapshot: &Snapshot<R>,
) -> CacheResult<(), ()>
    where R: Debug + Clone + Serialize + DeserializeOwned + Send
{
    let raw_bytes = serialize(&snapshot, Infinite).unwrap();
    let decompressed_len = raw_bytes.len();

    #[cfg(feature = "zstd")]
    let bytes = if config.use_compression {
        compress(&*raw_bytes, config.zstd_compression_factor).unwrap()
    } else {
        raw_bytes
    };

    #[cfg(not(feature = "zstd"))]
    let bytes = raw_bytes;

    let crc64: [u8; 8] = unsafe { std::mem::transmute(crc64(&*bytes)) };
    let len_bytes: [u8; 8] =
        unsafe { std::mem::transmute(decompressed_len as u64) };

    let path_1_suffix = format!(".snap.{:016X}.in___motion", snapshot.max_lsn);

    let mut path_1 = config.snapshot_prefix();
    path_1.push(path_1_suffix);

    let path_2_suffix = format!(".snap.{:016X}", snapshot.max_lsn);

    let mut path_2 = config.snapshot_prefix();
    path_2.push(path_2_suffix);

    let mut f = std::fs::OpenOptions::new().write(true).create(true).open(
        &path_1,
    )?;

    // write the snapshot bytes, followed by a crc64 checksum at the end
    maybe_fail!("snap write");
    f.write_all(&*bytes)?;
    maybe_fail!("snap write len");
    f.write_all(&len_bytes)?;
    maybe_fail!("snap write crc");
    f.write_all(&crc64)?;
    f.sync_all()?;
    maybe_fail!("snap write post");

    trace!("wrote snapshot to {}", path_1.to_string_lossy());

    maybe_fail!("snap write mv");
    std::fs::rename(path_1, &path_2)?;
    maybe_fail!("snap write mv post");

    trace!("renamed snapshot to {}", path_2.to_string_lossy());

    // clean up any old snapshots
    let candidates = config.get_snapshot_files()?;
    for path in candidates {
        let path_str = path.file_name().unwrap().to_str().unwrap();
        if !path_2.to_string_lossy().ends_with(&*path_str) {
            debug!("removing old snapshot file {:?}", path);

            maybe_fail!("snap write rm old");

            if let Err(_e) = std::fs::remove_file(&path) {
                // TODO should this just be a try return?
                warn!(
                    "failed to remove old snapshot file, maybe snapshot race? {}",
                    _e
                );
            }
        }
    }
    Ok(())
}
