use std::io::{Read, Write};

#[cfg(feature = "zstd")]
use zstd::block::{compress, decompress};

use super::*;

/// A snapshot of the state required to quickly restart
/// the `PageCache` and `SegmentAccountant`.
/// TODO consider splitting `Snapshot` into separate
/// snapshots for PC, SA, Materializer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    /// the last lsn included in the `Snapshot`
    pub max_lsn: Lsn,
    /// the last lid included in the `Snapshot`
    pub last_lid: LogId,
    /// the highest lid observed while generating the `Snapshot`
    pub max_lid: LogId,
    /// the highest allocated pid
    pub max_pid: PageId,
    /// the mapping from pages to (lsn, lid)
    pub pt: FastMap8<PageId, PageState>,
    /// replaced pages per segment index
    pub replacements:
        FastMap8<SegmentId, (Lsn, FastSet8<(PageId, SegmentId)>)>,
    /// the free pids
    pub free: FastSet8<PageId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PageState {
    Present(Vec<(Lsn, DiskPtr)>),
    Free(Lsn, DiskPtr),
}

impl PageState {
    fn push(&mut self, item: (Lsn, DiskPtr)) {
        match *self {
            PageState::Present(ref mut items) => items.push(item),
            PageState::Free(_, _) => {
                panic!("pushed items to a PageState::Free")
            }
        }
    }

    /// Iterate over the (lsn, lid) pairs that hold this page's state.
    pub fn iter(&self) -> Box<dyn Iterator<Item = (Lsn, DiskPtr)>> {
        match *self {
            PageState::Present(ref items) => {
                Box::new(items.clone().into_iter())
            }
            PageState::Free(lsn, ptr) => {
                Box::new(vec![(lsn, ptr)].into_iter())
            }
        }
    }

    fn is_free(&self) -> bool {
        match *self {
            PageState::Free(_, _) => true,
            _ => false,
        }
    }
}

impl Default for Snapshot {
    fn default() -> Snapshot {
        Snapshot {
            max_lsn: 0,
            max_lid: 0,
            last_lid: 0,
            max_pid: 0,
            pt: FastMap8::default(),
            replacements: FastMap8::default(),
            free: FastSet8::default(),
        }
    }
}

impl Snapshot {
    fn apply<P>(
        &mut self,
        lsn: Lsn,
        disk_ptr: DiskPtr,
        bytes: &[u8],
        config: &Config,
    ) -> Result<()>
    where
        P: 'static
            + Debug
            + Clone
            + Serialize
            + DeserializeOwned
            + Send
            + Sync,
    {
        // unwrapping this because it's already passed the crc check
        // in the log iterator
        trace!(
            "trying to deserialize buf for ptr {} lsn {}",
            disk_ptr,
            lsn
        );
        let deserialization = deserialize::<LoggedUpdate<P>>(&*bytes);

        if let Err(e) = deserialization {
            error!(
                "failed to deserialize buffer for item in log: lsn {} \
                    ptr {}: {:?}",
                lsn,
                disk_ptr,
                e
            );
            return Err(Error::Corruption { at: disk_ptr });
        }

        let prepend = deserialization.unwrap();
        let pid = prepend.pid;

        if pid >= self.max_pid {
            self.max_pid = pid + 1;
        }

        let replaced_at_idx =
            disk_ptr.lid() as SegmentId / config.io_buf_size;
        let replaced_at_segment_lsn = (lsn
            / config.io_buf_size as Lsn)
            * config.io_buf_size as Lsn;

        match prepend.update {
            Update::Append(append) => {
                // Because we rewrite pages over time, we may have relocated
                // a page's initial Compact to a later segment. We should skip
                // over pages here unless we've encountered a Compact for them.
                if let Some(lids) = self.pt.get_mut(&pid) {
                    trace!(
                        "append of pid {} at lid {} lsn {}: {:?}",
                        pid,
                        disk_ptr,
                        lsn,
                        append,
                    );

                    if lids.is_free() {
                        // this can happen if the allocate or replace
                        // has been moved to a later segment.

                        trace!(
                            "we have not yet encountered an \
                             allocation of this page, skipping push"
                        );

                        return Ok(());
                    }

                    lids.push((lsn, disk_ptr));
                }
                self.free.remove(&pid);
            }
            Update::Meta(_)
            | Update::Counter(_)
            | Update::Compact(_) => {
                trace!(
                    "compact of pid {} at ptr {} lsn {}: {:?}",
                    pid,
                    disk_ptr,
                    lsn,
                    prepend.update,
                );

                self.replace_pid(
                    pid,
                    replaced_at_segment_lsn,
                    replaced_at_idx,
                    config,
                );
                self.pt.insert(
                    pid,
                    PageState::Present(vec![(lsn, disk_ptr)]),
                );
                if prepend.update.is_compact() {
                    self.free.remove(&pid);
                } else {
                    assert!(!self.free.contains(&pid));
                }
            }
            Update::Free => {
                trace!(
                    "free of pid {} at ptr {} lsn {}",
                    pid,
                    disk_ptr,
                    lsn
                );
                self.replace_pid(
                    pid,
                    replaced_at_segment_lsn,
                    replaced_at_idx,
                    config,
                );
                self.pt.insert(pid, PageState::Free(lsn, disk_ptr));
                self.free.insert(pid);
            }
        }

        Ok(())
    }

    fn replace_pid(
        &mut self,
        pid: PageId,
        replaced_at_segment_lsn: Lsn,
        replaced_at_idx: usize,
        config: &Config,
    ) {
        let replacements =
            self.replacements.entry(replaced_at_idx).or_insert((
                replaced_at_segment_lsn,
                FastSet8::default(),
            ));

        assert_eq!(replaced_at_segment_lsn, replacements.0);

        match self.pt.remove(&pid) {
            Some(PageState::Present(coords)) => {
                for (_lsn, ptr) in &coords {
                    let old_idx =
                        ptr.lid() as SegmentId / config.io_buf_size;
                    if replaced_at_idx == old_idx {
                        continue;
                    }
                    replacements.1.insert((pid, old_idx));
                }

                // re-run any blob removals in case
                // they were not completed. blobs are
                // not rewritten if they are the only
                // frag for a page during rewrite.
                if coords.len() > 1 {
                    let blob_ptrs = coords
                        .iter()
                        .filter(|(_, ptr)| ptr.is_blob())
                        .map(|(_, ptr)| ptr.blob().1);

                    for blob_ptr in blob_ptrs {
                        trace!(
                            "removing blob while advancing \
                             snapshot: {}",
                            blob_ptr,
                        );

                        // we don't care if this actually works
                        // because it's possible that a previous
                        // snapshot has run over this log and
                        // removed the blob already.
                        let _ = remove_blob(blob_ptr, config);
                    }
                }
            }
            Some(PageState::Free(_lsn, ptr)) => {
                let old_idx =
                    ptr.lid() as SegmentId / config.io_buf_size;
                if replaced_at_idx != old_idx {
                    replacements.1.insert((pid, old_idx));
                }
            }
            None => {
                // we just encountered a replace without it
                // existing in the pt. this means we've
                // relocated it to a later segment than
                // the one we're currently at during recovery.
                // so we can skip it here.
            }
        }
    }
}

pub(super) fn advance_snapshot<PM, P>(
    iter: LogIter,
    mut snapshot: Snapshot,
    config: &Config,
) -> Result<Snapshot>
where
    PM: Materializer<PageFrag = P>,
    P: 'static
        + Debug
        + Clone
        + Serialize
        + DeserializeOwned
        + Send
        + Sync,
{
    let _measure = Measure::new(&M.advance_snapshot);

    trace!("building on top of old snapshot: {:?}", snapshot);

    let io_buf_size = config.io_buf_size;

    let mut last_seg_lsn = snapshot.max_lsn / io_buf_size as Lsn;

    for (lsn, ptr, bytes) in iter {
        trace!(
            "in advance_snapshot looking at item with lsn {} ptr {}",
            lsn,
            ptr
        );

        if lsn <= snapshot.max_lsn {
            // don't process already-processed Lsn's. max_lsn is for the last
            // item ALREADY INCLUDED lsn in the snapshot.
            trace!(
                "continuing in advance_snapshot, lsn {} ptr {} max_lsn {}",
                lsn,
                ptr,
                snapshot.max_lsn
            );
            continue;
        }

        assert!(lsn > snapshot.max_lsn);
        snapshot.max_lsn = lsn;
        snapshot.last_lid = ptr.lid();
        if ptr.lid() > snapshot.max_lid {
            snapshot.max_lid = ptr.lid();
        }

        if lsn / (io_buf_size as Lsn) > last_seg_lsn {
            // invalidate any removed pids
            let segment_idx = ptr.lid() as usize / io_buf_size;

            snapshot.replacements.remove(&segment_idx);
            last_seg_lsn = lsn / (io_buf_size as Lsn);
        }

        if !PM::is_null() {
            if let Err(e) =
                snapshot.apply::<P>(lsn, ptr, &*bytes, config)
            {
                error!(
                    "encountered error while reading log message: {}",
                    e
                );
                break;
            }
        }
    }

    write_snapshot(config, &snapshot)?;

    trace!("generated new snapshot: {:?}", snapshot);

    Ok(snapshot)
}

/// Read a `Snapshot` or generate a default, then advance it to
/// the tip of the data file, if present.
pub fn read_snapshot_or_default<PM, P>(
    config: &Config,
) -> Result<Snapshot>
where
    PM: Materializer<PageFrag = P>,
    P: 'static
        + Debug
        + Clone
        + Serialize
        + DeserializeOwned
        + Send
        + Sync,
{
    let last_snap =
        read_snapshot(config)?.unwrap_or_else(Snapshot::default);

    let log_iter = raw_segment_iter_from(last_snap.max_lsn, config)?;

    advance_snapshot::<PM, P>(log_iter, last_snap, config)
}

/// Read a `Snapshot` from disk.
fn read_snapshot(
    config: &Config,
) -> std::io::Result<Option<Snapshot>> {
    let mut candidates = config.get_snapshot_files()?;
    if candidates.is_empty() {
        debug!("no previous snapshot found");
        return Ok(None);
    }

    candidates.sort();

    let path = candidates.pop().unwrap();

    let mut f = std::fs::OpenOptions::new().read(true).open(&path)?;
    if f.metadata()?.len() <= 12 {
        warn!("empty/corrupt snapshot file found");
        return Ok(None);
    }

    let mut buf = vec![];
    f.read_to_end(&mut buf)?;
    let len = buf.len();
    let mut len_expected_bytes = [0u8; 8];
    len_expected_bytes.copy_from_slice(&buf[len - 12..len - 4]);

    let mut crc_expected_bytes = [0u8; 4];
    crc_expected_bytes.copy_from_slice(&buf[len - 4..]);

    buf.split_off(len - 12);
    let crc_expected: u32 = arr_to_u32(&crc_expected_bytes);

    let crc_actual = crc32(&buf);

    if crc_expected != crc_actual {
        return Ok(None);
    }

    #[cfg(feature = "zstd")]
    let bytes = if config.use_compression {
        let len_expected: u64 = arr_to_u64(&len_expected_bytes);
        decompress(&*buf, len_expected as usize).unwrap()
    } else {
        buf
    };

    #[cfg(not(feature = "zstd"))]
    let bytes = buf;

    Ok(deserialize::<Snapshot>(&*bytes).ok())
}

pub(crate) fn write_snapshot(
    config: &Config,
    snapshot: &Snapshot,
) -> Result<()> {
    let raw_bytes = serialize(&snapshot).unwrap();
    let decompressed_len = raw_bytes.len();

    #[cfg(feature = "zstd")]
    let bytes = if config.use_compression {
        compress(&*raw_bytes, config.compression_factor).unwrap()
    } else {
        raw_bytes
    };

    #[cfg(not(feature = "zstd"))]
    let bytes = raw_bytes;

    let crc32: [u8; 4] = u32_to_arr(crc32(&bytes));
    let len_bytes: [u8; 8] = u64_to_arr(decompressed_len as u64);

    let path_1_suffix =
        format!("snap.{:016X}.in___motion", snapshot.max_lsn);

    let mut path_1 = config.snapshot_prefix();
    path_1.push(path_1_suffix);

    let path_2_suffix = format!("snap.{:016X}", snapshot.max_lsn);

    let mut path_2 = config.snapshot_prefix();
    path_2.push(path_2_suffix);

    let _res = std::fs::create_dir_all(path_1.parent().unwrap());
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&path_1)?;

    // write the snapshot bytes, followed by a crc64 checksum at the end
    maybe_fail!("snap write");
    f.write_all(&*bytes)?;
    maybe_fail!("snap write len");
    f.write_all(&len_bytes)?;
    maybe_fail!("snap write crc");
    f.write_all(&crc32)?;
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
