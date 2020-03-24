#[cfg(feature = "zstd")]
use zstd::block::{compress, decompress};

use crate::*;

use super::{
    arr_to_u32, pwrite_all, raw_segment_iter_from, u32_to_arr, u64_to_arr,
    DiskPtr, LogIter, LogKind, LogOffset, Lsn, MessageKind, MAX_MSG_HEADER_LEN,
};

/// A snapshot of the state required to quickly restart
/// the `PageCache` and `SegmentAccountant`.
#[derive(Debug, Default)]
#[cfg_attr(test, derive(Clone, PartialEq))]
pub struct Snapshot {
    /// The last read message lsn
    pub last_lsn: Lsn,
    /// The last read message lid
    pub last_lid: LogOffset,
    /// The highest stable offset persisted
    /// into a segment header
    pub max_header_stable_lsn: Lsn,
    /// the mapping from pages to (lsn, lid)
    pub pt: Vec<PageState>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PageState {
    Present(Vec<(Lsn, DiskPtr, u64)>),
    Free(Lsn, DiskPtr),
    Uninitialized,
}

impl PageState {
    fn push(&mut self, item: (Lsn, DiskPtr, u64)) {
        match *self {
            PageState::Present(ref mut items) => items.push(item),
            _ => panic!("pushed items to {:?}", self),
        }
    }

    /// Iterate over the (lsn, lid) pairs that hold this page's state.
    pub fn iter(&self) -> impl Iterator<Item = (Lsn, DiskPtr, u64)> {
        match *self {
            PageState::Present(ref items) => items.clone().into_iter(),
            PageState::Free(lsn, ptr) => {
                vec![(lsn, ptr, u64::try_from(MAX_MSG_HEADER_LEN).unwrap())]
                    .into_iter()
            }
            PageState::Uninitialized => panic!("canned iter on a {:?}", self),
        }
    }

    pub(crate) fn is_free(&self) -> bool {
        match *self {
            PageState::Free(_, _) => true,
            _ => false,
        }
    }
}

impl Snapshot {
    fn apply(
        &mut self,
        log_kind: LogKind,
        pid: PageId,
        lsn: Lsn,
        disk_ptr: DiskPtr,
        sz: u64,
    ) {
        // unwrapping this because it's already passed the crc check
        // in the log iterator
        trace!("trying to deserialize buf for ptr {} lsn {}", disk_ptr, lsn);
        let _measure = Measure::new(&M.snapshot_apply);

        if self.pt.len() <= usize::try_from(pid).unwrap() {
            self.pt.resize(
                usize::try_from(pid + 1).unwrap(),
                PageState::Uninitialized,
            );
        }

        match log_kind {
            LogKind::Replace => {
                trace!(
                    "compact of pid {} at ptr {} lsn {}",
                    pid,
                    disk_ptr,
                    lsn,
                );

                self.pt[usize::try_from(pid).unwrap()] =
                    PageState::Present(vec![(lsn, disk_ptr, sz)]);
            }
            LogKind::Link => {
                // Because we rewrite pages over time, we may have relocated
                // a page's initial Compact to a later segment. We should skip
                // over pages here unless we've encountered a Compact for them.
                if let Some(lids @ PageState::Present(_)) =
                    self.pt.get_mut(usize::try_from(pid).unwrap())
                {
                    trace!(
                        "append of pid {} at lid {} lsn {}",
                        pid,
                        disk_ptr,
                        lsn,
                    );

                    if lids.is_free() {
                        // this can happen if the allocate or replace
                        // has been moved to a later segment.

                        trace!(
                            "we have not yet encountered an \
                             allocation of this page, skipping push"
                        );

                        return;
                    }

                    lids.push((lsn, disk_ptr, sz));
                } else {
                    trace!(
                        "skipping dangling append of pid {} at lid {} lsn {}",
                        pid,
                        disk_ptr,
                        lsn,
                    );
                }
            }
            LogKind::Free => {
                trace!("free of pid {} at ptr {} lsn {}", pid, disk_ptr, lsn);
                self.pt[usize::try_from(pid).unwrap()] =
                    PageState::Free(lsn, disk_ptr);
            }
            LogKind::Corrupted | LogKind::Skip => panic!(
                "unexppected messagekind in snapshot application: {:?}",
                log_kind
            ),
        }
    }
}

pub(crate) fn advance_snapshot(
    iter: LogIter,
    mut snapshot: Snapshot,
    config: &RunningConfig,
) -> Result<Snapshot> {
    let _measure = Measure::new(&M.advance_snapshot);

    trace!("building on top of old snapshot: {:?}", snapshot);

    let old_lsn = snapshot.last_lsn;

    for (log_kind, pid, lsn, ptr, sz) in iter {
        trace!(
            "in advance_snapshot looking at item with lsn {} ptr {}",
            lsn,
            ptr
        );

        if lsn <= snapshot.last_lsn {
            // don't process already-processed Lsn's. last_lsn is for the last
            // item ALREADY INCLUDED lsn in the snapshot.
            trace!(
                "continuing in advance_snapshot, lsn {} ptr {} last_lsn {}",
                lsn,
                ptr,
                snapshot.last_lsn
            );
            continue;
        }

        assert!(lsn > snapshot.last_lsn);
        snapshot.last_lsn = lsn;
        snapshot.last_lid = ptr.lid();

        snapshot.apply(log_kind, pid, lsn, ptr, sz);
    }

    if snapshot.last_lsn != old_lsn {
        write_snapshot(config, &snapshot)?;
    }

    trace!("generated new snapshot: {:?}", snapshot);

    #[cfg(feature = "event_log")]
    config.event_log.recovered_lsn(snapshot.last_lsn);

    Ok(snapshot)
}

/// Read a `Snapshot` or generate a default, then advance it to
/// the tip of the data file, if present.
pub fn read_snapshot_or_default(config: &RunningConfig) -> Result<Snapshot> {
    let mut last_snap =
        read_snapshot(config)?.unwrap_or_else(Snapshot::default);

    let (log_iter, max_header_stable_lsn, to_zero) =
        raw_segment_iter_from(last_snap.last_lsn, config)?;

    last_snap.max_header_stable_lsn = max_header_stable_lsn;

    let res = advance_snapshot(log_iter, last_snap, config)?;

    for lid in to_zero {
        debug!("zeroing torn segment at lid {}", lid);

        // NB we intentionally corrupt this header to prevent any segment
        // from being allocated which would duplicate its LSN, messing
        // up recovery in the future.
        io_fail!(config, "segment initial free zero");
        pwrite_all(
            &config.file,
            &*vec![MessageKind::Corrupted.into(); SEG_HEADER_LEN],
            lid,
        )?;
        if !config.temporary {
            config.file.sync_all()?;
        }
    }

    Ok(res)
}

/// Read a `Snapshot` from disk.
fn read_snapshot(config: &RunningConfig) -> std::io::Result<Option<Snapshot>> {
    let mut f = loop {
        let mut candidates = config.get_snapshot_files()?;
        if candidates.is_empty() {
            debug!("no previous snapshot found");
            return Ok(None);
        }

        candidates.sort();

        let path = candidates.pop().unwrap();

        match std::fs::OpenOptions::new().read(true).open(&path) {
            Ok(f) => break f,
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {
                // this can happen if there's a race
                continue;
            }
            Err(other) => return Err(other),
        }
    };

    if f.metadata()?.len() <= 12 {
        warn!("empty/corrupt snapshot file found");
        return Ok(None);
    }

    let mut buf = vec![];
    let _read = f.read_to_end(&mut buf)?;
    let len = buf.len();
    let mut len_expected_bytes = [0; 8];
    len_expected_bytes.copy_from_slice(&buf[len - 12..len - 4]);

    let mut crc_expected_bytes = [0; 4];
    crc_expected_bytes.copy_from_slice(&buf[len - 4..]);

    let _ = buf.split_off(len - 12);
    let crc_expected: u32 = arr_to_u32(&crc_expected_bytes);

    let crc_actual = crc32(&buf);

    if crc_expected != crc_actual {
        return Ok(None);
    }

    #[cfg(feature = "zstd")]
    let bytes = if config.use_compression {
        use std::convert::TryInto;

        let len_expected: u64 =
            u64::from_le_bytes(len_expected_bytes.as_ref().try_into().unwrap());

        decompress(&*buf, usize::try_from(len_expected).unwrap()).unwrap()
    } else {
        buf
    };

    #[cfg(not(feature = "zstd"))]
    let bytes = buf;

    Ok(Snapshot::deserialize(&mut bytes.as_slice()).ok())
}

fn write_snapshot(config: &RunningConfig, snapshot: &Snapshot) -> Result<()> {
    let raw_bytes = snapshot.serialize();
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

    let path_1_suffix = format!("snap.{:016X}.generating", snapshot.last_lsn);

    let mut path_1 = config.get_path();
    path_1.push(path_1_suffix);

    let path_2_suffix = format!("snap.{:016X}", snapshot.last_lsn);

    let mut path_2 = config.get_path();
    path_2.push(path_2_suffix);

    let parent = path_1.parent().unwrap();
    std::fs::create_dir_all(parent)?;
    let mut f =
        std::fs::OpenOptions::new().write(true).create(true).open(&path_1)?;

    // write the snapshot bytes, followed by a crc64 checksum at the end
    io_fail!(config, "snap write");
    f.write_all(&*bytes)?;
    io_fail!(config, "snap write len");
    f.write_all(&len_bytes)?;
    io_fail!(config, "snap write crc");
    f.write_all(&crc32)?;
    io_fail!(config, "snap write post");

    trace!("wrote snapshot to {}", path_1.to_string_lossy());

    io_fail!(config, "snap write mv");
    std::fs::rename(&path_1, &path_2)?;
    io_fail!(config, "snap write mv post");

    trace!("renamed snapshot to {}", path_2.to_string_lossy());

    // clean up any old snapshots
    let candidates = config.get_snapshot_files()?;
    for path in candidates {
        let path_str = path.file_name().unwrap().to_str().unwrap();
        if !path_2.to_string_lossy().ends_with(&*path_str) {
            debug!("removing old snapshot file {:?}", path);

            io_fail!(config, "snap write rm old");

            if let Err(e) = std::fs::remove_file(&path) {
                // TODO should this just be a try return?
                warn!(
                    "failed to remove old snapshot file, maybe snapshot race? {}",
                    e
                );
            }
        }
    }
    Ok(())
}
