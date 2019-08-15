use std::io::{Read, Write};

#[cfg(feature = "zstd")]
use zstd::block::{compress, decompress};

use super::*;

/// A snapshot of the state required to quickly restart
/// the `PageCache` and `SegmentAccountant`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Snapshot {
    /// The last read message lsn
    pub last_lsn: Lsn,
    /// The last read message lid
    pub last_lid: LogId,
    /// the mapping from pages to (lsn, lid)
    pub pt: FastMap8<PageId, PageState>,
    /// The highest stable offset persisted
    /// into a segment header
    pub max_header_stable_lsn: Lsn,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PageState {
    Present(Vec<(Lsn, DiskPtr, usize)>),
    Free(Lsn, DiskPtr),
}

impl PageState {
    fn push(&mut self, item: (Lsn, DiskPtr, usize)) {
        match *self {
            Self::Present(ref mut items) => items.push(item),
            Self::Free(_, _) => {
                panic!("pushed items to a PageState::Free")
            }
        }
    }

    /// Iterate over the (lsn, lid) pairs that hold this page's state.
    pub fn iter(&self) -> impl Iterator<Item = (Lsn, DiskPtr, usize)> {
        match *self {
            Self::Present(ref items) => items.clone().into_iter(),
            Self::Free(lsn, ptr) => {
                vec![(lsn, ptr, MSG_HEADER_LEN)].into_iter()
            }
        }
    }

    fn is_free(&self) -> bool {
        match *self {
            Self::Free(_, _) => true,
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
        sz: usize,
    ) {
        // unwrapping this because it's already passed the crc check
        // in the log iterator
        trace!("trying to deserialize buf for ptr {} lsn {}", disk_ptr, lsn);

        match log_kind {
            LogKind::Replace => {
                trace!(
                    "compact of pid {} at ptr {} lsn {}",
                    pid,
                    disk_ptr,
                    lsn,
                );

                self.pt
                    .insert(pid, PageState::Present(vec![(lsn, disk_ptr, sz)]));
            }
            LogKind::Append => {
                // Because we rewrite pages over time, we may have relocated
                // a page's initial Compact to a later segment. We should skip
                // over pages here unless we've encountered a Compact for them.
                if let Some(lids) = self.pt.get_mut(&pid) {
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
                }
            }
            LogKind::Free => {
                trace!("free of pid {} at ptr {} lsn {}", pid, disk_ptr, lsn);
                self.pt.insert(pid, PageState::Free(lsn, disk_ptr));
            }
            LogKind::Corrupted | LogKind::Skip => panic!(
                "unexppected messagekind in snapshot application: {:?}",
                log_kind
            ),
        }
    }
}

pub(super) fn advance_snapshot(
    iter: LogIter,
    mut snapshot: Snapshot,
    config: &Config,
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

    Ok(snapshot)
}

/// Read a `Snapshot` or generate a default, then advance it to
/// the tip of the data file, if present.
pub fn read_snapshot_or_default(config: &Config) -> Result<Snapshot> {
    let mut last_snap =
        read_snapshot(config)?.unwrap_or_else(Snapshot::default);

    let (log_iter, max_header_stable_lsn) =
        raw_segment_iter_from(last_snap.last_lsn, config)?;

    last_snap.max_header_stable_lsn = max_header_stable_lsn;

    advance_snapshot(log_iter, last_snap, config)
}

/// Read a `Snapshot` from disk.
fn read_snapshot(config: &Config) -> std::io::Result<Option<Snapshot>> {
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
    f.read_to_end(&mut buf)?;
    let len = buf.len();
    let mut len_expected_bytes = [0; 8];
    len_expected_bytes.copy_from_slice(&buf[len - 12..len - 4]);

    let mut crc_expected_bytes = [0; 4];
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

fn write_snapshot(config: &Config, snapshot: &Snapshot) -> Result<()> {
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

    let path_1_suffix = format!("snap.{:016X}.generating", snapshot.last_lsn);

    let mut path_1 = config.snapshot_prefix();
    path_1.push(path_1_suffix);

    let path_2_suffix = format!("snap.{:016X}", snapshot.last_lsn);

    let mut path_2 = config.snapshot_prefix();
    path_2.push(path_2_suffix);

    let parent = path_1.parent().unwrap();
    std::fs::create_dir_all(parent)?;
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
    maybe_fail!("snap write post");

    trace!("wrote snapshot to {}", path_1.to_string_lossy());

    maybe_fail!("snap write mv");
    std::fs::rename(&path_1, &path_2)?;
    maybe_fail!("snap write mv post");

    trace!("renamed snapshot to {}", path_2.to_string_lossy());

    // clean up any old snapshots
    let candidates = config.get_snapshot_files()?;
    for path in candidates {
        let path_str = path.file_name().unwrap().to_str().unwrap();
        if !path_2.to_string_lossy().ends_with(&*path_str) {
            debug!("removing old snapshot file {:?}", path);

            maybe_fail!("snap write rm old");

            if let Err(e) = std::fs::remove_file(&path) {
                // TODO should this just be a try return?
                warn!("failed to remove old snapshot file, maybe snapshot race? {}", e);
            }
        }
    }
    Ok(())
}
