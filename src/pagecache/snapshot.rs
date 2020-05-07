#[cfg(feature = "zstd")]
use zstd::block::{compress, decompress};

use crate::*;

use super::{
    arr_to_u32, gc_blobs, pwrite_all, raw_segment_iter_from, u32_to_arr,
    u64_to_arr, BasedBuf, DiskPtr, LogIter, LogKind, LogOffset, Lsn,
    MessageKind,
};

/// A snapshot of the state required to quickly restart
/// the `PageCache` and `SegmentAccountant`.
#[derive(Debug, Default)]
#[cfg_attr(test, derive(Clone, PartialEq))]
pub struct Snapshot {
    /// The last read message lsn
    pub stable_lsn: Lsn,
    /// The last read message lid
    pub tip_lid: Option<LogOffset>,
    /// the mapping from pages to (lsn, lid)
    pub pt: Vec<PageState>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PageState {
    Present { base: (Lsn, DiskPtr, u64), frags: Vec<(Lsn, DiskPtr, u64)> },
    Free(Lsn, DiskPtr),
    Uninitialized,
}

impl PageState {
    fn push(&mut self, item: (Lsn, DiskPtr, u64)) {
        match *self {
            PageState::Present { base, ref mut frags } => {
                if frags.last().map_or(base.0, |f| f.0) < item.0 {
                    frags.push(item)
                } else {
                    debug!(
                        "skipping merging item {:?} into \
                        existing PageState::Present({:?})",
                        item, frags
                    );
                }
            }
            _ => panic!("pushed frags to {:?}", self),
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
        trace!("trying to deserialize buf for ptr {} lsn {}", disk_ptr, lsn);
        let _measure = Measure::new(&M.snapshot_apply);

        let pushed = if self.pt.len() <= usize::try_from(pid).unwrap() {
            self.pt.resize(
                usize::try_from(pid + 1).unwrap(),
                PageState::Uninitialized,
            );
            true
        } else {
            false
        };

        match log_kind {
            LogKind::Replace => {
                trace!(
                    "compact of pid {} at ptr {} lsn {}",
                    pid,
                    disk_ptr,
                    lsn,
                );

                self.pt[usize::try_from(pid).unwrap()] = PageState::Present {
                    base: (lsn, disk_ptr, sz),
                    frags: vec![],
                };
            }
            LogKind::Link => {
                // Because we rewrite pages over time, we may have relocated
                // a page's initial Compact to a later segment. We should skip
                // over pages here unless we've encountered a Compact for them.
                if let Some(lids @ PageState::Present { .. }) =
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
                    if pushed {
                        let old = self.pt.pop().unwrap();
                        assert_eq!(old, PageState::Uninitialized);
                    }
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

fn advance_snapshot(
    mut iter: LogIter,
    mut snapshot: Snapshot,
    config: &RunningConfig,
) -> Result<Snapshot> {
    let _measure = Measure::new(&M.advance_snapshot);

    trace!("building on top of old snapshot: {:?}", snapshot);

    let old_stable_lsn = snapshot.stable_lsn;

    while let Some((log_kind, pid, lsn, ptr, sz)) = iter.next() {
        trace!(
            "in advance_snapshot looking at item with pid {} lsn {} ptr {}",
            pid,
            lsn,
            ptr
        );

        if lsn < snapshot.stable_lsn {
            // don't process already-processed Lsn's. stable_lsn is for the last
            // item ALREADY INCLUDED lsn in the snapshot.
            trace!(
                "continuing in advance_snapshot, lsn {} ptr {} stable_lsn {:?}",
                lsn,
                ptr,
                snapshot.stable_lsn
            );
            continue;
        }

        snapshot.apply(log_kind, pid, lsn, ptr, sz);
    }

    // `snapshot.tip_lid` can be set based on 4 possibilities for the tip of the
    // log:
    // 1. an empty DB - tip set to None, causing a fresh segment to be
    //    allocated on initialization
    // 2. the recovered tip is at the end of a
    //    segment with less space left than would fit MAX_MSG_HEADER_LEN -
    //    tip set to None, causing a fresh segment to be allocated on
    //    initialization, as in #1 above
    // 3. the recovered tip is in the middle of a segment - both set to the end
    //    of the last valid message, causing the system to be initialized to
    //    that point without allocating a new segment
    // 4. the recovered tip is at the beginning of a new segment, but without
    //    any valid messages in it yet. treat as #3 above, but also take care
    //    in te SA initialization to properly initialize any segment tracking
    //    state despite not having any pages currently residing there.
    //
    // The information we have at this point is iter.cur_lsn and the
    // iter.segment_base.offset. Situation 1 & 2 happen if iter.cur_lsn %
    // segment_size == 0, and result in None for the recovered log tip.
    // Situations 3 & 4 otherwise. We don't need to differentiate between them
    // here.

    let segment_progress: Lsn = iter.cur_lsn % (config.segment_size as Lsn);

    snapshot.stable_lsn = if segment_progress + MAX_MSG_HEADER_LEN as Lsn
        >= config.segment_size as Lsn
    {
        assert!(iter.segment_base.is_none());
        config.normalize(iter.cur_lsn) + config.segment_size as Lsn
    } else if iter.cur_lsn == 0 {
        snapshot.stable_lsn
    } else {
        iter.cur_lsn
    };

    if segment_progress == 0 || iter.segment_base.is_none() {
        // either situation 1 or situation 2
        snapshot.tip_lid = None;
    } else if let Some(BasedBuf { offset, .. }) = iter.segment_base {
        // either situation 3 or situation 4. we need to zero the

        snapshot.tip_lid = Some(offset + segment_progress as LogOffset);

        // zero the tail of the segment after the recovered tip
        let shred_len = config.segment_size - segment_progress as usize;
        let shred_zone = vec![MessageKind::Corrupted.into(); shred_len];
        debug!(
            "zeroing the end of the recovered segment between {} and {}",
            snapshot.tip_lid.unwrap(),
            snapshot.tip_lid.unwrap() + shred_len as LogOffset
        );
        pwrite_all(&config.file, &shred_zone, snapshot.tip_lid.unwrap())?;
        config.file.sync_all()?;
    } else {
        unreachable!()
    }

    // turn off snapshot mutability
    let snapshot = snapshot;

    if snapshot.tip_lid.is_some() {
        assert!(segment_progress >= SEG_HEADER_LEN as Lsn);
    }

    trace!("generated snapshot: {:?}", snapshot);

    assert!(snapshot.stable_lsn >= old_stable_lsn);

    if snapshot.stable_lsn > old_stable_lsn {
        write_snapshot(config, &snapshot)?;
    }

    for to_zero in iter.segments.values().copied() {
        debug!("zeroing torn segment at lid {}", to_zero);

        // NB we intentionally corrupt this header to prevent any segment
        // from being allocated which would duplicate its LSN, messing
        // up recovery in the future.
        io_fail!(config, "segment initial free zero");
        pwrite_all(
            &config.file,
            &*vec![MessageKind::Corrupted.into(); config.segment_size],
            to_zero,
        )?;
        if !config.temporary {
            config.file.sync_all()?;
        }
    }

    // remove all blob files larger than our stable offset
    gc_blobs(config, snapshot.stable_lsn)?;

    #[cfg(feature = "event_log")]
    config.event_log.recovered_lsn(snapshot.stable_lsn);

    Ok(snapshot)
}

/// Read a `Snapshot` or generate a default, then advance it to
/// the tip of the data file, if present.
pub fn read_snapshot_or_default(config: &RunningConfig) -> Result<Snapshot> {
    let last_snap = read_snapshot(config)?.unwrap_or_else(Snapshot::default);

    let log_iter = raw_segment_iter_from(last_snap.stable_lsn, config)?;

    let res = advance_snapshot(log_iter, last_snap, config)?;

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

    let mut buf = vec![];
    let _read = f.read_to_end(&mut buf)?;
    let len = buf.len();
    if len <= 12 {
        warn!("empty/corrupt snapshot file found");
        return Ok(None);
    }

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
    trace!("writing snapshot {:?}", snapshot);

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

    let path_1_suffix = format!("snap.{:016X}.generating", snapshot.stable_lsn);

    let mut path_1 = config.get_path();
    path_1.push(path_1_suffix);

    let path_2_suffix = format!("snap.{:016X}", snapshot.stable_lsn);

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
    f.sync_all()?;

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
