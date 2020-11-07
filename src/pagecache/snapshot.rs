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
#[derive(PartialEq, Debug, Default)]
#[cfg_attr(test, derive(Clone))]
pub struct Snapshot {
    /// The last read message lsn
    pub stable_lsn: Option<Lsn>,
    /// The last read message lid
    pub active_segment: Option<LogOffset>,
    /// the mapping from pages to (lsn, lid)
    pub pt: Vec<PageState>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PageState {
    /// Present signifies a page that has some data.
    ///
    /// It has two parts. The base and the fragments.
    /// `base` is separated to guarantee that it will
    /// always have at least one because it is
    /// correct by construction.
    /// The third element in each tuple is the on-log
    /// size for the corresponding write. If things
    /// are pretty large, they spill into the blobs
    /// directory, but still get a small pointer that
    /// gets written into the log. The sizes are used
    /// for the garbage collection statistics on
    /// segments. The lsn and the DiskPtr can be used
    /// for actually reading the item off the disk,
    /// and the size tells us how much storage it uses
    /// on the disk.
    Present {
        base: (Lsn, DiskPtr, u64),
        frags: Vec<(Lsn, DiskPtr, u64)>,
    },

    /// This is a free page.
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

    #[cfg(feature = "testing")]
    fn offsets(&self) -> Vec<LogOffset> {
        match *self {
            PageState::Present { base, ref frags } => {
                let mut offsets = vec![base.1.lid()];
                for (_, ptr, _) in frags {
                    offsets.push(ptr.lid());
                }
                offsets
            }
            PageState::Free(_, ptr) => vec![ptr.lid()],
            PageState::Uninitialized => {
                panic!("called offsets on Uninitialized")
            }
        }
    }
}

impl Snapshot {
    pub fn recovered_coords(
        &self,
        segment_size: usize,
    ) -> (Option<LogOffset>, Option<Lsn>) {
        if self.stable_lsn.is_none() {
            return (None, None);
        }

        let stable_lsn = self.stable_lsn.unwrap();

        if let Some(base_offset) = self.active_segment {
            let progress = stable_lsn % segment_size as Lsn;
            let offset = base_offset + LogOffset::try_from(progress).unwrap();

            (Some(offset), Some(stable_lsn))
        } else {
            let lsn_idx = stable_lsn / segment_size as Lsn
                + if stable_lsn % segment_size as Lsn == 0 { 0 } else { 1 };
            let next_lsn = lsn_idx * segment_size as Lsn;
            (None, Some(next_lsn))
        }
    }

    fn apply(
        &mut self,
        log_kind: LogKind,
        pid: PageId,
        lsn: Lsn,
        disk_ptr: DiskPtr,
        sz: u64,
    ) -> Result<()> {
        trace!(
            "trying to deserialize buf for pid {} ptr {} lsn {}",
            pid,
            disk_ptr,
            lsn
        );
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

                let pid_usize = usize::try_from(pid).unwrap();

                self.pt[pid_usize] = PageState::Present {
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
                        if old != PageState::Uninitialized {
                            error!("expected previous page state to be uninitialized");
                            return Err(Error::corruption(None));
                        }
                    }
                }
            }
            LogKind::Free => {
                trace!("free of pid {} at ptr {} lsn {}", pid, disk_ptr, lsn);
                self.pt[usize::try_from(pid).unwrap()] =
                    PageState::Free(lsn, disk_ptr);
            }
            LogKind::Corrupted | LogKind::Skip => {
                error!(
                    "unexpected messagekind in snapshot application for pid {}: {:?}",
                    pid, log_kind
                );
                return Err(Error::corruption(None));
            }
        }

        Ok(())
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

        if lsn < snapshot.stable_lsn.unwrap_or(-1) {
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

        if lsn >= iter.max_lsn.unwrap() {
            error!("lsn {} >= iter max_lsn {}", lsn, iter.max_lsn.unwrap());
            return Err(Error::corruption(None));
        }

        snapshot.apply(log_kind, pid, lsn, ptr, sz)?;
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

    let no_recovery_progress = iter.cur_lsn.is_none()
        || iter.cur_lsn.unwrap() <= snapshot.stable_lsn.unwrap_or(0);
    let db_is_empty = no_recovery_progress && snapshot.stable_lsn.is_none();

    #[cfg(feature = "testing")]
    let mut shred_point = None;

    let snapshot = if db_is_empty {
        trace!("db is empty, returning default snapshot");
        if snapshot != Snapshot::default() {
            error!("expected snapshot to be Snapshot::default");
            return Err(Error::corruption(None));
        }
        snapshot
    } else if iter.cur_lsn.is_none() {
        trace!(
            "no recovery progress happened since the last snapshot \
            was generated, returning the previous one"
        );
        snapshot
    } else {
        let iterated_lsn = iter.cur_lsn.unwrap();

        let segment_progress: Lsn = iterated_lsn % (config.segment_size as Lsn);

        // progress should never be below the SEG_HEADER_LEN if the segment_base
        // is set. progress can only be 0 if we've maxed out the
        // previous segment, unsetting the iterator segment_base in the
        // process.
        let monotonic = segment_progress >= SEG_HEADER_LEN as Lsn
            || (segment_progress == 0 && iter.segment_base.is_none());
        if !monotonic {
            error!("expected segment progress {} to be above SEG_HEADER_LEN or == 0, cur_lsn: {}",
                segment_progress,
                iterated_lsn,
            );
            return Err(Error::corruption(None));
        }

        let (stable_lsn, active_segment) = if segment_progress
            + MAX_MSG_HEADER_LEN as Lsn
            >= config.segment_size as Lsn
        {
            let bumped =
                config.normalize(iterated_lsn) + config.segment_size as Lsn;
            trace!("bumping snapshot.stable_lsn to {}", bumped);
            (bumped, None)
        } else {
            if let Some(BasedBuf { offset, .. }) = iter.segment_base {
                // either situation 3 or situation 4. we need to zero the
                // tail of the segment after the recovered tip
                let shred_len = config.segment_size
                    - usize::try_from(segment_progress).unwrap()
                    - 1;
                let shred_zone = vec![MessageKind::Corrupted.into(); shred_len];
                let shred_base =
                    offset + LogOffset::try_from(segment_progress).unwrap();

                #[cfg(feature = "testing")]
                {
                    shred_point = Some(shred_base);
                }

                debug!(
                    "zeroing the end of the recovered segment at lsn {} between lids {} and {}",
                    config.normalize(iterated_lsn),
                    shred_base,
                    shred_base + shred_len as LogOffset
                );
                pwrite_all(&config.file, &shred_zone, shred_base)?;
                config.file.sync_all()?;
            }
            (iterated_lsn, iter.segment_base.map(|bb| bb.offset))
        };

        if stable_lsn < snapshot.stable_lsn.unwrap_or(0) {
            error!(
                "unexpected corruption encountered in storage snapshot file. \
                stable lsn {} should be >= snapshot.stable_lsn {}",
                stable_lsn,
                snapshot.stable_lsn.unwrap_or(0),
            );
            return Err(Error::corruption(None));
        }

        snapshot.stable_lsn = Some(stable_lsn);
        snapshot.active_segment = active_segment;

        snapshot
    };

    trace!("generated snapshot: {:?}", snapshot);

    if snapshot.stable_lsn < old_stable_lsn {
        error!("unexpected corruption encountered in storage snapshot file");
        return Err(Error::corruption(None));
    }

    if snapshot.stable_lsn > old_stable_lsn {
        write_snapshot(config, &snapshot)?;
    }

    #[cfg(feature = "testing")]
    let reverse_segments = {
        use std::collections::{HashMap, HashSet};
        let shred_base = shred_point.unwrap_or(LogOffset::max_value());
        let mut reverse_segments = HashMap::new();
        for (pid, page) in snapshot.pt.iter().enumerate() {
            let offsets = page.offsets();
            for offset in offsets {
                let segment = config.normalize(offset);
                if segment == config.normalize(shred_base) {
                    assert!(
                        offset < shred_base,
                        "we shredded the location for pid {}
                        with locations {:?}
                        by zeroing the file tip after lid {}",
                        pid,
                        page,
                        shred_base
                    );
                }
                let entry = reverse_segments
                    .entry(segment)
                    .or_insert_with(HashSet::new);
                entry.insert((pid, offset));
            }
        }
        reverse_segments
    };

    for (lsn, to_zero) in &iter.segments {
        debug!("zeroing torn segment at lsn {} lid {}", lsn, to_zero);

        #[cfg(feature = "testing")]
        {
            if let Some(pids) = reverse_segments.get(to_zero) {
                assert!(
                    pids.is_empty(),
                    "expected segment that we're zeroing at lid {} \
                    lsn {} \
                    to contain no pages, but it contained pids {:?}",
                    to_zero,
                    lsn,
                    pids
                );
            }
        }

        // NB we intentionally corrupt this header to prevent any segment
        // from being allocated which would duplicate its LSN, messing
        // up recovery in the future.
        io_fail!(config, "segment initial free zero");
        pwrite_all(
            &config.file,
            &*vec![MessageKind::Corrupted.into(); config.segment_size],
            *to_zero,
        )?;
        if !config.temporary {
            config.file.sync_all()?;
        }
    }

    // remove all blob files larger than our stable offset
    if let Some(stable_lsn) = snapshot.stable_lsn {
        gc_blobs(config, stable_lsn)?;
    }

    #[cfg(feature = "event_log")]
    config.event_log.recovered_lsn(snapshot.stable_lsn.unwrap_or(0));

    Ok(snapshot)
}

/// Read a `Snapshot` or generate a default, then advance it to
/// the tip of the data file, if present.
pub fn read_snapshot_or_default(config: &RunningConfig) -> Result<Snapshot> {
    // NB we want to error out if the read snapshot was corrupted.
    // We only use a default Snapshot when there is no snapshot found.
    let last_snap = read_snapshot(config)?.unwrap_or_else(Snapshot::default);

    let log_iter =
        raw_segment_iter_from(last_snap.stable_lsn.unwrap_or(0), config)?;

    let res = advance_snapshot(log_iter, last_snap, config)?;

    Ok(res)
}

/// Read a `Snapshot` from disk.
/// Returns an error if the read snapshot was corrupted.
/// Returns `Ok(Some(snapshot))` if there was nothing written.
fn read_snapshot(config: &RunningConfig) -> Result<Option<Snapshot>> {
    let mut candidates = config.get_snapshot_files()?;
    if candidates.is_empty() {
        debug!("no previous snapshot found");
        return Ok(None);
    }

    candidates.sort();
    let path = candidates.pop().unwrap();

    let mut f = std::fs::OpenOptions::new().read(true).open(&path)?;

    let mut buf = vec![];
    let _read = f.read_to_end(&mut buf)?;
    let len = buf.len();
    if len <= 12 {
        warn!("empty/corrupt snapshot file found");
        return Err(Error::corruption(None));
    }

    let mut len_expected_bytes = [0; 8];
    len_expected_bytes.copy_from_slice(&buf[len - 12..len - 4]);

    let mut crc_expected_bytes = [0; 4];
    crc_expected_bytes.copy_from_slice(&buf[len - 4..]);

    let _ = buf.split_off(len - 12);
    let crc_expected: u32 = arr_to_u32(&crc_expected_bytes);

    let crc_actual = crc32(&buf);

    if crc_expected != crc_actual {
        warn!("corrupt snapshot file found, crc does not match expected");
        return Err(Error::corruption(None));
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

    Snapshot::deserialize(&mut bytes.as_slice()).map(Some)
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

    let path_1_suffix =
        format!("snap.{:016X}.generating", snapshot.stable_lsn.unwrap_or(0));

    let mut path_1 = config.get_path();
    path_1.push(path_1_suffix);

    let path_2_suffix =
        format!("snap.{:016X}", snapshot.stable_lsn.unwrap_or(0));

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
