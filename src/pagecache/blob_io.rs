use crate::pagecache::*;
use crate::*;

pub(crate) fn read_blob(
    blob_ptr: Lsn,
    config: &Config,
) -> Result<(MessageKind, Vec<u8>)> {
    let path = config.blob_path(blob_ptr);
    let f_res = std::fs::OpenOptions::new().read(true).open(&path);

    if let Err(e) = &f_res {
        debug!("failed to open file for blob read at {}: {:?}", blob_ptr, e);
    }

    let mut f = f_res?;

    let mut crc_expected_bytes = [0_u8; std::mem::size_of::<u32>()];

    if let Err(e) = f.read_exact(&mut crc_expected_bytes) {
        debug!(
            "failed to read the initial CRC bytes in the blob at {}: {:?}",
            blob_ptr, e,
        );
        return Err(e.into());
    }

    let crc_expected = arr_to_u32(&crc_expected_bytes);

    let mut kind_byte = [0_u8];

    if let Err(e) = f.read_exact(&mut kind_byte) {
        debug!(
            "failed to read the initial CRC bytes in the blob at {}: {:?}",
            blob_ptr, e,
        );
        return Err(e.into());
    }

    let mut buf = vec![];

    if let Err(e) = f.read_to_end(&mut buf) {
        debug!(
            "failed to read data after the CRC bytes in blob at {}: {:?}",
            blob_ptr, e,
        );
        return Err(e.into());
    }

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&kind_byte);
    hasher.update(&buf);
    let crc_actual = hasher.finalize();

    if crc_expected == crc_actual {
        let buf =
            if config.use_compression { maybe_decompress(buf)? } else { buf };
        Ok((MessageKind::from(kind_byte[0]), buf))
    } else {
        warn!("blob {} failed crc check!", blob_ptr);

        Err(Error::corruption(Some(DiskPtr::Blob(0, blob_ptr))))
    }
}

pub(crate) fn write_blob<T: Serialize>(
    config: &Config,
    kind: MessageKind,
    id: Lsn,
    item: &T,
) -> Result<()> {
    let path = config.blob_path(id);
    let mut f =
        std::fs::OpenOptions::new().write(true).create_new(true).open(&path)?;

    let kind_buf = &[kind.into()];

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(kind_buf);

    let data = {
        let _ = Measure::new(&M.serialize);
        item.serialize()
    };

    hasher.update(&data);
    let crc = u32_to_arr(hasher.finalize());

    io_fail!(config, "write_blob write crc");
    f.write_all(&crc)?;
    io_fail!(config, "write_blob write kind_byte");
    f.write_all(kind_buf)?;
    io_fail!(config, "write_blob write buf");
    f.write_all(&data)
        .map(|r| {
            trace!("successfully wrote blob at {:?}", path);
            r
        })
        .map_err(|e| e.into())
}

pub(crate) fn gc_blobs(config: &Config, stable_lsn: Lsn) -> Result<()> {
    let mut base_dir = config.get_path();
    base_dir.push("blobs");
    let blob_dir = base_dir;
    let blobs = std::fs::read_dir(blob_dir)?;

    debug!("gc_blobs removing any blob with an lsn above {}", stable_lsn);

    let mut to_remove = vec![];
    for blob in blobs {
        let path = blob?.path();
        let lsn_str = path.file_name().unwrap().to_str().unwrap();
        let lsn_res: std::result::Result<Lsn, _> = lsn_str.parse();

        if let Err(e) = lsn_res {
            warn!(
                "blobs directory contains \
                 unparsable path ({:?}): {}",
                path, e
            );
            continue;
        }

        let lsn = lsn_res.unwrap();

        if lsn >= stable_lsn {
            to_remove.push(path);
        }
    }

    if !to_remove.is_empty() {
        warn!(
            "removing {} blobs that have \
             a higher lsn than our stable log: {:?}",
            to_remove.len(),
            stable_lsn
        );
    }

    for path in to_remove {
        std::fs::remove_file(&path)?;
    }

    Ok(())
}

pub(crate) fn remove_blob(id: Lsn, config: &Config) -> Result<()> {
    let path = config.blob_path(id);
    if let Err(e) = std::fs::remove_file(&path) {
        debug!("removing blob at {:?} failed: {}", path, e);
    } else {
        trace!("successfully removed blob at {:?}", path);
    }

    // TODO return a future
    Ok(())
}
