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
        let buf = if config.use_compression { decompress(buf) } else { buf };
        Ok((MessageKind::from(kind_byte[0]), buf))
    } else {
        warn!("blob {} failed crc check!", blob_ptr);

        Err(Error::corruption(Some(DiskPtr::Blob(None, blob_ptr))))
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

pub(crate) fn remove_blob(id: Lsn, config: &Config) -> Result<()> {
    let path = config.blob_path(id);
    let res = std::fs::remove_file(&path);
    if let Err(e) = res {
        debug!("removing blob at {:?} failed: {}", path, e);
        return Err(e.into());
    } else {
        trace!("successfully removed blob at {:?}", path);
    }

    Ok(())
}

pub(crate) fn gc_unknown_blobs(
    config: &Config,
    snapshot: &Snapshot,
) -> Result<()> {
    let mut blob_lsns = FastSet8::default();

    for page_state in &snapshot.pt {
        for blob_lsn in page_state.blob_lsns() {
            blob_lsns.insert(blob_lsn);
        }
    }

    for entry in std::fs::read_dir(config.get_path().join("blobs"))? {
        let item = entry?;
        if let Some(Ok(lsn)) =
            item.path().file_name().unwrap().to_str().map(str::parse)
        {
            if !blob_lsns.contains(&lsn) {
                log::trace!(
                    "removing file that is not known by \
                    the current pagetable: {:?}",
                    item.path()
                );
                std::fs::remove_file(item.path())?;
            }
        } else {
            println!("src/pagecache/mod.rs:2280: {:?}", item.path());
        }
    }

    Ok(())
}
