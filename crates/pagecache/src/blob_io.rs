use std::io::{Read, Write};

use super::*;

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
        let buf = if config.use_compression {
            maybe_decompress(buf)?
        } else {
            buf
        };
        Ok((MessageKind::from(kind_byte[0]), buf))
    } else {
        warn!("blob {} failed crc check!", blob_ptr);

        Err(Error::Corruption {
            at: DiskPtr::Blob(0, blob_ptr),
        })
    }
}

pub(crate) fn write_blob(
    config: &Config,
    kind: MessageKind,
    id: Lsn,
    data: &[u8],
) -> Result<()> {
    let path = config.blob_path(id);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;

    let kind_buf = &[kind.into()];

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(kind_buf);
    hasher.update(data);
    let crc = u32_to_arr(hasher.finalize());

    f.write_all(&crc)
        .and_then(|_| f.write_all(kind_buf))
        .and_then(|_| f.write_all(data))
        .map(|r| {
            trace!("successfully wrote blob at {:?}", path);
            r
        })
        .map_err(|e| e.into())
}

pub(crate) fn gc_blobs(config: &Config, stable_lsn: Lsn) -> Result<()> {
    let stable = config.blob_path(stable_lsn);
    let blob_dir = stable.parent().unwrap();
    let blobs = std::fs::read_dir(blob_dir)?;

    debug!(
        "gc_blobs removing any blob with an lsn above {}",
        stable_lsn
    );

    for blob in blobs {
        let path = blob?.path();
        let lsn_str = path.file_name().unwrap().to_str().unwrap();
        let lsn_res: std::result::Result<Lsn, _> = lsn_str.parse();

        if let Err(e) = lsn_res {
            return Err(Error::Unsupported(format!(
                "blobs directory contains \
                 unparsable path ({:?}): {}",
                path, e
            )));
        }

        let lsn = lsn_res.unwrap();

        if lsn > stable_lsn {
            warn!(
                "removing blob {:?} that has \
                 a higher lsn than our stable log: {:?}",
                path, stable
            );
            std::fs::remove_file(&path)?;
        }
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
