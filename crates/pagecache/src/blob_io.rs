use std::io::{Read, Write};

use super::*;

pub(crate) fn read_blob(
    blob_ptr: Lsn,
    config: &Config,
) -> Result<Vec<u8>, ()> {
    let path = config.blob_path(blob_ptr);
    let mut f = std::fs::OpenOptions::new().read(true).open(&path)?;

    let mut crc_expected_bytes = [0u8; EXTERNAL_VALUE_LEN];
    f.read_exact(&mut crc_expected_bytes).unwrap();
    let crc_expected = arr_to_u64(crc_expected_bytes);

    let mut buf = vec![];
    f.read_to_end(&mut buf)?;

    let crc_actual = crc64(&*buf);

    if crc_expected != crc_actual {
        warn!("blob {} failed crc check!", blob_ptr);

        Err(Error::Corruption {
            // FIXME Corruption pointer below should not have 0 as its LogId
            at: DiskPtr::Blob(0, blob_ptr),
        })
    } else {
        Ok(buf)
    }
}

pub(crate) fn write_blob(
    config: &Config,
    id: Lsn,
    data: Vec<u8>,
) -> Result<(), ()> {
    let path = config.blob_path(id);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;

    let crc = u64_to_arr(crc64(&*data));

    f.write_all(&crc)
        .and_then(|_| f.write_all(&data))
        .map_err(|e| e.into())
}

pub(crate) fn gc_blobs(
    config: &Config,
    stable_lsn: Lsn,
) -> Result<(), ()> {
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

pub(crate) fn remove_blob(
    id: Lsn,
    config: &Config,
) -> Result<(), ()> {
    let path = config.blob_path(id);

    if let Err(e) = std::fs::remove_file(&path) {
        warn!("removing blob at {:?} failed: {}", path, e);
    } else {
        trace!("successfully removed blob at {:?}", path);
    }

    // TODO return a future
    Ok(())
}
