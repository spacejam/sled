use std::io::{Read, Write};

use super::*;

use epoch::pin;

pub(crate) fn read_blob(
    external_ptr: Lsn,
    config: &Config,
) -> CacheResult<Vec<u8>, ()> {
    let path = config.blob_path(external_ptr);
    let mut f = std::fs::OpenOptions::new().read(true).open(&path)?;

    let mut crc_expected_bytes = [0u8; EXTERNAL_VALUE_LEN];
    f.read_exact(&mut crc_expected_bytes).unwrap();
    let crc_expected: u64 =
        unsafe { std::mem::transmute(crc_expected_bytes) };

    let mut buf = vec![];
    f.read_to_end(&mut buf)?;

    let crc_actual = crc64(&*buf);

    if crc_expected != crc_actual {
        warn!("blob {} failed crc check!", external_ptr);

        Err(Error::Corruption {
            // FIXME Corruption pointer below should not have 0 as its LogID
            at: DiskPtr::External(0, external_ptr),
        })
    } else {
        Ok(buf)
    }
}

pub(crate) fn write_blob(
    config: &Config,
    id: Lsn,
    mut data: Vec<u8>,
) -> CacheResult<(), ()> {
    let path = config.blob_path(id);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;

    let mut crc: [u8; EXTERNAL_VALUE_LEN] =
        unsafe { std::mem::transmute(crc64(&*data)) };

    f.write_all(&mut crc)
        .and_then(|_| f.write_all(&mut data))
        .map_err(|e| e.into())
}

pub(crate) fn gc_blobs(
    config: &Config,
    stable_lsn: Lsn,
) -> CacheResult<(), ()> {
    let stable = config.blob_path(stable_lsn);
    let blob_dir = stable.parent().unwrap();
    let blobs = std::fs::read_dir(blob_dir)?;

    for blob in blobs {
        let path = blob?.path();
        let lsn_str = path.file_name().unwrap().to_str().unwrap();
        let lsn_res: Result<Lsn, _> = lsn_str.parse();

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
) -> CacheResult<(), ()> {
    let path = config.blob_path(id);

    let guard = pin();

    unsafe {
        guard.defer(move || {
            if let Err(e) = std::fs::remove_file(&path) {
                warn!("removing blob at {:?} failed: {}", path, e);
            }
        });
    }

    // TODO return a future
    Ok(())
}
