use std::convert::TryInto;

#[cfg(feature = "compression")]
use zstd::block::decompress;

#[inline]
pub(crate) fn u64_to_arr(number: u64) -> [u8; 8] {
    number.to_le_bytes()
}

#[inline]
pub(crate) fn arr_to_u64(arr: &[u8]) -> u64 {
    arr.try_into().map(u64::from_le_bytes).unwrap()
}

#[inline]
pub(crate) fn arr_to_u32(arr: &[u8]) -> u32 {
    arr.try_into().map(u32::from_le_bytes).unwrap()
}

#[inline]
pub(crate) fn u32_to_arr(number: u32) -> [u8; 4] {
    number.to_le_bytes()
}

pub(crate) fn maybe_decompress(buf: Vec<u8>) -> std::io::Result<Vec<u8>> {
    #[cfg(feature = "compression")]
    {
        use std::sync::atomic::AtomicUsize;

        use super::*;

        static MAX_COMPRESSION_RATIO: AtomicUsize = AtomicUsize::new(1);
        use std::sync::atomic::Ordering::{Acquire, Release};

        let _measure = Measure::new(&M.decompress);
        loop {
            let ratio = MAX_COMPRESSION_RATIO.load(Acquire);
            match decompress(&*buf, buf.len() * ratio) {
                Err(ref e) if e.kind() == io::ErrorKind::Other => {
                    debug!(
                        "bumping expected compression \
                         ratio up from {} to {}: {:?}",
                        ratio,
                        ratio + 1,
                        e
                    );
                    MAX_COMPRESSION_RATIO.compare_and_swap(
                        ratio,
                        ratio + 1,
                        Release,
                    );
                }
                other => return other,
            }
        }
    }

    #[cfg(not(feature = "compression"))]

    Ok(buf)
}
