use super::*;

use zstd::block::{compress, decompress};

pub struct ZStdCompression {
    pub compression_factor: i32,
    pub segment_len: usize
}

impl InvertibleTransform for ZStdCompression {
    type Error = Error<std::io::Error>;

    fn forward(&self, buf: &[u8]) -> Result<Vec<u8>, Self::Error> {
        let _measure = Measure::new(&M.compress);
        let deflated = compress(&buf, self.compression_factor)?;
        Ok(deflated)
    }

    fn backward(&self, buf: &[u8]) -> Result<Vec<u8>, Self::Error> {
        let _measure = Measure::new(&M.decompress);
        let inflated = decompress(&buf, self.segment_len)?;
        Ok(inflated)
    }
}

