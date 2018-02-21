use std::fs::File;

#[cfg(feature = "zstd")]
use zstd::block::decompress;

use super::Pio;

use super::*;

pub(crate) trait LogReader {
    fn read_segment_header(&self, id: LogID) -> CacheResult<SegmentHeader, ()>;

    fn read_segment_trailer(
        &self,
        id: LogID,
    ) -> CacheResult<SegmentTrailer, ()>;

    fn read_message_header(&self, id: LogID) -> CacheResult<MessageHeader, ()>;

    fn read_message(
        &self,
        id: LogID,
        segment_len: usize,
        use_compression: bool,
    ) -> CacheResult<LogRead, ()>;
}

#[cfg(unix)]
impl LogReader for File {
    fn read_segment_header(
        &self,
        lid: LogID,
    ) -> CacheResult<SegmentHeader, ()> {
        trace!("reading segment header at {}", lid);

        let mut seg_header_buf = [0u8; SEG_HEADER_LEN];
        self.pread_exact(&mut seg_header_buf, lid)?;

        Ok(seg_header_buf.into())
    }

    fn read_segment_trailer(
        &self,
        lid: LogID,
    ) -> CacheResult<SegmentTrailer, ()> {
        trace!("reading segment trailer at {}", lid);

        let mut seg_trailer_buf = [0u8; SEG_TRAILER_LEN];
        self.pread_exact(&mut seg_trailer_buf, lid)?;

        Ok(seg_trailer_buf.into())
    }

    fn read_message_header(
        &self,
        lid: LogID,
    ) -> CacheResult<MessageHeader, ()> {
        let mut msg_header_buf = [0u8; MSG_HEADER_LEN];
        self.pread_exact(&mut msg_header_buf, lid)?;
        if msg_header_buf[0] == EVIL_BYTE {
            return Err(Error::Corruption {
                at: lid,
            });
        }

        Ok(msg_header_buf.into())
    }

    /// read a buffer from the disk
    fn read_message(
        &self,
        lid: LogID,
        segment_len: usize,
        _use_compression: bool,
    ) -> CacheResult<LogRead, ()> {
        let _measure = Measure::new(&M.read);
        let seg_start = lid / segment_len as LogID * segment_len as LogID;
        trace!("reading message from segment: {} at lid: {}", seg_start, lid);
        assert!(seg_start + SEG_HEADER_LEN as LogID <= lid);

        let ceiling = seg_start + segment_len as LogID -
            SEG_TRAILER_LEN as LogID;

        assert!(lid + MSG_HEADER_LEN as LogID <= ceiling);

        let header = self.read_message_header(lid)?;

        let max_possible_len = (ceiling - lid - MSG_HEADER_LEN as LogID) as
            usize;
        if header.len > max_possible_len {
            trace!("read a corrupted message of len {}", header.len);
            return Ok(LogRead::Corrupted(header.len));
        }

        let mut buf = Vec::with_capacity(header.len);
        unsafe {
            buf.set_len(header.len);
        }
        self.pread_exact(&mut buf, lid + MSG_HEADER_LEN as LogID)?;

        let checksum = crc16_arr(&buf);
        if checksum != header.crc16 {
            trace!("read a message with a bad checksum of len {}", header.len);
            return Ok(LogRead::Corrupted(header.len));
        }

        if !header.successful_flush {
            trace!("read zeroes of len {}", header.len);
            return Ok(LogRead::Failed(header.len));
        }

        #[cfg(feature = "zstd")]
        let res = {
            if _use_compression {
                let _measure = Measure::new(&M.decompress);
                Ok(LogRead::Flush(
                    header.lsn,
                    decompress(&*buf, segment_len).unwrap(),
                    len,
                ))
            } else {
                Ok(LogRead::Flush(header.lsn, buf, len))
            }
        };

        #[cfg(not(feature = "zstd"))]
        let res = Ok(LogRead::Flush(header.lsn, buf, header.len));

        trace!("read a successful flushed message");
        res
    }
}
