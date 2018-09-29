use std::fs::File;

#[cfg(feature = "zstd")]
use zstd::block::decompress;

use super::Pio;

use super::*;

pub(crate) trait LogReader {
    fn read_segment_header(
        &self,
        id: LogID,
    ) -> CacheResult<SegmentHeader, ()>;

    fn read_segment_trailer(
        &self,
        id: LogID,
    ) -> CacheResult<SegmentTrailer, ()>;

    fn read_message_header(
        &self,
        id: LogID,
    ) -> CacheResult<MessageHeader, ()>;

    fn read_message(
        &self,
        lid: LogID,
        config: &Config,
    ) -> CacheResult<LogRead, ()>;
}

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

        Ok(msg_header_buf.into())
    }

    /// read a buffer from the disk
    fn read_message(
        &self,
        lid: LogID,
        config: &Config,
    ) -> CacheResult<LogRead, ()> {
        let _measure = Measure::new(&M.read);
        let segment_len = config.io_buf_size;
        let seg_start =
            lid / segment_len as LogID * segment_len as LogID;
        trace!(
            "reading message from segment: {} at lid: {}",
            seg_start,
            lid
        );
        assert!(seg_start + SEG_HEADER_LEN as LogID <= lid);

        let ceiling = seg_start + segment_len as LogID
            - SEG_TRAILER_LEN as LogID;

        assert!(lid + MSG_HEADER_LEN as LogID <= ceiling);

        let header = self.read_message_header(lid)?;

        if header.lsn as usize % segment_len
            != lid as usize % segment_len
        {
            // our message lsn was not aligned to our segment offset
            return Ok(LogRead::Corrupted(header.len));
        }

        let max_possible_len =
            (ceiling - lid - MSG_HEADER_LEN as LogID) as usize;
        if header.len > max_possible_len {
            trace!("read a corrupted message of len {}", header.len);
            return Ok(LogRead::Corrupted(header.len));
        }

        if header.kind == MessageKind::Corrupted {
            trace!("read a corrupted message of len {}", header.len);
            return Ok(LogRead::Corrupted(header.len));
        }

        // perform crc check on everything that isn't Corrupted

        let mut buf = Vec::with_capacity(header.len);
        unsafe {
            buf.set_len(header.len);
        }
        self.pread_exact(&mut buf, lid + MSG_HEADER_LEN as LogID)?;

        let checksum = crc16_arr(&buf);
        if checksum != header.crc16 {
            trace!(
                "read a message with a bad checksum with header {:?}",
                header
            );
            return Ok(LogRead::Corrupted(header.len));
        }

        match header.kind {
            MessageKind::Failed => {
                trace!("read failed of len {}", header.len);
                Ok(LogRead::Failed(header.lsn, header.len))
            }
            MessageKind::Pad => {
                trace!("read pad at lsn {}", header.lsn);
                Ok(LogRead::Pad(header.lsn))
            }
            MessageKind::SuccessBlob => {
                let mut id_bytes = [0u8; 8];
                id_bytes.copy_from_slice(&*buf);
                let id: Lsn = arr_to_u64(id_bytes) as Lsn;

                match read_blob(id, config) {
                    Ok(buf) => {
                        trace!("read a successful external message");

                        Ok(LogRead::External(header.lsn, buf, id))
                    }
                    Err(Error::Io(ref e))
                        if e.kind()
                            == std::io::ErrorKind::NotFound =>
                    {
                        Ok(LogRead::DanglingExternal(header.lsn, id))
                    }
                    Err(other_e) => Err(other_e),
                }
            }
            MessageKind::Success => {
                trace!("read a successful inline message");
                let buf = {
                    #[cfg(feature = "zstd")]
                    {
                        if config.use_compression {
                            let _measure =
                                Measure::new(&M.decompress);
                            decompress(&*buf, segment_len).unwrap()
                        } else {
                            buf
                        }
                    }

                    #[cfg(not(feature = "zstd"))]
                    buf
                };

                Ok(LogRead::Inline(header.lsn, buf, header.len))
            }
            MessageKind::Corrupted => panic!(
                "corrupted should have been handled \
                 before reading message length above"
            ),
        }
    }
}
