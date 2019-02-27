use std::fs::File;

#[cfg(feature = "compression")]
use zstd::block::decompress;

use super::Pio;

use super::*;

pub(crate) trait LogReader {
    fn read_segment_header(
        &self,
        id: LogId,
    ) -> Result<SegmentHeader, ()>;

    fn read_segment_trailer(
        &self,
        id: LogId,
    ) -> Result<SegmentTrailer, ()>;

    fn read_message_header(
        &self,
        id: LogId,
    ) -> Result<MessageHeader, ()>;

    fn read_message(
        &self,
        lid: LogId,
        config: &Config,
    ) -> Result<LogRead, ()>;
}

impl LogReader for File {
    fn read_segment_header(
        &self,
        lid: LogId,
    ) -> Result<SegmentHeader, ()> {
        trace!("reading segment header at {}", lid);

        let mut seg_header_buf = [0u8; SEG_HEADER_LEN];
        self.pread_exact(&mut seg_header_buf, lid)?;

        Ok(seg_header_buf.into())
    }

    fn read_segment_trailer(
        &self,
        lid: LogId,
    ) -> Result<SegmentTrailer, ()> {
        trace!("reading segment trailer at {}", lid);

        let mut seg_trailer_buf = [0u8; SEG_TRAILER_LEN];
        self.pread_exact(&mut seg_trailer_buf, lid)?;

        Ok(seg_trailer_buf.into())
    }

    fn read_message_header(
        &self,
        lid: LogId,
    ) -> Result<MessageHeader, ()> {
        let mut msg_header_buf = [0u8; MSG_HEADER_LEN];
        self.pread_exact(&mut msg_header_buf, lid)?;

        Ok(msg_header_buf.into())
    }

    /// read a buffer from the disk
    fn read_message(
        &self,
        lid: LogId,
        config: &Config,
    ) -> Result<LogRead, ()> {
        let _measure = Measure::new(&M.read);
        let segment_len = config.io_buf_size;
        let seg_start =
            lid / segment_len as LogId * segment_len as LogId;
        trace!(
            "reading message from segment: {} at lid: {}",
            seg_start,
            lid
        );
        assert!(seg_start + SEG_HEADER_LEN as LogId <= lid);

        let ceiling = seg_start + segment_len as LogId
            - SEG_TRAILER_LEN as LogId;

        assert!(lid + MSG_HEADER_LEN as LogId <= ceiling);

        let header = self.read_message_header(lid)?;

        if header.lsn as usize % segment_len
            != lid as usize % segment_len
        {
            let _hb: [u8; MSG_HEADER_LEN] = header.into();
            // our message lsn was not aligned to our segment offset
            trace!(
                "read a message whose header lsn \
                 is not aligned to its position \
                 within its segment. header: {:?} \
                 expected: relative offset {} bytes: {:?}",
                header,
                lid as usize % segment_len,
                _hb
            );
            return Ok(LogRead::Corrupted(header.len));
        }

        let max_possible_len =
            (ceiling - lid - MSG_HEADER_LEN as LogId) as usize;
        if header.len > max_possible_len {
            trace!("read a corrupted message with impossibly long length of {}", header.len);
            return Ok(LogRead::Corrupted(header.len));
        }

        if header.kind == MessageKind::Corrupted {
            trace!("read a corrupted message with Corrupted MessageKind with len {}", header.len);
            return Ok(LogRead::Corrupted(header.len));
        }

        // perform crc check on everything that isn't Corrupted

        let mut buf = Vec::with_capacity(header.len);
        unsafe {
            buf.set_len(header.len);
        }
        self.pread_exact(&mut buf, lid + MSG_HEADER_LEN as LogId)?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc32 = hasher.finalize();

        if crc32 != header.crc32 {
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
            MessageKind::Blob => {
                let id = arr_to_u64(&buf) as Lsn;

                match read_blob(id, config) {
                    Ok(buf) => {
                        trace!(
                            "read a successful blob message for Blob({}, {})",
                            header.lsn,
                            id,
                        );

                        Ok(LogRead::Blob(header.lsn, buf, id))
                    }
                    Err(Error::Io(ref e))
                        if e.kind()
                            == std::io::ErrorKind::NotFound =>
                    {
                        debug!(
                            "underlying blob file not found for Blob({}, {})",
                            header.lsn,
                            id,
                        );
                        Ok(LogRead::DanglingBlob(header.lsn, id))
                    }
                    Err(other_e) => {
                        debug!("failed to read blob: {:?}", other_e);
                        Err(other_e)
                    }
                }
            }
            MessageKind::Inline => {
                trace!("read a successful inline message");
                let buf = {
                    #[cfg(feature = "compression")]
                    {
                        if config.use_compression {
                            let _measure =
                                Measure::new(&M.decompress);
                            decompress(&*buf, segment_len).unwrap()
                        } else {
                            buf
                        }
                    }

                    #[cfg(not(feature = "compression"))]
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
