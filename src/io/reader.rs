use std::io::{Read, Seek, SeekFrom};
use std::fs::File;
use std::io::ErrorKind::UnexpectedEof;

#[cfg(feature = "zstd")]
use zstd::block::decompress;

use super::*;

pub(super) trait LogReader {
    fn read_segment_header(
        &mut self,
        id: LogID,
    ) -> std::io::Result<SegmentHeader>;

    fn read_segment_trailer(
        &mut self,
        id: LogID,
    ) -> std::io::Result<SegmentTrailer>;

    fn read_message_header(
        &mut self,
        id: LogID,
    ) -> std::io::Result<MessageHeader>;

    fn read_message(
        &mut self,
        id: LogID,
        segment_len: usize,
        use_compression: bool,
    ) -> std::io::Result<LogRead>;
}

impl LogReader for File {
    fn read_segment_header(
        &mut self,
        id: LogID,
    ) -> std::io::Result<SegmentHeader> {
        trace!("reading segment header at {}", id);
        self.seek(SeekFrom::Start(id))?;

        let mut seg_header_buf = [0u8; SEG_HEADER_LEN];
        self.read_exact(&mut seg_header_buf)?;

        Ok(seg_header_buf.into())
    }

    fn read_segment_trailer(
        &mut self,
        id: LogID,
    ) -> std::io::Result<SegmentTrailer> {
        trace!("reading segment trailer at {}", id);
        self.seek(SeekFrom::Start(id))?;

        let mut seg_trailer_buf = [0u8; SEG_TRAILER_LEN];
        self.read_exact(&mut seg_trailer_buf)?;

        Ok(seg_trailer_buf.into())
    }

    fn read_message_header(
        &mut self,
        id: LogID,
    ) -> std::io::Result<MessageHeader> {
        self.seek(SeekFrom::Start(id))?;

        let mut msg_header_buf = [0u8; MSG_HEADER_LEN];
        self.read_exact(&mut msg_header_buf)?;

        Ok(msg_header_buf.into())
    }

    /// read a buffer from the disk
    fn read_message(
        &mut self,
        id: LogID,
        segment_len: usize,
        _use_compression: bool,
    ) -> std::io::Result<LogRead> {
        trace!("reading message at lid {}", id);
        let start = clock();
        let seg_start = id / segment_len as LogID * segment_len as LogID;
        println!("seg_start: {} id: {}", seg_start, id);
        assert!(seg_start + MSG_HEADER_LEN as LogID <= id);

        let ceiling = seg_start + segment_len as LogID -
            SEG_TRAILER_LEN as LogID;

        assert!(id + MSG_HEADER_LEN as LogID <= ceiling);

        let header = self.read_message_header(id)?;
        assert!(id + MSG_HEADER_LEN as LogID + header.len as LogID <= ceiling);

        self.seek(SeekFrom::Start(id + MSG_HEADER_LEN as LogID))?;

        let max = (ceiling - id - MSG_HEADER_LEN as LogID) as usize;
        let mut len = header.len;
        if len > max {
            error!(
                "log read invalid message length, {} should be <= {}",
                len,
                max
            );
            M.read.measure(clock() - start);
            return Ok(LogRead::Corrupted(len));
        } else if len == 0 && !header.valid {
            // skip to next record, which starts with 1
            while len <= max {
                let mut byte = [0u8; 1];
                if let Err(e) = self.read_exact(&mut byte) {
                    if e.kind() == UnexpectedEof {
                        // we've hit the end of the file
                        break;
                    }
                    panic!("{:?}", e);
                }
                if byte[0] != 1 {
                    len += 1;
                } else {
                    break;
                }
            }
        }

        if !header.valid {
            M.read.measure(clock() - start);
            return Ok(LogRead::Zeroed(len + MSG_HEADER_LEN));
        }

        let mut buf = Vec::with_capacity(len);
        unsafe {
            buf.set_len(len);
        }
        self.read_exact(&mut buf)?;

        let checksum = crc16_arr(&buf);
        if checksum != header.crc16 {
            M.read.measure(clock() - start);
            return Ok(LogRead::Corrupted(len));
        }

        #[cfg(feature = "zstd")]
        let res = {
            if _use_compression {
                let start = clock();
                let res = Ok(LogRead::Flush(
                    header.lsn,
                    decompress(&*buf, segment_len).unwrap(),
                    len,
                ));
                M.decompress.measure(clock() - start);
                res
            } else {
                Ok(LogRead::Flush(header.lsn, buf, len))
            }
        };

        #[cfg(not(feature = "zstd"))]
        let res = Ok(LogRead::Flush(header.lsn, buf, len));

        M.read.measure(clock() - start);
        res
    }
}
