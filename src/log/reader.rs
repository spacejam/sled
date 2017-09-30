use std::io::{Read, Seek, SeekFrom};
use std::fs::File;
use std::io::ErrorKind::UnexpectedEof;

use super::*;

pub trait LogReader {
    fn read_segment_header(&mut self, id: LogID) -> std::io::Result<SegmentHeader>;
    fn read_segment_trailer(&mut self, id: LogID) -> std::io::Result<SegmentTrailer>;
    fn read_message_header(&mut self, id: LogID) -> std::io::Result<MessageHeader>;
    fn read_entry(&mut self, id: LogID) -> std::io::Result<LogRead>;
}

impl LogReader for File {
    fn read_segment_header(&mut self, id: LogID) -> std::io::Result<SegmentHeader> {
        // println!("seeking to {}", id);
        self.seek(SeekFrom::Start(id))?;

        // println!("trying to read {} bytes", SEG_HEADER_LEN);

        let mut seg_header_buf = [0u8; SEG_HEADER_LEN];
        self.read_exact(&mut seg_header_buf)?;

        // println!("read the header!!!");

        Ok(seg_header_buf.into())
    }

    fn read_segment_trailer(&mut self, id: LogID) -> std::io::Result<SegmentTrailer> {
        self.seek(SeekFrom::Start(id))?;

        let mut seg_trailer_buf = [0u8; SEG_TRAILER_LEN];
        self.read_exact(&mut seg_trailer_buf)?;

        Ok(seg_trailer_buf.into())
    }

    fn read_message_header(&mut self, id: LogID) -> std::io::Result<MessageHeader> {
        self.seek(SeekFrom::Start(id))?;

        let mut msg_header_buf = [0u8; MSG_HEADER_LEN];
        self.read_exact(&mut msg_header_buf)?;

        Ok(msg_header_buf.into())
    }

    /// read a buffer from the disk
    fn read_entry(&mut self, id: LogID) -> std::io::Result<LogRead> {
        // println!("read_entry({})", id);
        let start = clock();
        let header = self.read_message_header(id)?;

        self.seek(SeekFrom::Start(id + MSG_HEADER_LEN as LogID))?;

        let max = 2 << 22; // FIXME self.get_io_buf_size() - MSG_HEADER_LEN;
        let mut len = header.len;
        if len > max {
            error!("log read invalid message length, {} should be <= {}", len, max);
            M.read.measure(clock() - start);
            return Ok(LogRead::Corrupted(len));
        } else if len == 0 && !header.valid {
            // skip to next record, which starts with 1
            loop {
                let mut byte = [0u8; 1];
                if let Err(e) = self.read_exact(&mut byte) {
                    if e.kind() == UnexpectedEof {
                        // we've hit the end of the file
                        break;
                    }
                    panic!("{:?}", e);
                }
                if byte[0] != 1 {
                    debug_assert_eq!(byte[0], 0);
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
            if self.get_use_compression() {
                let start = clock();
                let res = Ok(LogRead::Flush(header.lsn, decompress(&*buf, max).unwrap(), len));
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
