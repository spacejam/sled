// log page storage
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::BTreeMap;
use std::fs;
use std::thread;
use std::io::{self, Read, Write, Seek, SeekFrom, Error, ErrorKind};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::cell::RefCell;
use std::os::unix::fs::OpenOptionsExt;

use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode, DecodingResult};
use rustc_serialize::{Encodable, Decodable};

use super::*;

thread_local! {
    static LOCAL_READER: RefCell<fs::File> = RefCell::new(open_log_for_reading());
}

fn open_log_for_reading() -> fs::File {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    if cfg!(unix) {
        // TODO get O_DIRECT working
        // options.custom_flags(libc::O_DIRECT);
    }
    options.open("rsdb.log").unwrap()
}

fn read(id: LogID) -> io::Result<LogData> {
    LOCAL_READER.with(|f| {
        let mut f = f.borrow_mut();
        f.seek(SeekFrom::Start(id))?;
        let mut len_buf = [0u8; 4];
        f.read_exact(&mut len_buf)?;
        let len = array_to_usize(len_buf);
        let mut buf = Vec::with_capacity(len);
        unsafe {
            buf.set_len(len);
        }
        f.read_exact(&mut buf)?;
        from_binary::<LogData>(buf)
            .map_err(|_| Error::new(ErrorKind::Other, "failed to deserialize LogData"))
    })
}

pub struct Log {
    file: fs::File,
    cur_id: LogID,
    stable: LogID,
}

impl Log {
    pub fn open() -> Log {
        let cur_id = fs::metadata("rsdb.log").map(|m| m.len()).unwrap_or(0);

        let mut options = fs::OpenOptions::new();
        options.write(true).create(true);

        if cfg!(unix) {
            // TODO get O_DIRECT working
            // options.custom_flags(// libc::O_DIRECT |
            //                     libc::O_DSYNC);
        }

        let file = options.open("rsdb.log").unwrap();

        Log {
            file: file,
            cur_id: cur_id,
            stable: cur_id,
        }
    }

    pub fn append(&mut self, data_bytes: &[u8]) -> io::Result<LogID> {
        let id = self.cur_id;
        self.cur_id += data_bytes.len() as u64;
        let len_bytes = usize_to_array(data_bytes.len());
        self.file.seek(SeekFrom::Start(id));
        self.file.write_all(&len_bytes)?;
        self.file.write_all(&*data_bytes)?;
        self.stable = id;
        Ok(id)
    }
}


fn to_binary<T: Encodable>(s: &T) -> Vec<u8> {
    encode(s, SizeLimit::Infinite).unwrap()
}

fn from_binary<T: Decodable>(encoded: Vec<u8>) -> DecodingResult<T> {
    decode(&encoded[..])
}

pub fn usize_to_array(u: usize) -> [u8; 4] {
    [(u >> 24) as u8, (u >> 16) as u8, (u >> 8) as u8, u as u8]
}

pub fn array_to_usize(ip: [u8; 4]) -> usize {
    ((ip[0] as usize) << 24) as usize + ((ip[1] as usize) << 16) as usize +
    ((ip[2] as usize) << 8) as usize + (ip[3] as usize)
}

#[test]
fn rtt_log() {
    let mut writer = Log::open();
    let deltablock = LogData::Deltas(vec![]);
    let data_bytes = to_binary(&deltablock);
    let lid = writer.append(&*data_bytes).unwrap();

    let rt = read(lid).unwrap();
    let t1 = thread::spawn(move || {
        assert_eq!(rt, deltablock);
    });
    t1.join();
}
