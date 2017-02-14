use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Read, Write};

use log::{usize_to_array, array_to_usize};

pub struct RSDB<'a> {
    dir_path: &'a str,
    log: fs::File,
    store: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl<'a> RSDB<'a> {
    pub fn new<'l>(dir_path: &'l str) -> io::Result<RSDB<'l>> {
        recover(dir_path).map(|(log, store)| {
            RSDB {
                dir_path: dir_path,
                log: log,
                store: store,
            }
        })
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<&Vec<u8>>> {
        Ok(self.store.get(key))
    }

    pub fn set(&mut self, key: &'a [u8], value: &'a [u8]) -> io::Result<()> {
        self.log(key.clone(), value.clone())?;
        self.store.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn log(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.log.write_all(&usize_to_array(key.len()))?;
        self.log.write_all(&usize_to_array(value.len()))?;
        self.log.write_all(key)?;
        self.log.write_all(value)?;
        Ok(())
    }
}

macro_rules! read_or_break {
    ($file:expr, $buf:expr, $count:expr) => (
        match $file.read(&mut $buf) {
            Ok(n) if n == $buf.len() => {
                $count += n;
            },
            Ok(_) => {
                // tear occurred here
                break;
            },
            Err(_) => {
                break
            }
        }
    )
}

fn recover<'a>(dir_path: &str) -> io::Result<(fs::File, BTreeMap<Vec<u8>, Vec<u8>>)> {
    let filename = format!("{}/rsdb.log", dir_path);
    fs::create_dir_all(dir_path)?;
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(filename)?;

    let mut store = BTreeMap::new();

    let mut read = 0;
    loop {

        // until we hit end/tear:
        //   sizeof(size) >= remaining ? read size : keep file ptr here and trim with warning
        //   size >= remaining ? read remaining : back up file ptr sizeof(size) and trim with warning
        //   add msg to map

        let (mut k_len_buf, mut v_len_buf) = ([0u8; 4], [0u8; 4]);
        read_or_break!(file, k_len_buf, read);
        read_or_break!(file, v_len_buf, read);
        let (klen, vlen) = (array_to_usize(k_len_buf), array_to_usize(v_len_buf));
        let (mut k_buf, mut v_buf) = (Vec::with_capacity(klen), Vec::with_capacity(vlen));
        read_or_break!(file, k_buf, read);
        read_or_break!(file, v_buf, read);
        store.insert(k_buf, v_buf);
    }

    // clear potential tears
    file.set_len(read as u64)?;

    Ok((file, store))
}
