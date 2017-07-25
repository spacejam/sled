use super::*;

const MEMLOG_PATH: &'static str = "/dev/shm/__rsdb_memory.log";

pub struct MemLog(LockFreeLog);

impl MemLog {
    pub fn new() -> MemLog {
        let log = LockFreeLog::start_system(MEMLOG_PATH.to_owned());
        MemLog(log)
    }
}

impl Drop for MemLog {
    fn drop(&mut self) {
        fs::remove_file(MEMLOG_PATH).unwrap();
    }
}

impl Log for MemLog {
    fn reserve(&self, buf: Vec<u8>) -> Reservation {
        self.0.reserve(buf)
    }

    fn write(&self, buf: Vec<u8>) -> LogID {
        self.0.write(buf)
    }

    fn read(&self, id: LogID) -> io::Result<Option<Vec<u8>>> {
        self.0.read(id)
    }

    fn stable_offset(&self) -> LogID {
        self.0.stable_offset()
    }

    fn make_stable(&self, id: LogID) {
        self.0.make_stable(id);
    }

    fn punch_hole(&self, id: LogID) {
        self.0.punch_hole(id);
    }
}
