use super::*;

const MEMLOG_PATH: &'static str = "__rsdb_memory.log";

pub struct MemLog(LockFreeLog);

impl MemLog {
    pub fn new() -> MemLog {
        let mut options = fs::OpenOptions::new();
        options.create(true);
        options.read(true);
        options.write(true);
        let file = options.open(MEMLOG_PATH).unwrap();

        // "poor man's shared memory"
        // We retain an open descriptor to the file,
        // but it is no longer attached to this path,
        // so it continues to exist as a set of
        // anonymously mapped pages in memory only.
        fs::remove_file(MEMLOG_PATH).unwrap();

        let iobufs = IOBufs::new(file, 0);

        MemLog(LockFreeLog { iobufs: iobufs })
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
