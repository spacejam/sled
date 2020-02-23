use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const SZ: usize = 128;

#[derive(Default, Debug)]
struct Log {
    segment_accountant: Arc<SegmentAccountant>,
    io_buf: Arc<IoBuf>,
}

impl Log {
    fn new() -> Log {
        let io_buf = Arc::new(IoBuf::default());
        let segment_accountant = io_buf.segment.segment_accountant.clone();
        Log { io_buf, segment_accountant }
    }

    fn reserve(&mut self, size: usize) -> Reservation {
        assert!(size <= SZ);
        if self.io_buf.buf.load(Ordering::SeqCst) + size > SZ {
            let segment = self.segment_accountant.clone().next_segment();
            let buf = AtomicUsize::new(0);
            self.io_buf = Arc::new(IoBuf { segment, buf });
        }
        let io_buf = self.io_buf.clone();
        io_buf.buf.fetch_add(size, Ordering::SeqCst);
        Reservation { io_buf }
    }
}

#[derive(Default, Debug)]
struct Reservation {
    io_buf: Arc<IoBuf>,
}

#[derive(Default, Debug)]
struct IoBuf {
    segment: Arc<Segment>,
    buf: AtomicUsize,
}

#[derive(Default, Debug)]
struct Segment {
    offset: usize,
    segment_accountant: Arc<SegmentAccountant>,
}

#[derive(Default, Debug)]
struct SegmentAccountant {
    tip: AtomicUsize,
    free: Vec<Segment>,
}

impl SegmentAccountant {
    fn next_segment(self: Arc<SegmentAccountant>) -> Arc<Segment> {
        let offset = SZ + self.tip.fetch_add(SZ, Ordering::SeqCst);
        println!("setting new segment {}", offset);
        Arc::new(Segment { segment_accountant: self, offset })
    }
}

fn main() {
    let mut log = Log::new();
    {
        let _ = log.reserve(64);
        let _ = log.reserve(64);
    }
    println!("src/main.rs:70");
    {
        let _ = log.reserve(128);
    }
    println!("src/main.rs:74");
    {
        let _ = log.reserve(128);
    }
    println!("src/main.rs:78");
    {
        let _ = log.reserve(128);
    }
    println!("src/main.rs:77");
}

mod dropz {
    use super::*;

    impl Drop for IoBuf {
        fn drop(&mut self) {
            println!("IoBuf::drop");
        }
    }
    impl Drop for Segment {
        fn drop(&mut self) {
            println!("dropping Segment {:?}", self.offset);
        }
    }
    impl Drop for SegmentAccountant {
        fn drop(&mut self) {
            println!("SegmentAccountant::drop");
        }
    }
    impl Drop for Reservation {
        fn drop(&mut self) {
            println!("Reservation::drop");
        }
    }
}
