extern crate env_logger;
extern crate log;
extern crate quickcheck;
extern crate rand;
extern crate sled;

pub mod tree;

#[cfg_attr(
    // only enable jemalloc on linux and macos by default
    // for fast tests
    all(
        any(target_os = "linux", target_os = "macos"),
        not(feature = "no_jemalloc"),
    ),
    global_allocator
)]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub fn setup_logger() {
    use std::io::Write;

    fn tn() -> String {
        std::thread::current()
            .name()
            .unwrap_or("unknown")
            .to_owned()
    }

    let mut builder = env_logger::Builder::new();
    builder
        .format(|buf, record| {
            writeln!(
                buf,
                "{:05} {:20} {:10} {}",
                record.level(),
                tn(),
                record
                    .module_path()
                    .unwrap()
                    .split("::")
                    .last()
                    .unwrap(),
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Info);

    if std::env::var("RUST_LOG").is_ok() {
        builder.parse(&std::env::var("RUST_LOG").unwrap());
    }

    let _r = builder.try_init();
}
