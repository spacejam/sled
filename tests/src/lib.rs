pub mod tree;

#[cfg_attr(
    // only enable jemalloc on linux and macos by default
    // for faster tests
    any(target_os = "linux", target_os = "macos"),
    global_allocator
)]
#[cfg(any(target_os = "linux", target_os = "macos"))]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[cfg_attr(
    // only enable mimalloc on windows by default
    // for faster tests
    target_os = "windows",
    global_allocator
)]
#[cfg(target_os = "windows")]
static ALLOC: mimallocator::Mimalloc = mimallocator::Mimalloc;

pub fn setup_logger() {
    color_backtrace::install();

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
                record.module_path().unwrap().split("::").last().unwrap(),
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Info);

    if std::env::var("RUST_LOG").is_ok() {
        builder.parse_filters(&std::env::var("RUST_LOG").unwrap());
    }

    let _r = builder.try_init();
}
