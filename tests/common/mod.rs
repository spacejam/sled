#[cfg(not(feature = "testing"))]
compile_error!(
    "please run tests using the \"testing\" feature, \
     which enables additional checks at runtime and \
     causes more race conditions to jump out by \
     inserting delays in concurrent code."
);

pub fn setup_logger() {
    use std::io::Write;

    fn tn() -> String {
        std::thread::current().name().unwrap_or("unknown").to_owned()
    }

    #[cfg(feature = "pretty_backtrace")]
    color_backtrace::install();

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

#[allow(dead_code)]
pub fn cleanup(dir: &str) {
    let dir = std::path::Path::new(dir);
    if dir.exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
}
