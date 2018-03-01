extern crate env_logger;
extern crate log;

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
            write!(
                buf,
                "{:05} {:10} {:10} {}\n",
                record.level(),
                tn(),
                record.module_path().unwrap().split("::").last().unwrap(),
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Info);

    if std::env::var("RUST_LOG").is_ok() {
        builder.parse(&std::env::var("RUST_LOG").unwrap());
    }

    let _r = builder.init();
}
