use super::*;

pub fn run_crash_metadata_store() {
    let (metadata_store, recovered) =
        MetadataStore::recover(&HEAP_DIR).unwrap();

    // validate

    spawn_killah();

    loop {}
}
