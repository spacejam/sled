use super::*;

const FANOUT: usize = 3;

pub fn run_crash_object_cache() {
    let config = Config::new().flush_every_ms(Some(1)).path(OBJECT_CACHE_DIR);
    let (oc, collections, was_recovered): (ObjectCache<FANOUT>, _, bool) =
        ObjectCache::recover(&config).unwrap();

    // validate

    spawn_killah();

    loop {}
}
