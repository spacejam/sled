use super::*;

const FANOUT: usize = 3;

pub fn run_crash_heap() {
    let path = std::path::Path::new(CRASH_DIR).join(HEAP_DIR);
    let config = Config::new().path(path);

    let HeapRecovery { heap, recovered_nodes, was_recovered } =
        Heap::recover(FANOUT, &config).unwrap();

    // validate

    spawn_killah();

    loop {}
}
