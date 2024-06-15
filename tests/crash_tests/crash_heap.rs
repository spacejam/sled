use super::*;

const FANOUT: usize = 3;

pub fn run_crash_heap() {
    let config = Config::new().path(HEAP_DIR);

    let HeapRecovery { heap, recovered_nodes, was_recovered } =
        Heap::recover(FANOUT, &config).unwrap();

    // validate

    spawn_killah();

    loop {}
}
