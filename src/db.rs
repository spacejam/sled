use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

pub struct DB {
    // tx table maps from TxID to Tx chain
    tx_table: Arc<Radix<Stack<Tx>>>,
    tree: Arc<Tree>,
    // end_stable_log is the highest LSN that may be flushed
    esl: Arc<AtomicUsize>,
}
