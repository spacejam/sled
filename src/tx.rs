use std::sync::Arc;

use super::*;

pub struct TxStore {
    // tx table maps from TxID to Tx chain
    tx_table: Arc<Radix<Stack<Tx>>>,
    tree: Arc<Tree>,
}
