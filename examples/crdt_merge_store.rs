extern crate crdts;
extern crate sled;

use std::path::Path;

use bincode::{deserialize, serialize};
use crdts::{Orswot, VClock};
use serde::{Serialize, Deserialize};

const KEY: &[u8] = b"dat orswot";

pub struct OrswotStore {
    db: sled::Db,
}

impl OrswotStore {
    pub fn new(path: &dyn AsRef<Path>) -> Self {
        let config = sled::ConfigBuilder::new().path(path).build();

        let db = sled::Db::start(config).unwrap();
        db.set_merge_operator(orswot_merge);

        Self { db }
    }

    pub fn get(&self) -> Orswot<Vec<u8>, DeviceID> {
        if let Some(bytes) = self.db.get(KEY).unwrap() {
            deserialize(&bytes).unwrap()
        } else {
            Orswot::new()
        }
    }

    pub fn merge(&self, other: Orswot<Vec<u8>, DeviceID>) {
        self.apply(Merge { other });
    }

    pub fn add(&self, device: DeviceID, item: Vec<u8>) {
        self.apply(Add {
            device,
            item,
        });
    }

    pub fn remove(&self, context: VClock<DeviceID>, item: Vec<u8>) {
        self.apply(Remove {
            context,
            item,
        });
    }

    fn apply(&self, op: OrswotOp) {
        let op_bytes = serialize(&op).unwrap();
        self.db.merge(KEY.to_vec(), op_bytes).unwrap();
    }
}

type DeviceID = Vec<u8>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum OrswotOp {
    Add {
        device: DeviceID,
        item: Vec<u8>,
    },
    Remove {
        context: VClock<DeviceID>,
        item: Vec<u8>,
    },
    Merge {
        other: Orswot<Vec<u8>, DeviceID>,
    },
}

use OrswotOp::{Add, Remove, Merge};

fn orswot_merge(
    _key: &[u8],
    old_value: Option<&[u8]>,
    merged_bytes: &[u8],
) -> Option<Vec<u8>> {
    let mut ret = old_value
        .map_or_else(Orswot::new, |ov| deserialize(ov).unwrap());

    let op = deserialize(merged_bytes).unwrap();

    match op {
        Add { device, item } => ret.add(item, device),
        Remove { context, item } => ret.remove_with_context(item, &context),
        Merge { other } => ret.merge(other),
    }

    Some(serialize(&ret).unwrap())
}

fn main() {
    let store = OrswotStore::new(&"orswot_store");

    store.add(b"phone".to_vec(), vec![55]);

    let stored_1 = store.get();
    assert_eq!(stored_1.value(), vec![vec![55]]);

    let ctx = stored_1.precondition_context();
    assert_eq!(ctx.dots.len(), 1);

    store.remove(ctx, vec![55]);
    let stored_2 = store.get();
    assert!(stored_2.value().is_empty());

    // cleanup
    drop(store);
    std::fs::remove_dir_all("orswot_store").unwrap();
}
