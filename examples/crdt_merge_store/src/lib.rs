#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate bincode;
extern crate sled;
extern crate crdts;

use std::path::Path;

use crdts::{Orswot, VClock};
use bincode::{Infinite, deserialize, serialize};

const KEY: &'static [u8] = b"dat orswot";

pub struct OrswotStore {
    db: sled::Tree,
}

impl OrswotStore {
    pub fn new(path: &AsRef<Path>) -> OrswotStore {
        let config = sled::ConfigBuilder::new()
            .path(path)
            .merge_operator(orswot_merge)
            .build();

        OrswotStore {
            db: sled::Tree::start(config).unwrap(),
        }
    }

    pub fn get(&self) -> Orswot<Vec<u8>, DeviceID> {
        if let Some(bytes) = self.db.get(KEY).unwrap() {
            deserialize(&bytes).unwrap()
        } else {
            Orswot::new()
        }
    }

    pub fn merge(&self, other: Orswot<Vec<u8>, DeviceID>) {
        self.apply(Merge {
            other,
        });
    }

    pub fn add(&self, device: DeviceID, item: Vec<u8>) {
        self.apply(Add {
            device: device,
            item: item,
        });
    }

    pub fn remove(&self, context: VClock<DeviceID>, item: Vec<u8>) {
        self.apply(Remove {
            context: context,
            item: item,
        });
    }

    fn apply(&self, op: OrswotOp) {
        let op_bytes = serialize(&op, Infinite).unwrap();
        self.db.merge(KEY.to_vec(), op_bytes).unwrap();
    }
}

type DeviceID = Vec<u8>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum OrswotOp {
    Add { device: DeviceID, item: Vec<u8> },
    Remove {
        context: VClock<DeviceID>,
        item: Vec<u8>,
    },
    Merge { other: Orswot<Vec<u8>, DeviceID> },
}

use OrswotOp::*;

fn orswot_merge(
    _key: &[u8],
    old_value: Option<&[u8]>,
    merged_bytes: &[u8],
) -> Option<Vec<u8>> {
    let mut ret = old_value
        .map(|ov| deserialize(ov).unwrap())
        .unwrap_or_else(|| Orswot::new());

    let op = deserialize(merged_bytes).unwrap();

    match op {
        Add {
            device,
            item,
        } => ret.add(item, device),
        Remove {
            context,
            item,
        } => ret.remove_with_context(item, &context),
        Merge {
            other,
        } => ret.merge(other),
    }

    Some(serialize(&ret, Infinite).unwrap())
}

#[test]
fn store_works() {
    let store = OrswotStore::new(&"orswot_store");

    store.add(b"phone".to_vec(), vec![55]);

    let stored_1 = store.get();
    assert_eq!(stored_1.value(), vec![vec![55]]);

    let ctx = stored_1.precondition_context();
    assert_eq!(ctx.dots.len(), 1);

    store.remove(ctx, vec![55]);
    let stored_2 = store.get();
    assert!(stored_2.value().is_empty());
}
