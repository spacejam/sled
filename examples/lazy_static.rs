extern crate sled;

use sled::{Config, Result};

// use lazy_static to instantiate a global sled configuration
use lazy_static::lazy_static;
lazy_static! {
    pub static ref DB: sled::Db = Config::new().temporary(true).open()
        .unwrap_or_else(|e| {
            panic!("{}", e);
        });
}


fn basic() -> Result<()> {
    let k = b"k".to_vec();
    let v1 = b"v1".to_vec();
    let v2 = b"v2".to_vec();

    // set and get
    DB.insert(k.clone(), v1.clone())?;
    assert_eq!(DB.get(&k).unwrap().unwrap(), (v1));

    // compare and swap
    match DB.compare_and_swap(k.clone(), Some(&v1), Some(v2.clone()))? {
        Ok(()) => println!("it worked!"),
        Err(sled::CompareAndSwapError { current: cur, proposed: _ }) => {
            println!("the actual current value is {:?}", cur)
        }
    }

    // scan forward
    let mut iter = DB.range(k.as_slice()..);
    let (k1, v1) = iter.next().unwrap().unwrap();
    assert_eq!(v1, v2);
    assert_eq!(k1, k);
    assert_eq!(iter.next(), None);

    // deletion
    DB.remove(&k)?;

    Ok(())
}


fn main() -> Result<()> {
    basic()?;
}
