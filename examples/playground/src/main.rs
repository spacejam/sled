extern crate pagecache;
extern crate sled;

use sled::{ConfigBuilder, Db, Result};

fn basic() -> Result<()> {
    let config = ConfigBuilder::new().temporary(true).build();

    let db = Db::start(config)?;

    let k = b"k".to_vec();
    let v1 = b"v1".to_vec();
    let v2 = b"v2".to_vec();

    // set and get
    db.set(k.clone(), v1.clone())?;
    assert_eq!(db.get(&k).unwrap().unwrap(), (v1.clone()));

    // compare and swap
    match db.cas(k.clone(), Some(&v1.clone()), Some(v2.clone()))? {
        Ok(()) => println!("it worked!"),
        Err(actual) => println!("the actual current value is {:?}", actual),
    }

    // scan forward
    let mut iter = db.range(k.as_slice()..);
    let (k1, v1) = iter.next().unwrap().unwrap();
    assert_eq!(v1, v2.clone());
    assert_eq!(k1, k.clone());
    assert_eq!(iter.next(), None);

    // deletion
    db.del(&k)?;

    Ok(())
}

fn merge_operator() -> Result<()> {
    fn concatenate_merge(
        _key: &[u8],              // the key being merged
        old_value: Option<&[u8]>, // the previous value, if one existed
        merged_bytes: &[u8],      // the new bytes being merged in
    ) -> Option<Vec<u8>> {
        // set the new value, return None to delete
        let mut ret = old_value.map(|ov| ov.to_vec()).unwrap_or_else(|| vec![]);

        ret.extend_from_slice(merged_bytes);

        Some(ret)
    }

    let config = ConfigBuilder::new()
        .temporary(true)
        .merge_operator(concatenate_merge)
        .build();

    let db = Db::start(config)?;

    let k = b"k".to_vec();

    db.set(k.clone(), vec![0])?;
    db.merge(k.clone(), vec![1])?;
    db.merge(k.clone(), vec![2])?;
    assert_eq!(db.get(&*k).unwrap().unwrap(), (vec![0, 1, 2]));

    // sets replace previously merged data,
    // bypassing the merge function.
    db.set(k.clone(), vec![3])?;
    assert_eq!(db.get(&*k).unwrap().unwrap(), (vec![3]));

    // merges on non-present values will add them
    db.del(&*k)?;
    db.merge(k.clone(), vec![4])?;
    assert_eq!(db.get(&*k).unwrap().unwrap(), (vec![4]));

    Ok(())
}

fn main() -> Result<()> {
    basic()?;
    merge_operator()
}
