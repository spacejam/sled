extern crate rsdb;
extern crate coco;

use rsdb::*;

#[derive(Clone)]
pub struct TestMaterializer;

impl Materializer for TestMaterializer {
    type PageFrag = String;
    type Recovery = ();

    fn merge(&self, frags: &[String]) -> String {
        let mut consolidated = String::new();
        for frag in frags.into_iter() {
            consolidated.push_str(&*frag);
        }

        consolidated
    }

    fn recover(&self, _: &String) -> Option<()> {
        None
    }
}

#[test]
fn basic_recovery() {
    let path = "test_pagecache.log";
    let snapshot_path = "test_pagecache.snapshot";
    let conf = Config::default()
        .flush_every_ms(None)
        .path(Some(path.to_owned()))
        .snapshot_path(Some(snapshot_path.to_owned()));

    let mut pc = PageCache::new(TestMaterializer, conf.clone());
    pc.recover();
    let (id, key) = pc.allocate();
    let key = pc.set(id, key, "a".to_owned()).unwrap();
    let key = pc.merge(id, key, "b".to_owned()).unwrap();
    let _key = pc.merge(id, key, "c".to_owned()).unwrap();
    let (consolidated, _) = pc.get(id).unwrap();
    assert_eq!(consolidated, "abc".to_owned());
    drop(pc);

    let mut pc2 = PageCache::new(TestMaterializer, conf.clone());
    pc2.recover();
    let (consolidated2, key) = pc2.get(id).unwrap();
    assert_eq!(consolidated, consolidated2);

    pc2.merge(id, key, "d".to_owned()).unwrap();
    drop(pc2);

    let mut pc3 = PageCache::new(TestMaterializer, conf.clone());
    pc3.recover();
    let (consolidated3, _key) = pc3.get(id).unwrap();
    assert_eq!(consolidated3, "abcd".to_owned());
    pc3.free(id);
    drop(pc3);

    let mut pc4 = PageCache::new(TestMaterializer, conf.clone());
    pc4.recover();
    let res = pc4.get(id);
    assert!(res.is_none());
    pc4.__delete_all_files();
}
