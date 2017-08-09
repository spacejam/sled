extern crate rsdb;

#[macro_use]
extern crate serde_derive;

use std::fs;

use rsdb::*;

#[derive(Clone, Serialize, Deserialize)]
pub struct TestMaterializer;

impl Materializer for TestMaterializer {
    type MaterializedPage = String;
    type PartialPage = String;
    type Recovery = ();

    fn materialize(&self, frags: &Vec<String>) -> String {
        self.consolidate(frags).pop().unwrap()
    }

    fn consolidate(&self, frags: &Vec<String>) -> Vec<String> {
        let mut consolidated = String::new();
        for frag in frags.into_iter() {
            consolidated.push_str(&*frag);
        }

        vec![consolidated]
    }

    fn recover(&mut self, _: &String) -> Option<()> {
        None
    }
}

#[test]
fn basic_recovery() {
    let path = "test_pagecache.log";
    let conf = Config::default().path(Some(path.to_owned()));
    let pc = PageCache::new(TestMaterializer, conf.clone());
    let (id, key) = pc.allocate();
    let key = pc.prepend(id, key, "a".to_owned()).unwrap();
    let key = pc.prepend(id, key, "b".to_owned()).unwrap();
    let _key = pc.prepend(id, key, "c".to_owned()).unwrap();
    let (consolidated, _) = pc.get(id).unwrap();
    assert_eq!(consolidated, "abc".to_owned());
    drop(pc);

    let mut pc2 = PageCache::new(TestMaterializer, conf.clone());
    pc2.recover(0);
    let (consolidated2, key) = pc2.get(id).unwrap();
    assert_eq!(consolidated, consolidated2);

    pc2.prepend(id, key, "d".to_owned()).unwrap();
    drop(pc2);

    let mut pc3 = PageCache::new(TestMaterializer, conf.clone());
    pc3.recover(0);
    let (consolidated3, _key) = pc3.get(id).unwrap();
    assert_eq!(consolidated3, "abcd".to_owned());
    pc3.free(id);
    drop(pc3);

    let mut pc4 = PageCache::new(TestMaterializer, conf.clone());
    pc4.recover(0);
    let res = pc4.get(id);
    assert_eq!(res, None);
    drop(pc4);

    fs::remove_file(path).unwrap();
}
