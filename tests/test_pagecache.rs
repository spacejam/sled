extern crate rsdb;

use std::fs;

use rsdb::*;

pub struct TestMaterializer;

impl Materializer for TestMaterializer {
    type MaterializedPage = String;
    type PartialPage = String;

    fn materialize(&self, frags: Vec<String>) -> String {
        self.consolidate(frags).pop().unwrap()
    }

    fn consolidate(&self, frags: Vec<String>) -> Vec<String> {
        let mut consolidated = String::new();
        for frag in frags.into_iter() {
            consolidated.push_str(&*frag);
        }

        vec![consolidated]
    }
}

#[test]
fn basic_recovery() {
    let path = "test_pagecache.log";
    let pc = PageCache::new(TestMaterializer, Some(path.to_owned()));
    let (id, key) = pc.allocate();
    let key = pc.append(id, key, "a".to_owned()).unwrap();
    let key = pc.append(id, key, "b".to_owned()).unwrap();
    let _key = pc.append(id, key, "c".to_owned()).unwrap();
    let (consolidated, _) = pc.get(id).unwrap();
    assert_eq!(consolidated, "abc".to_owned());
    drop(pc);

    let pc2 = PageCache::new(TestMaterializer, Some(path.to_owned()));
    let (consolidated2, key) = pc2.get(id).unwrap();
    assert_eq!(consolidated, consolidated2);
    pc2.append(id, key, "d".to_owned()).unwrap();
    drop(pc2);

    let pc3 = PageCache::new(TestMaterializer, Some(path.to_owned()));
    let (consolidated3, _key) = pc3.get(id).unwrap();
    assert_eq!(consolidated3, "abcd".to_owned());
    pc3.free(id);
    drop(pc3);

    let pc4 = PageCache::new(TestMaterializer, Some(path.to_owned()));
    let res = pc4.get(id);
    assert_eq!(res, None);
    drop(pc4);

    fs::remove_file(path).unwrap();
}
