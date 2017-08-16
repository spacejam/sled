extern crate rsdb;
extern crate coco;

use rsdb::*;
use coco::epoch::pin;

#[derive(Clone)]
pub struct TestMaterializer;

impl Materializer for TestMaterializer {
    type MaterializedPage = String;
    type PartialPage = String;
    type Recovery = ();

    fn materialize(&self, frags: &[String]) -> String {
        self.consolidate(frags).pop().unwrap()
    }

    fn consolidate(&self, frags: &[String]) -> Vec<String> {
        let mut consolidated = String::new();
        for frag in frags.into_iter() {
            consolidated.push_str(&*frag);
        }

        vec![consolidated]
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

    let (id, consolidated) = pin(|scope| {
        let mut pc = PageCache::new(TestMaterializer, conf.clone());
        pc.recover();
        let (id, key) = pc.allocate();
        let key = pc.replace(id, key, vec!["a".to_owned()], scope).unwrap();
        let key = pc.prepend(id, key, "b".to_owned(), scope).unwrap();
        let _key = pc.prepend(id, key, "c".to_owned(), scope).unwrap();
        let (consolidated, _) = pc.get(id, scope).unwrap();
        assert_eq!(consolidated, "abc".to_owned());
        (id, consolidated)
    });

    pin(|scope| {
        let mut pc2 = PageCache::new(TestMaterializer, conf.clone());
        pc2.recover();
        let (consolidated2, key) = pc2.get(id, scope).unwrap();
        assert_eq!(consolidated, consolidated2);

        pc2.prepend(id, key, "d".to_owned(), scope).unwrap();
    });

    pin(|scope| {
        let mut pc3 = PageCache::new(TestMaterializer, conf.clone());
        pc3.recover();
        let (consolidated3, _key) = pc3.get(id, scope).unwrap();
        assert_eq!(consolidated3, "abcd".to_owned());
        pc3.free(id);
    });

    pin(|scope| {
        let mut pc4 = PageCache::new(TestMaterializer, conf.clone());
        pc4.recover();
        let res = pc4.get(id, scope);
        assert!(res.is_none());
        pc4.__delete_all_files();
    });
}
