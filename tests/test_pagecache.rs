extern crate rsdb;

use std::thread;
use std::sync::Arc;

use rsdb::*;

macro_rules! par {
    ($pc:ident, $f:expr) => {
        let mut threads = vec![];
        for tn in 0..N_THREADS {
            let sz = N / N_THREADS;
            let page_cache = $pc.clone();
            let thread = thread::Builder::new()
                .name(format!("t({})", tn))
                .spawn(move || {
                    for i in (tn * sz)..((tn + 1) * sz) {
                        let k = kv(i);
                        $f(&*page_cache, k);
                    }
                })
                .unwrap();
            threads.push(thread);
        }
        while let Some(thread) = threads.pop() {
            thread.join().unwrap();
        }
    };
}

pub struct TestMaterializer;

impl PageMaterializer for TestMaterializer {
    type MaterializedPage = String;
    type PartialPage = String;

    fn materialize(&self, frags: Vec<String>) -> String {
        self.consolidate(frags).pop().unwrap()
    }

    fn consolidate(&self, mut frags: Vec<String>) -> Vec<String> {
        let mut consolidated = String::new();
        for frag in frags.into_iter() {
            consolidated.push_str(&*frag);
        }

        vec![consolidated]
    }
}

#[test]
fn basic_recovery() {
    let pc = PageCache::new(TestMaterializer, Some("pc_test.log".to_owned()));
    let (id, key) = pc.allocate();
    let key = pc.append(id, key, "a".to_owned()).unwrap();
    let key = pc.append(id, key, "b".to_owned()).unwrap();
    let key = pc.append(id, key, "c".to_owned()).unwrap();

    let (consolidated, _) = pc.get(id).unwrap();
    assert_eq!(consolidated, "abc".to_owned());

    drop(pc);

    let pc2 = PageCache::new(TestMaterializer, Some("pc_test.log".to_owned()));

    let (consolidated2, _) = pc2.get(id).unwrap();

    assert_eq!(consolidated, consolidated2);
}
