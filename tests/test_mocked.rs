mod common;
mod mocks;
mod tree;

use mocks::VirtualFileSystem;

#[test]
fn test_tree_mock_vfs() {
    let db = sled::Config::default()
        .mock_io(Box::new(VirtualFileSystem::new()))
        .open()
        .unwrap();
    db.insert("key", "value").unwrap();
    db.flush().unwrap();
    assert_eq!(db.iter().count(), 1);
    assert_eq!(db.get("key").unwrap(), Some("value".as_bytes().into()));

    let new_config = sled::Config::default().chain_mocked_io(&db.context);
    drop(db);

    let db = new_config.open().unwrap();
    assert_eq!(db.iter().count(), 1);
    assert_eq!(db.get("key").unwrap(), Some("value".as_bytes().into()));
    drop(db);

    let db = sled::Config::default()
        .mock_io(Box::new(VirtualFileSystem::new()))
        .temporary(true)
        .open()
        .unwrap();
    db.insert("key", "value").unwrap();
    db.flush().unwrap();
    assert_eq!(db.iter().count(), 1);
    assert_eq!(db.get("key").unwrap(), Some("value".as_bytes().into()));
}
