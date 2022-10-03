use sled::Config;

#[test]
fn test_exact_matching_of_tree_names() {
    let db = Config::new().temporary(true).flush_every_ms(None).open().unwrap();
    db.open_tree(String::from("X")).unwrap();
    db.open_tree(String::from("Y")).unwrap();
    db.open_tree(String::from("Z")).unwrap();

    let raw_names = db.tree_names();
    assert_eq!(raw_names.len(), 3);

    let deserialized: Vec<String> = raw_names
        .iter()
        .map(|ivec| String::from_utf8(ivec.to_vec()))
        .collect::<Result<Vec<String>, _>>()
        .unwrap();

    assert_eq!(deserialized.len(), 3);
    assert!(deserialized.contains(&String::from("X")));
    assert!(deserialized.contains(&String::from("Y")));
    assert!(deserialized.contains(&String::from("Z")));
}
