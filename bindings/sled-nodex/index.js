const { sled } = require('./sled.node')
const fs = require('fs')

let db = new sled({
  path: "./index.db",
  use_compression: false,
})

fs.readFile('./sled.node', (err, data) => {
  let key = new TextEncoder().encode("sled.node").buffer

  let item = db.insert(
    key,
    Uint8Array.from(data).buffer,
  )
  if (item) {
    console.log("insert: ", item)
  }

  console.log("get: ", db.get(key))

  let old = db.remove(key)
  if (old) {
    console.log('remove old:', old);
  }

  setTimeout(() => {
    let tree = db.open_tree("test-tree")

    let item = tree.insert(
      key,
      Uint8Array.from(data).buffer,
    )
    if (item) {
      console.log("tree insert: ", item)
    }

    console.log("tree get: ", tree.get(key))

    let old = tree.remove(key)
    if (old) {
      console.log('tree remove old:', old);
    }

    for (let name of db.tree_names()) {
      console.log("tree:", new TextDecoder().decode(name))
    }

  }, 1000)
})

