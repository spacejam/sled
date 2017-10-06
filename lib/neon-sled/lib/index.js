const sled = require('../native');

function open (path) {
    console.log("Creating a modern embedded database at", path);
    let ptr_str = sled.createDb(path);
    console.log("Sled at pointer",  ptr_str);

    return {
        set: (k, v) => {
            //console.log("SET", ptr_str, k, v);
            return sled.set(ptr_str, k, v);
        },
        get: (k) => {
            //console.log("GET", ptr_str, k);
            return sled.get(ptr_str, k);
        },
        del: (k) => {
            return sled.del(ptr_str, k);
        },
        syncAndClose: () => {
            console.log("Saving DB and closing", ptr_str);
            sled.syncAndClose(ptr_str);
            console.log("Saved DB and closing", ptr_str);
        }
    }
}

module.exports = open;
