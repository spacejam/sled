let addon = require('../native');

function open (path) {
    let ptr_str = addon.createDb(path);
    console.log("Created DB at", path, ", Pointer",  ptr_str);

    return {
        set: (k, v) => {
            //console.log("SET", ptr_str, k, v);
            return addon.set(ptr_str, k, v);
        },
        get: (k) => {
            //console.log("GET", ptr_str, k);
            return addon.get(ptr_str, k);
        },
        del: (k) => {
            return addon.del(ptr_str, k);
        },
        syncAndClose: () => {
            console.log("Saving DB and closing", ptr_str);
            addon.syncAndClose(ptr_str);
        }
    }
}

module.exports.open = open;
