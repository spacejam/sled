var addon = require('../native');

console.log(addon.set("foo_key","bar_val"));

module.exports.set = addon.set;
module.exports.get = addon.get;

