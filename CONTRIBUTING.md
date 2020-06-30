# Welcome to the Project :)

General considerations:

* API changes and additions should begin by opening an issue, and later move to implementation after it's clear that we are interested in changing the API in the proposed way. Significant changes that happen without prior discussion have a high risk of being closed.
* All PR's block on failing tests.
* All PR's block on breaking API changes (with the sole exception of the emerging C API).
* Don't be a jerk - here's our [code of conduct](./code-of-conduct.md).

There are a few areas that I would love external help with:

* Better docs: whatever you find confusing!
* Performance tuning: this should obliterate leveldb and rocksdb performance.
  if it doesn't, please submit a flamegraph and workload description!
* A better C API: the current one is pretty unfriendly.
* Generally any new kind of test that avoids biases inherent in the others.

Want to help sled but don't have time for individual contributions? Contribute via [GitHub Sponsors](https://github.com/sponsors/spacejam) to support the people pushing the project forward!
