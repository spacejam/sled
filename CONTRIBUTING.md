# Welcome to the Project :)

* Don't be a jerk - here's our [code of conduct](./code-of-conduct.md).
  We have a track record of defending our community from harm.

There are at least three great ways to contribute to sled:

* [financial contribution](https://github.com/sponsors/spacejam)
* coding
* conversation

#### Coding Considerations:

Please don't waste your time or ours by implementing things that
we do not want to introduce and maintain. Please discuss in an
issue or on chat before submitting a PR with:

* public API changes
* new functionality of any sort
* additional unsafe code
* significant refactoring

The above changes are unlikely to be merged or receive
timely attention without prior discussion.

PRs that generally require less coordination beforehand:

* Anything addressing a correctness issue.
* Better docs: whatever you find confusing!
* Small code changes with big performance implications, substantiated with [responsibly-gathered metrics](https://sled.rs/perf#experiment-checklist).
* FFI submodule changes: these are generally less well maintained than the Rust core, and benefit more from public assistance.
* Generally any new kind of test that avoids biases inherent in the others.

#### All PRs block on failing tests!

sled has intense testing, including crash tests, multi-threaded tests with
delay injection, a variety of mechanically-generated tests that combine fault
injection with concurrency in interesting ways, cross-compilation and minimum
supported Rust version checks, LLVM sanitizers, and more. It can sometimes be
challenging to understand why something is failing these intense tests.

For better understanding test failures, please:

1. read the failing test name and output log for clues
1. try to reproduce the failed test locally by running its assocated command from the [test script](https://github.com/spacejam/sled/blob/main/.github/workflows/test.yml)
1. If it is not clear why your test is failing, feel free to request help with understanding it either on discord or requesting help on the PR, and we will do our best to help.

Want to help sled but don't have time for individual contributions? Contribute via [GitHub Sponsors](https://github.com/sponsors/spacejam) to support the people pushing the project forward!
