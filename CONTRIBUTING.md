# Welcome to the Project :)
Now that some property, sanitization, crash and interleaving-exercising tests exist,
it's safe to open this up for wider contribution! If you accidentally break something,
there is a pretty good chance the tests will explode! So go wild!

There are a ton of features and interesting directions this project can go in, and
far more than I can implement on my own in a reasonable timespan. I want to scale out
feature ownership to interested people. See the "plans" section on the main 
[README](/README.md) for an overview.

If you would like to help out, but don't know how, I probably have time to mentor you!
The more people I teach about the underlying architecture, the more I realize
certain things can be fixed up, and I have a lot of fun teaching, so you're doing me
favors!

Specifically, there are a few areas that I would love external help with:

* better docs: whatever you find confusing!
* performance tuning: this should theoretically be able to outperform rocksdb on reads,
  and innodb on writes! let's get there! I usually run this to generate and view flamegraphs:
  ```
  cargo build -p stress2; hack/flamerun.sh ./target/debug/stress2 --duration 5; firefox ./flamegraph.svg
  ```
* a better C API: the current one is pretty unfriendly
* generally any new kind of test that avoids biases inherent in the others
* the deep end: prototyping a compiler plugin that allows specifiable blocks of code to be
instrumented with pauses that cause interesting interleavings to be exhausted.

General considerations:

* all PR's block on failing tests
* all PR's block on breaking API changes (with the sole exception of the emerging C API)
