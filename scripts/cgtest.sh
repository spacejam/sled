#!/bin/sh
set -e

cgdelete memory:sledTest || true
cgcreate -g memory:sledTest
echo 100M > /sys/fs/cgroup/memory/sledTest/memory.limit_in_bytes

su $SUDO_USER -c 'cargo build --release --features=testing'

for test in target/release/deps/test*; do
  if [[ -x $test ]]
  then
    echo running test: $test
    cgexec -g memory:sledTest $test --test-threads=1
    rm $test
  fi
done
