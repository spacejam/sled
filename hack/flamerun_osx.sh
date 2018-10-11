#!/bin/bash
set -ex

proc_name=${1##*/}
echo "expecting process name to be $proc_name"
sudo dtrace -c "$*" -o out.stacks -n "profile-997 /execname == \"$proc_name\"/ { @[ustack(100)] = count(); }"
./hack/stackcollapse.pl out.stacks | ./hack/flamegraph.pl > flamegraph.svg
sudo rm out.stacks
