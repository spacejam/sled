#!/bin/bash
set -ex

DIR=$(dirname "$0")

case `uname -s` in
  Darwin)
    proc_name=${1##*/}
    echo "expecting process name to be $proc_name"
    sudo dtrace -c "$*" -o out.stacks -n "profile-997 /execname == \"$proc_name\"/ { @[ustack(100)] = count(); }"
    ;;
  Linux)
    perf record -F 99 -g "$@"
    perf script > out.stacks
    ;;
  *)
    echo "Only Linux and OSX currently supported. Pull-requests open!"
    exit 1
esac

$DIR/stackcollapse.pl out.stacks | $DIR/flamegraph.pl > flamegraph.svg
sudo rm out.stacks

