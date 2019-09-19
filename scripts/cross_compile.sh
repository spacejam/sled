#!/bin/sh
set -e

# checks sled's compatibility using several targets

targets="aarch64-fuchsia aarch64-linux-android i686-linux-android i686-unknown-linux-gnu x86_64-pc-windows-gnu x86_64-linux-android x86_64-fuchsia"

for target in $targets; do
  echo "setting up $target..."
  rustup target add $target
  echo "checking $target..."
  cargo check --target $target
done
