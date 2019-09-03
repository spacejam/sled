#!/bin/bash
set -eo pipefail
echo "cross build"
echo "https://github.com/rust-lang/cargo/issues/4753"
crates="sled pagecache"
targets="aarch64-fuchsia aarch64-linux-android"
targets="$targets i686-linux-android i686-unknown-linux-gnu"
targets="$targets x86_64-pc-windows-gnu x86_64-linux-android x86_64-fuchsia"
for crate in $crates; do
for target in $targets; do
  pushd crates/$crate
  echo "setting up $target..."
  rustup target add $target
  echo "checking $target..."
  cargo check --target $target
  popd
done
done
