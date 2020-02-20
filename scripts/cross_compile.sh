#!/bin/sh
set -e

# checks sled's compatibility using several targets

targets="wasm32-unknown-unknown aarch64-fuchsia aarch64-linux-android \
         i686-linux-android i686-unknown-linux-gnu \
         x86_64-linux-android x86_64-fuchsia \
         aarch64-apple-ios"

for target in $targets; do
  echo "setting up $target..."
  rustup target add $target
  echo "checking $target..."
  cargo check --target $target
done
