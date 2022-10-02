#!/bin/sh
set -e

# checks sled's compatibility using several targets

targets="wasm32-wasi wasm32-unknown-unknown aarch64-fuchsia aarch64-linux-android \
         i686-linux-android i686-unknown-linux-gnu \
         x86_64-linux-android x86_64-fuchsia \
         mips-unknown-linux-musl aarch64-apple-ios"

rustup update --no-self-update

RUSTFLAGS="--cfg miri" cargo check

rustup toolchain install 1.62 --no-self-update
cargo clean
rm Cargo.lock
cargo +1.62 check

for target in $targets; do
  echo "setting up $target..."
  rustup target add $target
  echo "checking $target..."
  cargo check --target $target
done

