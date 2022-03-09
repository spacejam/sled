#!/bin/bash
pushd $(dirname $0) 2>/dev/null

if [[ -n "${1}" ]]; then
  echo ${1} ' build'
  cargo build --${1:-release}
else
  echo 'debug build'
  cargo build
fi

rm -rf "sled.node" 2>/dev/null
cp ./target/${1:-debug}/libsled_nodex.dylib sled.node

node --napi-modules -e 'const sled = require("./sled.node"); console.log(sled)'

popd 2>/dev/null
