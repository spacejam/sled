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

[[ -f ${FILE:="./target/${1:-debug}/libsled_nodex.dylib"} ]] && cp ${FILE} sled.node
[[ -f ${FILE:="./target/${1:-debug}/libsled_nodex.so"} ]] && cp ${FILE} sled.node

node --napi-modules -e 'const sled = require("./sled.node"); console.log(sled)'

popd 2>/dev/null
