# Building older revisions

`master` and the revisions up to the switch to Rust 1.86 do not build out of the box in a current environment. None of the causes is in this crate's code.

## 1. rocksdb submodule URL

kvdb-rocksdb `507a6f8` (used by `master`) depends on rust-rocksdb `f4cbe0b`, whose `.gitmodules` points the `rocksdb` submodule to `facebook/rocksdb`. The pinned submodule commit `0bb38ab` is not on any branch there; it is on branch `v7.4.4_fix` of `rongma7/rocksdb`. Let Cargo fetch with the git CLI and redirect the URL:

```sh
export CARGO_NET_GIT_FETCH_WITH_CLI=true
export GIT_CONFIG_COUNT=1
export GIT_CONFIG_KEY_0=url.https://github.com/rongma7/rocksdb.git.insteadOf
export GIT_CONFIG_VALUE_0=https://github.com/facebook/rocksdb.git
```

## 2. No Cargo.lock

`Cargo.lock` is not tracked, so dependencies are resolved anew on every build, and today's versions need a newer Cargo than the pinned 1.77.2. Use Rust 1.86 and let Cargo pick versions that support it:

```sh
export RUSTUP_TOOLCHAIN=1.86
export CARGO_RESOLVER_INCOMPATIBLE_RUST_VERSIONS=fallback
```

## 3. bindgen 0.60 and clang 18

The rust-rocksdb revisions with bindgen 0.60 (`f4cbe0b`, `9183fa9`, `be41c52`) panic with libclang 18. Use libclang 14 (e.g. `apt-get install libclang-14-dev` on Ubuntu 24.04):

```sh
export LIBCLANG_PATH=/usr/lib/llvm-14/lib
```

## 4. rocksdb 7.4.4 and GCC 13

rocksdb 7.4.4 misses `#include <cstdint>`, which GCC 13 no longer pulls in transitively:

```sh
export CXXFLAGS="-include cstdint"
```

## Tests

The LVMT tests read the AMT parameters from `./pp` (see `dev_support/test.sh` for the download), then:

```sh
cargo test -r --all
```
