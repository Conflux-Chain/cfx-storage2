#!/bin/bash

echoStep() {
    echo -e "\n\033[1;34m────────────────────────────────────────────────────────"
    echo -e "\033[1;34m$1."
    echo -e "\033[1;34m────────────────────────────────────────────────────────\033[0m"
}

rm ppot2ark/data/*12*381
rm ppot2ark/data/*12*254
rm -rf amt/pp/*-05.bin

set -e

echoStep "Clean"
cargo clean

echoStep "Check fmt"
./cargo_fmt.sh -- --check

export RUSTFLAGS="-D warnings" 

echoStep "Check all"
cargo check --all
echoStep "Check all (parallel)"
cargo check --all --features parallel

echoStep "Check all tests"
cargo check --all --tests --benches
echoStep "Check all tests (parallel)"
cargo check --all --tests --benches --features parallel

echoStep "Check clippy"
cargo clippy
echoStep "Check clippy (parallel)"
cargo clippy --features parallel

echoStep "Test"
cargo test -r --all

echoStep "Test bn254 (parallel)"
cargo test -r --features parallel,bn254

#echoStep "Test ignore (bn254), must parallel"
#cargo test -r --features parallel,bn254 -- --ignored

echoStep "Test bls12-381 (parallel)"
cargo test -r --features parallel,bls12-381

#echoStep "Test ignore (bls12-381), must parallel"
#cargo test -r --features parallel,bls12-381 -- --ignored

echoStep "Test amt (parallel, bn254), use previous pp"
cargo test -r -p amt --features parallel,bn254

echoStep "Test amt (parallel, bls12-381), use previous pp"
cargo test -r -p amt --features parallel,bls12-381