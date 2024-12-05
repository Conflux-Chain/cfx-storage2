#!/bin/bash

echoStep() {
    echo -e "\n\033[1;34m────────────────────────────────────────────────────────"
    echo -e "\033[1;34m$1."
    echo -e "\033[1;34m────────────────────────────────────────────────────────\033[0m"
}

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

rm data/*381
rm data/*254
rm -rf ./pp/*-05.bin

echoStep "Test"
cargo test -r --all

echoStep "Test amt, use previous pp"
cargo test -r -- lvmt::amt

rm -rf ./pp/*-05.bin

echoStep "Test (parallel)"
cargo test -r --all --features parallel

echoStep "Test amt (parallel), use previous pp"
cargo test -r --features parallel -- lvmt::amt

rm -rf ./pp/*-05.bin

echoStep "Test amt (bn254)"
cargo test -r --features parallel,bn254 -- lvmt::amt

echoStep "Test amt (bls12-381)"
cargo test -r --features parallel,bls12-381 -- lvmt::amt

echoStep "Test amt (bn254), use previous pp"
cargo test -r --features parallel,bn254 -- lvmt::amt

echoStep "Test amt (bls12-381), use previous pp"
cargo test -r --features parallel,bls12-381 -- lvmt::amt
