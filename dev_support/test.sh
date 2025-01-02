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

echoStep "Check all tests"
cargo check --all --tests --benches

echoStep "Check clippy"
cargo clippy --all-targets --all-features -- -D warnings

echoStep "Test"
cargo test -r --all

if [ "$1" == "run-ignored" ]; then
    echoStep "Test ignored"
    cargo test -r --all -- --ignored
fi