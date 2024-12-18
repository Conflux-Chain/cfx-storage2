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
cargo clippy

echoStep "Test"
cargo test -r --all

#echoStep "Test ignore, must parallel"
#cargo test -r -- --ignored