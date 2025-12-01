#!/bin/bash

echoStep() {
    echo -e "\n\033[1;34m────────────────────────────────────────────────────────"
    echo -e "\033[1;34m$1."
    echo -e "\033[1;34m────────────────────────────────────────────────────────\033[0m"
}

set -e

echoStep "Clean"
cargo clean

echoStep "Download AMT parameters"
wget -P pp/ https://conflux-rust-dev.s3.ap-east-1.amazonaws.com/amt-params/amt-prove-mont-nxssWC-16-16.bin &

( # Start of the block that should run concurrently with the download
    # Environment setting for clang18
    # export LIBCLANG_PATH=/usr/lib/llvm-18/lib
    # export LD_LIBRARY_PATH=/usr/lib/llvm-18/lib:$LD_LIBRARY_PATH
    # export CXXFLAGS="-include cstdint"
    # echo $LIBCLANG_PATH
    # echo $LD_LIBRARY_PATH
    # echo $CXXFLAGS

    echoStep "Check fmt"
    ./cargo_fmt.sh -- --check

    export RUSTFLAGS="-D warnings" 

    echoStep "Check all"
    cargo check --all

    echoStep "Check all tests"
    cargo check --all --tests --benches

    echoStep "Check clippy"
    cargo clippy --all-targets --all-features -- -D warnings
) & # End of the block, run this block in the background

# Wait for both the download and the block of cargo operations to finish
wait
echoStep "Test"
cargo test -r --all