#!/bin/bash

degree=$1

cargo run -r -p ppot2ark --features ppot2ark/parallel,ppot2ark/bls12-381 -- ./params 21 $degree ./params Response && 
cargo run -r -p amt --features amt/parallel,amt/bls12-381 --bin build_params -- $degree $degree ./params