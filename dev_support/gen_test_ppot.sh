#!/bin/bash

set -e

if [ ! -d "data" ]; then
    mkdir -p ./data
fi

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <degree> <feature>"
    exit 1
fi

degree=$1

pot_size=$((2**degree))

feature=$2

echo $feature

cargo run -r -p ppot --bin new_constrained_bls --features $feature -- data/challenge_${degree}_${feature} $degree $pot_size

cargo run -r -p ppot --bin compute_constrained_bls --features $feature -- data/challenge_${degree}_${feature} data/response_${degree}_${feature} $degree $pot_size <<< "some random text"

echo "The BLAKE2b hash of the response file is:"
b2sum data/response_$degree

echo "The response file contains the Powers of Tau parameters"