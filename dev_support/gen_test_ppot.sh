#!/bin/bash

set -e

if [ ! -d "data" ]; then
    mkdir -p ./data
fi

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <degree>"
    exit 1
fi

degree=$1

pot_size=$((2**degree))

cargo run -r -p ppot --bin new_constrained_bls -- data/challenge_$degree $degree $pot_size

cargo run -r -p ppot --bin compute_constrained_bls -- data/challenge_$degree data/response_$degree $degree $pot_size <<< "some random text"

echo "The BLAKE2b hash of the response file is:"
b2sum data/response_$degree

echo "The response file contains the Powers of Tau parameters"