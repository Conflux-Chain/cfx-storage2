#!/bin/bash

echo ">>> Checking prerequisites..."
bash ensure_params.sh

# Check the exit status of ensure_params.sh. If it fails, exit the test script.
if [ $? -ne 0 ]; then
    echo "[x] Prerequisites check failed. Exiting test."
    exit 1
fi

echo ">>> Prerequisites met. Starting the test..."

cargo run --release --bin verify_database > verify_database.log 2>&1