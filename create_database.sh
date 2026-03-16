#!/bin/bash

echo ">>> Checking prerequisites..."
bash ensure_params.sh

# Check the exit status of ensure_params.sh. If it fails, exit the test script.
if [ $? -ne 0 ]; then
    echo "[x] Prerequisites check failed. Exiting test."
    exit 1
fi

echo ">>> Prerequisites met. Starting the test..."

cargo run --release --bin create_database -- --no-stat -k 40m -a lvmt --max-epoch 500 --epoch-size 10000 --report-epoch 100 --warmup-to database > create_database.log 2>&1