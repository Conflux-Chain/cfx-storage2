#!/bin/bash

# Before running the script for the first time, please perform the following steps:
# - Grant execution permission: chmod +x run_lvmt.sh
# - Ensure the cgroup has been created:
# sudo mkdir /sys/fs/cgroup/lvmt
#    and an 8 GB memory limit has been set:
# echo $((8*1024*1024*1024)) | sudo tee /sys/fs/cgroup/lvmt/memory.max

echo ">>> Checking prerequisites..."
bash ensure_params.sh

# Check the exit status of ensure_params.sh. If it fails, exit the test script.
if [ $? -ne 0 ]; then
    echo "[x] Prerequisites check failed. Exiting test."
    exit 1
fi

echo ">>> Prerequisites met. Starting the test..."

# 1. Move the current script's process (PID: $$) into the 'lvmt' cgroup.
#    This is the correct method for cgroup v2.
#    We use the 'tee' command to gracefully handle sudo file write permissions.
echo $$ | sudo tee /sys/fs/cgroup/lvmt/cgroup.procs > /dev/null

# 2. Combine all command-line arguments into a single command string.
# COMMAND=$@
# Note: Hardcoding test parameters here to ensure test reproducibility.
# Logic explanation: Each epoch contains 10,000 reads and 10,000 writes (i.e., 20,000 operations).
# 500 * 20,000 = 10 million read/write operations, meeting the test report requirements.
COMMAND="cargo run --release --bin benchmark_lvmt -- --no-stat -k 40m -a lvmt --max-epoch 500 --epoch-size 10000 --report-epoch 250 --pprof-report-to report --warmup-from database > benchmark.log 2>&1"

# 3. Execute the command.
#    Because this bash process is a child process of the current script, it will automatically inherit the parent process's cgroup settings.
#    Therefore, $COMMAND will run under the memory limit of the 'lvmt' cgroup.
echo "Executing command in the 'lvmt' cgroup: $COMMAND"
bash -c "$COMMAND"