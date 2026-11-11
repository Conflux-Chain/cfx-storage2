#!/bin/bash
# 测试 30 配置，但跳过 AMT 节点最新值记录的预热（LVMT_SKIP_AMT_WARMUP=1），
# 三张缓存表（CommitID / FlatKV 历史索引 / AmtNode 历史索引）保持启用，
# 随机读 50 万账户的预热保持不变。用于量化 AMT 最新值预热在分段历史索引下的作用。
# 输出：report_t30_noamtwarm_pct100/timing.log, benchmark_t30_noamtwarm_pct100.log

set -euo pipefail
cd "$(dirname "$0")"

PCT_FILE=/sys/devices/system/cpu/intel_pstate/max_perf_pct
OUT=report_t30_noamtwarm_pct100
LOG=benchmark_t30_noamtwarm_pct100.log

bash ensure_params.sh
[ "$(cat "$PCT_FILE")" = "100" ] || { echo "[x] max_perf_pct = $(cat "$PCT_FILE"), not 100"; exit 1; }
[ "$(cat /sys/fs/cgroup/lvmt/memory.max 2>/dev/null)" = "$((8*1024*1024*1024))" ] \
    || { echo "[x] /sys/fs/cgroup/lvmt/memory.max is not 8 GiB"; exit 1; }
[ "$(git branch --show-current)" = "benchmark2_fixed" ] || { echo "[x] not on benchmark2_fixed"; exit 1; }
grep -q "LVMT_SKIP_AMT_WARMUP" src/bin/benchmark_lvmt.rs || { echo "[x] skip-warmup switch missing"; exit 1; }
[ ! -e "$OUT" ] || { echo "[x] $OUT already exists"; exit 1; }

sudo -v
cargo build --release --bin benchmark_lvmt -j "${BUILD_JOBS:-8}" > "${LOG%.log}_build.log" 2>&1 \
    || { echo "[x] build failed"; tail -40 "${LOG%.log}_build.log"; exit 1; }

bash -c 'echo $$ | sudo tee /sys/fs/cgroup/lvmt/cgroup.procs > /dev/null && exec "$@"' _ \
    env LVMT_SKIP_AMT_WARMUP=1 \
    ./target/release/benchmark_lvmt --no-stat -k 40m -a lvmt --max-epoch 500 \
    --epoch-size 10000 --report-epoch 250 \
    --pprof-report-to "$OUT" --warmup-from database > "$LOG" 2>&1
echo "max_perf_pct after run: $(cat "$PCT_FILE")" >> "$LOG"

grep -q "Skipping HistoryIndex(AmtNode)" "$LOG" || { echo "[x] warmup was not skipped"; exit 1; }
cat "$OUT/timing.log"
