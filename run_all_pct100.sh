#!/bin/bash
# 在 intel_pstate max_perf_pct=100 下运行三组性能实验。
# 只写入 *_pct100 新目录，不动 report/、report_sysalloc/、report_full_naive/。
#
#   1. 测试 30                : benchmark2_fixed,             jemalloc,      database/
#   2. 分配器对照             : benchmark2_fixed_sysalloc,    --features system-alloc
#   3. 朴素索引对照           : benchmark2_fixed_revertPR29,  database/LVMT_4e7_naive
#
# 前提：cgroup lvmt 已建好且 memory.max = 8 GiB；跟踪文件无未提交改动（先 git stash）。

set -euo pipefail
cd "$(dirname "$0")"

PCT_FILE=/sys/devices/system/cpu/intel_pstate/max_perf_pct
NAIVE_DB_SRC=database/LVMT_4e7_naive
NAIVE_DB_BASE=database_naive_base   # benchmark 读取 <base>/LVMT_4e7/

check_pct() {
    local v; v=$(cat "$PCT_FILE")
    [ "$v" = "100" ] || { echo "[x] max_perf_pct = $v, not 100"; exit 1; }
}

# ---- 前置检查 ----
bash ensure_params.sh
check_pct
if [ "$(cat /sys/fs/cgroup/lvmt/memory.max 2>/dev/null)" != "$((8*1024*1024*1024))" ]; then
    echo "[x] /sys/fs/cgroup/lvmt/memory.max is not 8 GiB"; exit 1
fi
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "[x] Tracked files have uncommitted changes; run 'git stash' first."; exit 1
fi
for d in report_t30_pct100 report_sysalloc_pct100 report_full_naive_pct100; do
    [ ! -e "$d" ] || { echo "[x] $d already exists; move it away first"; exit 1; }
done
[ -d database/LVMT_4e7 ] || { echo "[x] database/LVMT_4e7 not found"; exit 1; }
[ -d "$NAIVE_DB_SRC" ]   || { echo "[x] $NAIVE_DB_SRC not found"; exit 1; }
[ ! -e "$NAIVE_DB_BASE" ] || { echo "[x] $NAIVE_DB_BASE already exists"; exit 1; }

ORIG_BRANCH=$(git branch --show-current)
restore() {
    if [ -d "$NAIVE_DB_BASE/LVMT_4e7" ]; then
        mv "$NAIVE_DB_BASE/LVMT_4e7" "$NAIVE_DB_SRC"
        rmdir "$NAIVE_DB_BASE"
    fi
    git checkout -q "$ORIG_BRANCH"
}
trap restore EXIT

sudo -v

# run_one <branch> <extra cargo args> <db base> <report dir> <log>
run_one() {
    local branch=$1 features=$2 db_base=$3 report_dir=$4 log=$5
    echo ">>> [$branch $features] building"
    git checkout -q "$branch"
    grep -q "notify_epoch(delta_epoch" src/bin/benchmark_lvmt.rs \
        || { echo "[x] $branch lacks the report-window fix"; exit 1; }
    # 在 cgroup 外编译，并行度调低（32 并发编 jemalloc 曾导致 gcc 崩溃）
    # shellcheck disable=SC2086
    if ! cargo build --release --bin benchmark_lvmt $features -j "${BUILD_JOBS:-8}" \
            > "${log%.log}_build.log" 2>&1; then
        echo "[x] build failed, see ${log%.log}_build.log"; tail -40 "${log%.log}_build.log"; exit 1
    fi
    check_pct
    echo ">>> [$branch $features] running -> $report_dir/timing.log"
    if ! bash -c 'echo $$ | sudo tee /sys/fs/cgroup/lvmt/cgroup.procs > /dev/null && exec "$@"' _ \
        ./target/release/benchmark_lvmt --no-stat -k 40m -a lvmt --max-epoch 500 \
        --epoch-size 10000 --report-epoch 250 \
        --pprof-report-to "$report_dir" --warmup-from "$db_base" > "$log" 2>&1; then
        echo "[x] benchmark failed, see $log"; tail -40 "$log"; exit 1
    fi
    echo "max_perf_pct after run: $(cat "$PCT_FILE")" >> "$log"
    cat "$report_dir/timing.log"
}

run_one benchmark2_fixed            ""                         database "report_t30_pct100"      benchmark_t30_pct100.log
run_one benchmark2_fixed_sysalloc   "--features system-alloc"  database "report_sysalloc_pct100" benchmark_sysalloc_pct100.log

mkdir "$NAIVE_DB_BASE"
mv "$NAIVE_DB_SRC" "$NAIVE_DB_BASE/LVMT_4e7"
run_one benchmark2_fixed_revertPR29 ""                         "$NAIVE_DB_BASE" "report_full_naive_pct100" benchmark_full_naive_pct100.log
mv "$NAIVE_DB_BASE/LVMT_4e7" "$NAIVE_DB_SRC" && rmdir "$NAIVE_DB_BASE"

# 切回原分支后重新编译，避免 target/ 里留着朴素实现的二进制
git checkout -q "$ORIG_BRANCH"
cargo build --release --bin benchmark_lvmt -j "${BUILD_JOBS:-8}" > /dev/null 2>&1

echo ">>> All done:"
for d in report_t30_pct100 report_sysalloc_pct100 report_full_naive_pct100; do
    echo "== $d"; cat "$d/timing.log"
done
