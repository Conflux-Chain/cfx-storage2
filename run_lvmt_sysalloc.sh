#!/bin/bash
# 分配器对照实验：与 run_lvmt.sh 完全相同的负载、内存限制和输入数据库，
# 唯一区别是以 --features system-alloc 编译，benchmark_lvmt 改用系统默认分配器。
# 复用默认 target 目录（依赖不重编），编译后把二进制另存为 benchmark_lvmt_sysalloc；
# run_lvmt.sh 用 cargo run，会按默认特性重新链接回 jemalloc 版本，互不影响。日志和 pprof 报告单独存放。
# 注意：与 run_lvmt.sh 共用默认数据库目录（运行开始时会被清空后从 database 复制），两者不能同时运行。

echo ">>> Checking prerequisites..."
bash ensure_params.sh
if [ $? -ne 0 ]; then
    echo "[x] Prerequisites check failed. Exiting test."
    exit 1
fi

# 先在 cgroup 外编译，避免编译过程占用 8 GiB 限额；并行度降到 8，与 run_index_comparison_full.sh 一致（默认 32 时 gcc 编 jemalloc 曾崩溃）
echo ">>> Building benchmark_lvmt with system allocator..."
cargo build --release --bin benchmark_lvmt --features system-alloc -j "${BUILD_JOBS:-8}" > build_sysalloc.log 2>&1
if [ $? -ne 0 ]; then
    echo "[x] Build failed, see build_sysalloc.log"
    exit 1
fi
cp target/release/benchmark_lvmt target/release/benchmark_lvmt_sysalloc

echo ">>> Starting the test..."
echo $$ | sudo tee /sys/fs/cgroup/lvmt/cgroup.procs > /dev/null

COMMAND="./target/release/benchmark_lvmt_sysalloc --no-stat -k 40m -a lvmt --max-epoch 500 --epoch-size 10000 --report-epoch 250 --pprof-report-to report_sysalloc --warmup-from database > benchmark_sysalloc.log 2>&1"
echo "Executing command in the 'lvmt' cgroup: $COMMAND"
bash -c "$COMMAND"
