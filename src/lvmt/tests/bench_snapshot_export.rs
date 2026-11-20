//! The cost of exporting the snapshot of a commit `N` (`prepare_snapshot` and
//! serving every chunk) on the workload of `bench_snapshot_anchor_volume`.
//!
//! Not run by default:
//!
//! ```text
//! BENCH_DIR=/some/dir BENCH_KEYS=40000000 BENCH_LOAD_BATCH=50000 \
//!     cargo test -r --lib bench_snapshot_export -- --ignored --nocapture
//! ```
//!
//! Knobs (env): `BENCH_KEYS` (default 1_000_000) loaded in commits of
//! `BENCH_LOAD_BATCH` (50_000), then `BENCH_COMMITS` (2300) commits of
//! `BENCH_WRITES` (1500) writes, `BENCH_NEW_PERCENT` (10) of them new keys;
//! every 100 commits are confirmed, keeping the last 10 pending. `N` is the
//! latest historical commit. `BENCH_BUDGET` is `max_chunk_bytes` (262144).
//! The database is kept; `BENCH_REUSE` opens it instead of building it.

use std::time::Instant;

use rand::{rngs::StdRng, Rng, SeedableRng};

use super::*;
use crate::lvmt::snapshot_sync::{ChunkId, Manifest};

fn env_or(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |v| v.parse().unwrap())
}

fn mb(bytes: u64) -> String {
    match bytes {
        b if b >= 1_000_000_000 => format!("{:.2} GB", b as f64 / 1e9),
        b if b >= 1_000_000 => format!("{:.1} MB", b as f64 / 1e6),
        b => format!("{:.1} KB", b as f64 / 1e3),
    }
}

/// Peak resident memory of this process, from `/proc/self/status`.
fn peak_rss() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let line = status.lines().find(|l| l.starts_with("VmHWM:")).unwrap();
    line.split_whitespace().nth(1).unwrap().parse::<u64>().unwrap() * 1024
}

fn reset_peak_rss() {
    std::fs::write("/proc/self/clear_refs", "5").unwrap();
}

fn report_sizes(name: &str, mut sizes: Vec<u64>, budget: u64) {
    if sizes.is_empty() {
        eprintln!("  {name}: none");
        return;
    }
    sizes.sort();
    let total: u64 = sizes.iter().sum();
    let pct = |p: usize| sizes[(sizes.len() - 1) * p / 100];
    eprintln!(
        "  {name}: {} chunks, {} total; min {} / p50 {} / p99 {} / max {}; over budget: {}",
        sizes.len(),
        mb(total),
        mb(sizes[0]),
        mb(pct(50)),
        mb(pct(99)),
        mb(*sizes.last().unwrap()),
        sizes.iter().filter(|s| **s > budget).count()
    );
}

#[test]
#[ignore]
fn bench_snapshot_export() {
    let dir = std::env::var("BENCH_DIR").expect("set BENCH_DIR");
    let keys = env_or("BENCH_KEYS", 1_000_000) as u64;
    let load_batch = env_or("BENCH_LOAD_BATCH", 50_000) as u64;
    let writes = env_or("BENCH_WRITES", 1500);
    let commits = env_or("BENCH_COMMITS", 2300);
    let new_percent = env_or("BENCH_NEW_PERCENT", 10);
    let budget = env_or("BENCH_BUDGET", 256 * 1024);
    let reuse = std::env::var("BENCH_REUSE").is_ok();

    let historical_path = format!("{dir}/historical");
    let pending_path = format!("{dir}/pending");
    if !reuse {
        clear_dir_then_create(&historical_path);
        clear_dir_then_create(&pending_path);
    }
    let mut db =
        LvmtStorage::<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>::new(
            Arc::new(WrappedRocksDb::open(&historical_path).unwrap()),
            Arc::new(WrappedRocksDb::open(&pending_path).unwrap()),
        )
        .unwrap();

    if !reuse {
        let mut rng = get_rng_for_test();
        let mut previous_commits = HashSet::new();
        let mut chain: Vec<CommitID> = Vec::new();
        let mut next_key = 0u64;
        let build_start = Instant::now();
        let mut commit = |db: &mut LvmtStorage<_, _>,
                          chain: &mut Vec<CommitID>,
                          changes: Vec<(Box<[u8]>, Option<Box<[u8]>>)>| {
            let id = gen_novel_commit_id(&mut rng, &mut previous_commits);
            let mut lvmt = db.as_manager().unwrap();
            lvmt.commit(chain.last().copied(), id, changes.into_iter(), &AMT)
                .unwrap();
            chain.push(id);
        };
        while next_key < keys {
            let end = (next_key + load_batch).min(keys);
            let changes = (next_key..end)
                .map(|k| (u64_to_boxed_u8(k), Some(u64_to_boxed_u8(k))))
                .collect();
            commit(&mut db, &mut chain, changes);
            next_key = end;
        }
        eprintln!(
            "initial load: {keys} keys in {} commits, {:.1} s",
            chain.len(),
            build_start.elapsed().as_secs_f64()
        );
        let mut value_rng = StdRng::seed_from_u64(7);
        for i in 0..commits {
            let changes = (0..writes)
                .map(|_| {
                    let k = if value_rng.gen_range(0..100) < new_percent {
                        next_key += 1;
                        next_key - 1
                    } else {
                        value_rng.gen_range(0..next_key)
                    };
                    (u64_to_boxed_u8(k), Some(u64_to_boxed_u8(value_rng.gen())))
                })
                .collect();
            commit(&mut db, &mut chain, changes);
            if (i + 1) % 100 == 0 {
                let confirm = chain[chain.len() - 10];
                db.confirmed_pending_to_history_with_commit_id(confirm)
                    .unwrap();
            }
        }
        eprintln!(
            "build: {} commits, {next_key} keys, {:.1} s",
            chain.len(),
            build_start.elapsed().as_secs_f64()
        );
    }

    let reader = db.as_reader().unwrap();
    let n = reader
        .key_value_reader()
        .get_latest_historical_commit()
        .expect("a historical commit");
    eprintln!(
        "\nN = latest historical commit, height {}; budget {}",
        reader.commit_height(n).unwrap(),
        mb(budget as u64)
    );

    reset_peak_rss();
    let rss_before = peak_rss();
    let t = Instant::now();
    let export = reader.prepare_snapshot(n, budget).unwrap();
    eprintln!(
        "prepare_snapshot: {:.1} s, peak RSS {} (from {} before)",
        t.elapsed().as_secs_f64(),
        mb(peak_rss()),
        mb(rss_before)
    );
    let manifest: &Manifest = export.manifest();
    let manifest_bytes = export.manifest_bytes(usize::MAX).unwrap();
    eprintln!(
        "manifest: {} ({} anchor commits, {} MMR nodes, {} chunk ids)",
        mb(manifest_bytes.len() as u64),
        manifest.anchor_commits.len(),
        manifest.mmr_nodes.len(),
        manifest.chunks.len()
    );

    let t = Instant::now();
    let (mut anchor_sizes, mut data_sizes) = (vec![], vec![]);
    let (mut anchor_time, mut data_time) = (0f64, 0f64);
    for (id, id_bytes) in manifest.chunks.iter().zip(manifest.chunk_id_bytes()) {
        let tc = Instant::now();
        let chunk = export.load_chunk(&reader, &id_bytes, 2 * budget).unwrap();
        let elapsed = tc.elapsed().as_secs_f64();
        match id {
            ChunkId::Anchor { .. } => {
                anchor_sizes.push(chunk.len() as u64);
                anchor_time += elapsed;
            }
            ChunkId::Data { .. } => {
                data_sizes.push(chunk.len() as u64);
                data_time += elapsed;
            }
        }
    }
    eprintln!(
        "load every chunk: {:.1} s (anchor {:.1} s, data {:.1} s), peak RSS {}",
        t.elapsed().as_secs_f64(),
        anchor_time,
        data_time,
        mb(peak_rss())
    );
    report_sizes("anchor", anchor_sizes, budget as u64);
    report_sizes("data", data_sizes, budget as u64);
}
