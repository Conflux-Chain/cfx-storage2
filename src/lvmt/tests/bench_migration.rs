//! Cost of a confirmation (`confirmed_pending_to_history_with_height_deferring_snapshot`),
//! of the pending snapshot it defers, and of the cleanup after it, for a fixed
//! number of confirmed commits and a varying number of commits left pending, over
//! enough confirmations to cover two snapshots of the pending part. Not run by
//! default:
//!
//! ```text
//! BENCH_DIR=/some/dir cargo test -r --lib bench_migration -- --ignored --nocapture
//! ```
use std::time::Instant;

use crate::middlewares::CHANGE_ROOTS_PER_SNAPSHOT;

use super::*;

#[test]
#[ignore]
fn bench_migration() {
    let dir = std::env::var("BENCH_DIR").expect("set BENCH_DIR");
    let confirm = 200u64;
    // Enough confirmations to cover two snapshots.
    let rounds = 2 * CHANGE_ROOTS_PER_SNAPSHOT + 2;
    for remaining in [200u64, 1000, 2000, 4000] {
        let historical_path = format!("{dir}/historical");
        let pending_path = format!("{dir}/pending");
        clear_dir_then_create(&historical_path);
        clear_dir_then_create(&pending_path);
        let mut db = LvmtStorage::<
            WrappedRocksDb<HistoricalTableName>,
            WrappedRocksDb<PendingTableName>,
        >::new(
            Arc::new(WrappedRocksDb::open(&historical_path).unwrap()),
            Arc::new(WrappedRocksDb::open(&pending_path).unwrap()),
        )
        .unwrap();
        let mut rng = get_rng_for_test();
        let mut prev = HashSet::new();
        let mut chain: Vec<CommitID> = vec![];
        let mut next_key = 1_000_000u64;
        let hot = u64_to_boxed_u8(1);
        let mut timings = vec![];
        // `rounds` confirmations of `confirm` commits each, with `remaining` commits
        // left pending after each; the first one is a warm-up.
        for round in 1..=rounds {
            while (chain.len() as u64) < round * confirm + remaining {
                let mut changes: Vec<(Box<[u8]>, Option<Box<[u8]>>)> =
                    vec![(hot.clone(), Some(u64_to_boxed_u8(chain.len() as u64)))];
                for _ in 0..27 {
                    changes.push((u64_to_boxed_u8(next_key), Some(u64_to_boxed_u8(next_key))));
                    next_key += 1;
                }
                let id = gen_novel_commit_id(&mut rng, &mut prev);
                db.as_manager()
                    .unwrap()
                    .commit(chain.last().copied(), id, changes.into_iter(), &AMT)
                    .unwrap();
                chain.push(id);
            }
            let tip = *chain.last().unwrap();
            // The confirmation with the snapshot due deferred, which is what needs
            // the storage borrowed, then that snapshot.
            let start = Instant::now();
            let snapshot = db
                .confirmed_pending_to_history_with_height_deferring_snapshot(round * confirm, tip)
                .unwrap();
            let migrate = start.elapsed().as_secs_f64() * 1e3;
            let start = Instant::now();
            if let Some(snapshot) = snapshot {
                snapshot.run().unwrap();
            }
            let snapshot = start.elapsed().as_secs_f64() * 1e3;
            let start = Instant::now();
            db.background_cleanup().unwrap();
            let cleanup = start.elapsed().as_secs_f64() * 1e3;
            if round > 1 {
                timings.push((migrate, snapshot, cleanup));
            }
        }
        let stats = |f: fn(&(f64, f64, f64)) -> f64| {
            let values: Vec<f64> = timings.iter().map(f).collect();
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let max = values.iter().cloned().fold(0.0, f64::max);
            format!("mean {mean:.0} ms, max {max:.0} ms")
        };
        let s = format!(
            "migrate {} | snapshot {} | cleanup {}",
            stats(|t| t.0),
            stats(|t| t.1),
            stats(|t| t.2)
        );
        eprintln!(
            "confirm {confirm}, remaining {remaining}, {} confirmations: {s}",
            timings.len()
        );
    }
}
