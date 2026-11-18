//! Cost of a confirmation (`confirmed_pending_to_history_with_height_deferring_snapshot`),
//! the part done with the storage borrowed, against the number of confirmed commits
//! and the number of keys each of them modifies, with the number of commits left
//! pending fixed. Pairs of cases confirm the same number of modified keys over
//! different numbers of commits. Not run by default:
//!
//! ```text
//! BENCH_DIR=/some/dir cargo test -r --lib bench_migration_keys -- --ignored --nocapture
//! ```
//!
//! With `BENCH_LARGE=1`, also runs a case of 2000 confirmed commits modifying 400
//! keys each, with 4400 left pending.
use std::time::Instant;

use super::*;

#[test]
#[ignore]
fn bench_migration_keys() {
    let dir = std::env::var("BENCH_DIR").expect("set BENCH_DIR");
    // (confirmed commits, keys modified per commit, commits left pending)
    let mut cases = vec![
        (200u64, 28u64, 2000u64),
        (200, 112, 2000),
        (200, 448, 2000),
        (800, 28, 2000),
        (3200, 28, 2000),
        // Close to the downstream setting: S = 2000, about 40 keys per block.
        (2000, 40, 4400),
    ];
    if std::env::var("BENCH_LARGE").is_ok() {
        cases.push((2000, 400, 4400));
    }
    // One warm-up confirmation, then the measured ones. Too few to make a pending
    // snapshot due, which would be written after the borrow anyway.
    let rounds = 4u64;
    for (confirm, keys, remaining) in cases {
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
        let mut committing = 0.0;
        for round in 1..=rounds {
            let start = Instant::now();
            while (chain.len() as u64) < round * confirm + remaining {
                // One key modified by every commit, the others new.
                let mut changes: Vec<(Box<[u8]>, Option<Box<[u8]>>)> =
                    vec![(hot.clone(), Some(u64_to_boxed_u8(chain.len() as u64)))];
                for _ in 1..keys {
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
            committing += start.elapsed().as_secs_f64();
            let tip = *chain.last().unwrap();
            let start = Instant::now();
            let snapshot = db
                .confirmed_pending_to_history_with_height_deferring_snapshot(round * confirm, tip)
                .unwrap();
            let migrate = start.elapsed().as_secs_f64() * 1e3;
            assert!(snapshot.is_none());
            db.background_cleanup().unwrap();
            if round > 1 {
                timings.push(migrate);
            }
        }
        let mean = timings.iter().sum::<f64>() / timings.len() as f64;
        let max = timings.iter().cloned().fold(0.0, f64::max);
        eprintln!(
            "confirm {confirm} x {keys} keys = {} keys, remaining {remaining}, {} confirmations: \
             migrate mean {mean:.0} ms, max {max:.0} ms (commits took {committing:.0} s)",
            confirm * keys,
            timings.len()
        );
    }
}
