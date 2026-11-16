//! Cost of a confirmation (`confirmed_pending_to_history_with_height`) and of the
//! cleanup after it, for a fixed number of confirmed commits and a varying number
//! of commits left pending. Not run by default:
//!
//! ```text
//! BENCH_DIR=/some/dir cargo test -r --lib bench_migration -- --ignored --nocapture
//! ```
use std::time::Instant;

use super::*;

#[test]
#[ignore]
fn bench_migration() {
    let dir = std::env::var("BENCH_DIR").expect("set BENCH_DIR");
    let confirm = 200u64;
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
        // Three confirmations of `confirm` commits each, with `remaining` commits
        // left pending after each; the first one is a warm-up.
        for round in 1..=3u64 {
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
            let start = Instant::now();
            db.confirmed_pending_to_history_with_height(round * confirm, tip)
                .unwrap();
            let migrate = start.elapsed().as_secs_f64() * 1e3;
            let start = Instant::now();
            db.background_cleanup().unwrap();
            let cleanup = start.elapsed().as_secs_f64() * 1e3;
            if round > 1 {
                timings.push((migrate, cleanup));
            }
        }
        let s = timings
            .iter()
            .map(|(m, c)| format!("migrate {m:.0} ms, cleanup {c:.0} ms"))
            .collect::<Vec<_>>()
            .join(" | ");
        eprintln!("confirm {confirm}, remaining {remaining}: {s}");
    }
}
