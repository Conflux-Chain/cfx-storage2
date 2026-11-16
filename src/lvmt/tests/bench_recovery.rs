//! Cost of a restart: opening the databases and recovering the pending part, which
//! reads the latest pending snapshot and replays the WAL logged since. Restarts
//! after every confirmation, so from right after a snapshot to the last
//! confirmation before the next one. Not run by default:
//!
//! ```text
//! BENCH_DIR=/some/dir cargo test -r --lib bench_recovery -- --ignored --nocapture
//! ```
//!
//! `BENCH_CONFIRM` and `BENCH_REMAINING` (default 200 and 4000) set the commits
//! confirmed each time and left pending after.
use std::time::Instant;

use crate::middlewares::CHANGE_ROOTS_PER_SNAPSHOT;

use super::*;

type Storage = LvmtStorage<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>;

#[test]
#[ignore]
fn bench_recovery() {
    let dir = std::env::var("BENCH_DIR").expect("set BENCH_DIR");
    let env = |name: &str, default: u64| {
        std::env::var(name).map_or(default, |v| v.parse().expect("a number"))
    };
    let confirm = env("BENCH_CONFIRM", 200);
    let remaining = env("BENCH_REMAINING", 4000);
    let historical_path = format!("{dir}/historical");
    let pending_path = format!("{dir}/pending");
    clear_dir_then_create(&historical_path);
    clear_dir_then_create(&pending_path);
    let open = || {
        Storage::new(
            Arc::new(WrappedRocksDb::open(&historical_path).unwrap()),
            Arc::new(WrappedRocksDb::open(&pending_path).unwrap()),
        )
        .unwrap()
    };
    let mut db = open();

    let mut rng = get_rng_for_test();
    let mut prev = HashSet::new();
    let mut chain: Vec<CommitID> = vec![];
    let mut next_key = 1_000_000u64;
    let hot = u64_to_boxed_u8(1);
    eprintln!("confirm {confirm}, remaining {remaining}:");
    // Two snapshots: the first after N confirmations, then restarts at every
    // number of confirmations since it.
    for round in 1..=2 * CHANGE_ROOTS_PER_SNAPSHOT {
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
        db.confirmed_pending_to_history_with_height(round * confirm, tip)
            .unwrap();
        db.background_cleanup().unwrap();
        if round < CHANGE_ROOTS_PER_SNAPSHOT {
            continue;
        }
        let root = db.as_reader().unwrap().get_state_root(tip).unwrap();
        drop(db);

        let start = Instant::now();
        let historical_db = Arc::new(WrappedRocksDb::open(&historical_path).unwrap());
        let pending_db = Arc::new(WrappedRocksDb::open(&pending_path).unwrap());
        let open_ms = start.elapsed().as_secs_f64() * 1e3;
        let start = Instant::now();
        db = Storage::new(historical_db, pending_db).unwrap();
        let recover_ms = start.elapsed().as_secs_f64() * 1e3;
        // Recovery falls back to an empty pending part if it fails.
        assert_eq!(db.as_reader().unwrap().get_state_root(tip).unwrap(), root);
        let since = round % CHANGE_ROOTS_PER_SNAPSHOT;
        eprintln!(
            "  {since} confirmations after the snapshot ({} commits logged since it): \
             open the dbs {open_ms:.0} ms, recover {recover_ms:.0} ms",
            since * confirm
        );
    }
}
