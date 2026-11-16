//! Where the time of a pending snapshot goes: copying each pending tree out of
//! memory, encoding the copy into a write batch, and committing the batch to
//! RocksDB. Decides which of these could leave the lock held during a root change.
//! The pending part is built as in `bench_migration`, and the batch is committed
//! to a separate database so that the storage itself is not changed. Not run by
//! default:
//!
//! ```text
//! BENCH_DIR=/some/dir cargo test -r --lib bench_snapshot_phases -- --ignored --nocapture
//! ```
use std::time::Duration;

use super::*;

#[test]
#[ignore]
fn bench_snapshot_phases() {
    let dir = std::env::var("BENCH_DIR").expect("set BENCH_DIR");
    let confirm = 200u64;
    let repeats = 5;
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
        while (chain.len() as u64) < confirm + remaining {
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
        db.confirmed_pending_to_history_with_height(confirm, *chain.last().unwrap())
            .unwrap();

        // One row per pending tree: summed export / encode / commit, and records.
        let mut rows = [("key-value", [Duration::ZERO; 3], 0usize); 3];
        rows[1].0 = "AMT nodes";
        rows[2].0 = "slot allocations";
        for _ in 0..repeats {
            let scratch_path = format!("{dir}/scratch");
            clear_dir_then_create(&scratch_path);
            let scratch = WrappedRocksDb::<PendingTableName>::open(&scratch_path).unwrap();
            let timings = [
                db.key_value_cache.time_snapshot(&scratch).unwrap(),
                db.amt_node_cache.time_snapshot(&scratch).unwrap(),
                db.slot_alloc_cache.time_snapshot(&scratch).unwrap(),
            ];
            for (row, t) in rows.iter_mut().zip(timings) {
                row.1[0] += t.export;
                row.1[1] += t.encode;
                row.1[2] += t.commit;
                row.2 = t.records;
            }
        }
        let ms = |d: Duration| d.as_secs_f64() * 1e3 / repeats as f64;
        eprintln!("remaining {remaining}, mean of {repeats}:");
        let mut total = [0.0; 3];
        for (name, d, records) in rows {
            eprintln!(
                "  {name}: {records} records | export {:.1} ms, encode {:.1} ms, commit {:.1} ms",
                ms(d[0]),
                ms(d[1]),
                ms(d[2])
            );
            for i in 0..3 {
                total[i] += ms(d[i]);
            }
        }
        eprintln!(
            "  total: export {:.1} ms, encode {:.1} ms, commit {:.1} ms",
            total[0], total[1], total[2]
        );
    }
}
