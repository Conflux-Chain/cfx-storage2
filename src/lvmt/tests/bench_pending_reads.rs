//! Read latency vs pending depth, with the key-value CurrentMap at different commits.
//! cargo test -r --lib bench_pending_reads -- --ignored --nocapture
use std::time::Instant;

use super::*;

#[test]
#[ignore]
fn bench_pending_reads() {
    let historical_db = Arc::new(WrappedInMemoryDb::empty());
    let pending_db = Arc::new(WrappedInMemoryDb::empty());
    let mut db = LvmtStorage::<
        WrappedInMemoryDb<HistoricalTableName>,
        WrappedInMemoryDb<PendingTableName>,
    >::new(historical_db, pending_db)
    .unwrap();
    let mut rng = get_rng_for_test();
    let mut prev = HashSet::new();
    let mut chain: Vec<CommitID> = vec![];
    let mut next_key = 1_000_000u64;
    let hot = u64_to_boxed_u8(1);
    let n_reads = 20_000u64;
    for target in [1000usize, 2000, 4000, 6000] {
        while chain.len() < target {
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
        let older = chain[chain.len() - 6];
        let time = |db: &LvmtStorage<_, _>, at: CommitID, key: &dyn Fn(u64) -> Box<[u8]>| {
            let r = db.as_reader().unwrap();
            let s = Instant::now();
            for i in 0..n_reads {
                std::hint::black_box(r.get(at, key(i)).unwrap());
            }
            s.elapsed().as_secs_f64() * 1e6 / n_reads as f64
        };
        let missing = |i: u64| u64_to_boxed_u8(u64::MAX - i);
        let hotk = |_| u64_to_boxed_u8(1);
        let early = |i: u64| u64_to_boxed_u8(1_000_000 + i % 1000);

        // current = parent of tip (state right after commit, no checkout)
        let a = time(&db, tip, &missing);
        db.as_reader().unwrap().checkout_current(tip).unwrap();
        let b = time(&db, tip, &missing);
        let c = time(&db, tip, &hotk);
        let d = time(&db, tip, &early);
        // current = tip, read at an ancestor 5 commits back
        let e = time(&db, older, &missing);
        let f = time(&db, older, &early);
        eprintln!(
            "depth {target}: cur=parent read=tip miss {a:.2}µs | cur=tip read=tip miss {b:.2} hot {c:.2} early {d:.2} | cur=tip read=tip-5 miss {e:.2} early {f:.2}"
        );
    }
}
