//! Garbage collection of the pending persistence layer running without any borrow
//! of the storage, concurrently with commits and confirmations.

use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    thread,
};

use parking_lot::RwLock;

use super::*;

#[test]
fn test_cleanup_concurrent_with_writes_then_recover() {
    let historical_db = Arc::new(WrappedInMemoryDb::<HistoricalTableName>::empty());
    let pending_db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
    let db = RwLock::new(LvmtStorage::new(historical_db.clone(), pending_db.clone()).unwrap());

    let num_commits = 150u64;
    let num_keys = 40u64;
    let done = AtomicBool::new(false);
    let cleanups = AtomicUsize::new(0);
    let mut chain: Vec<CommitID> = vec![];
    let mut rng = get_rng_for_test();
    let mut previous = HashSet::new();

    thread::scope(|s| {
        s.spawn(|| {
            while !done.load(Ordering::Acquire) {
                // Take the task under the lock, run it outside.
                let cleanup = db.read().pending_cleanup().unwrap();
                match cleanup {
                    Some(cleanup) => {
                        cleanup.run().unwrap();
                        cleanups.fetch_add(1, Ordering::Relaxed);
                    }
                    None => thread::yield_now(),
                }
            }
        });

        for i in 0..num_commits {
            let changes: Vec<(Box<[u8]>, Option<Box<[u8]>>)> = (0..5)
                .map(|j| {
                    let key = (i * 5 + j) % num_keys;
                    let value = (i % 7 != 0).then(|| u64_to_boxed_u8(i * 100 + j));
                    (u64_to_boxed_u8(key), value)
                })
                .collect();
            let id = gen_novel_commit_id(&mut rng, &mut previous);
            let mut guard = db.write();
            guard
                .as_manager()
                .unwrap()
                .commit(chain.last().copied(), id, changes.into_iter(), &AMT)
                .unwrap();
            chain.push(id);
            // Keep about 15 commits pending, confirming 5 at a time.
            if i >= 20 && i % 5 == 0 {
                guard
                    .confirmed_pending_to_history_with_height(i - 15, id)
                    .unwrap();
            }
        }
        done.store(true, Ordering::Release);
    });
    assert!(cleanups.load(Ordering::Relaxed) > 0);

    let db = db.into_inner();
    db.background_cleanup().unwrap();
    let read_all = |db: &LvmtStorage<_, _>, commit: CommitID| {
        let reader = db.as_reader().unwrap();
        (0..num_keys)
            .map(|k| reader.get(commit, u64_to_boxed_u8(k)).unwrap())
            .collect::<Vec<_>>()
    };
    let tip = *chain.last().unwrap();
    let probes = [tip, chain[chain.len() - 8], chain[chain.len() - 40]];
    let expected: Vec<_> = probes.iter().map(|&c| read_all(&db, c)).collect();
    let roots: Vec<_> = chain[chain.len() - 10..]
        .iter()
        .map(|&c| db.as_reader().unwrap().get_state_root(c).unwrap())
        .collect();
    drop(db);

    // Recovery from what the cleanups left.
    let db = LvmtStorage::new(historical_db, pending_db).unwrap();
    for (&commit, expected) in probes.iter().zip(&expected) {
        assert_eq!(&read_all(&db, commit), expected);
    }
    for (&commit, root) in chain[chain.len() - 10..].iter().zip(&roots) {
        assert_eq!(
            &db.as_reader().unwrap().get_state_root(commit).unwrap(),
            root
        );
    }
    // And it keeps working.
    let id = gen_novel_commit_id(&mut rng, &mut previous);
    let mut db = db;
    db.as_manager()
        .unwrap()
        .commit(
            Some(tip),
            id,
            std::iter::once((u64_to_boxed_u8(0), None)),
            &AMT,
        )
        .unwrap();
}
