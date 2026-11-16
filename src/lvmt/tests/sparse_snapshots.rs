//! Recovery with snapshots written only once every `CHANGE_ROOTS_PER_SNAPSHOT`
//! confirmations: restarting after every confirmation, after an incomplete one,
//! and after a cleanup. And with those snapshots written after the confirmation,
//! late, or never.

use crate::{
    lvmt::{table_schema::FlatKeyValue, PendingSnapshot},
    middlewares::{snapshot_heights, PendingKeyValueConfig, CHANGE_ROOTS_PER_SNAPSHOT},
};

use super::*;

type KeyValueSchema = PendingKeyValueConfig<FlatKeyValue, CommitID>;

type Storage =
    LvmtStorage<WrappedInMemoryDb<HistoricalTableName>, WrappedInMemoryDb<PendingTableName>>;

const NUM_KEYS: u64 = 30;

fn read_all(db: &Storage, commit: CommitID) -> Vec<Option<LvmtValue>> {
    let reader = db.as_reader().unwrap();
    (0..NUM_KEYS)
        .map(|k| reader.get(commit, u64_to_boxed_u8(k)).unwrap())
        .collect()
}

#[test]
fn test_recover_around_sparse_snapshots() {
    let historical_db = Arc::new(WrappedInMemoryDb::<HistoricalTableName>::empty());
    let pending_db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
    let reopen = || Storage::new(historical_db.clone(), pending_db.clone()).unwrap();
    let mut db = reopen();

    let mut rng = get_rng_for_test();
    let mut previous = HashSet::new();
    let mut chain: Vec<CommitID> = vec![];
    // Reads at the latest few commits, which stay pending.
    let check = |db: &Storage, chain: &[CommitID], expected: &[Vec<Option<LvmtValue>>]| {
        for (commit, expected) in chain[chain.len() - expected.len()..].iter().zip(expected) {
            assert_eq!(&read_all(db, *commit), expected);
        }
    };

    for round in 0..3 * CHANGE_ROOTS_PER_SNAPSHOT + 2 {
        // 6 commits in the first round, so that every round confirms 3 commits.
        for _ in 0..if round == 0 { 6 } else { 3 } {
            let i = chain.len() as u64;
            let changes: Vec<(Box<[u8]>, Option<Box<[u8]>>)> = (0..4)
                .map(|j| {
                    let value = ((i + j) % 5 != 0).then(|| u64_to_boxed_u8(i * 10 + j));
                    (u64_to_boxed_u8((i * 4 + j) % NUM_KEYS), value)
                })
                .collect();
            let id = gen_novel_commit_id(&mut rng, &mut previous);
            db.as_manager()
                .unwrap()
                .commit(chain.last().copied(), id, changes.into_iter(), &AMT)
                .unwrap();
            chain.push(id);
        }
        let expected: Vec<_> = chain[chain.len() - 3..]
            .iter()
            .map(|&c| read_all(&db, c))
            .collect();
        // Confirm all but the last 3 commits.
        let new_root_height = chain.len() as u64 - 3;
        let new_root = chain[new_root_height as usize];

        // A crash after the pending db is written but not the historical db: the
        // confirmation is rolled back.
        db.make_pending_db_ahead_for_test(new_root).unwrap();
        drop(db);
        db = reopen();
        check(&db, &chain, &expected);
        assert!(
            db.as_reader()
                .unwrap()
                .is_newer_than_pending_root(new_root_height),
            "round {round}: the incomplete confirmation is rolled back"
        );

        db.confirmed_pending_to_history_with_height(new_root_height, *chain.last().unwrap())
            .unwrap();
        drop(db);
        db = reopen();
        check(&db, &chain, &expected);
        assert!(
            !db.as_reader()
                .unwrap()
                .is_newer_than_pending_root(new_root_height),
            "round {round}: the confirmation is kept"
        );

        db.background_cleanup().unwrap();
        drop(db);
        db = reopen();
        check(&db, &chain, &expected);
    }
}

/// What to do with the snapshot task of a confirmation.
#[derive(Debug, Clone, Copy)]
enum Deferred {
    /// Run it right after the confirmation.
    Run,
    /// Never run it, and restart right after the confirmation.
    DropAndRestart,
    /// Never run it, and restart only after the next commits, which are logged under
    /// the snapshot ID it was due for.
    Drop,
    /// Hold it until the snapshot task of a later confirmation, and run it after
    /// that one, so that it does nothing.
    Late,
}

#[test]
fn test_recover_around_deferred_snapshots() {
    for policy in [
        Deferred::Run,
        Deferred::DropAndRestart,
        Deferred::Drop,
        Deferred::Late,
    ] {
        let historical_db = Arc::new(WrappedInMemoryDb::<HistoricalTableName>::empty());
        let pending_db = Arc::new(WrappedInMemoryDb::<PendingTableName>::empty());
        let reopen = || Storage::new(historical_db.clone(), pending_db.clone()).unwrap();
        let mut db = reopen();

        let mut rng = get_rng_for_test();
        let mut previous = HashSet::new();
        let mut chain: Vec<CommitID> = vec![];
        let check = |db: &Storage, chain: &[CommitID], expected: &[Vec<Option<LvmtValue>>]| {
            for (commit, expected) in chain[chain.len() - expected.len()..].iter().zip(expected) {
                assert_eq!(&read_all(db, *commit), expected, "{policy:?}");
            }
        };
        let mut held: Option<(PendingSnapshot<_>, u64)> = None;
        let mut snapshots_due = 0;

        for round in 0..3 * CHANGE_ROOTS_PER_SNAPSHOT + 2 {
            for _ in 0..if round == 0 { 6 } else { 3 } {
                let i = chain.len() as u64;
                let changes: Vec<(Box<[u8]>, Option<Box<[u8]>>)> = (0..4)
                    .map(|j| {
                        let value = ((i + j) % 5 != 0).then(|| u64_to_boxed_u8(i * 10 + j));
                        (u64_to_boxed_u8((i * 4 + j) % NUM_KEYS), value)
                    })
                    .collect();
                let id = gen_novel_commit_id(&mut rng, &mut previous);
                db.as_manager()
                    .unwrap()
                    .commit(chain.last().copied(), id, changes.into_iter(), &AMT)
                    .unwrap();
                chain.push(id);
            }
            let expected: Vec<_> = chain[chain.len() - 3..]
                .iter()
                .map(|&c| read_all(&db, c))
                .collect();
            let new_root_height = chain.len() as u64 - 3;
            let new_root = chain[new_root_height as usize];

            // A crash before the historical db is written, and so before any snapshot
            // is: the confirmation is rolled back. Not while a snapshot task is held,
            // which a crash would drop.
            if held.is_none() {
                db.make_pending_db_ahead_for_test(new_root).unwrap();
                drop(db);
                db = reopen();
                check(&db, &chain, &expected);
                assert!(
                    db.as_reader()
                        .unwrap()
                        .is_newer_than_pending_root(new_root_height),
                    "{policy:?}, round {round}: the incomplete confirmation is rolled back"
                );
            }

            let snapshot = db
                .confirmed_pending_to_history_with_height_deferring_snapshot(
                    new_root_height,
                    *chain.last().unwrap(),
                )
                .unwrap();
            snapshots_due += snapshot.is_some() as u64;
            match policy {
                Deferred::Run => {
                    if let Some(snapshot) = snapshot {
                        snapshot.run().unwrap();
                    }
                }
                Deferred::DropAndRestart => drop(snapshot),
                Deferred::Drop => {
                    if snapshot.is_some() {
                        // Restarted at the start of the next round.
                        db.background_cleanup().unwrap();
                        check(&db, &chain, &expected);
                        continue;
                    }
                }
                Deferred::Late => match (snapshot, held.take()) {
                    (Some(snapshot), Some((older, older_height))) => {
                        snapshot.run().unwrap();
                        older.run().unwrap();
                        let heights = snapshot_heights::<KeyValueSchema, _>(&pending_db).unwrap();
                        assert!(heights.contains(&new_root_height), "{heights:?}");
                        assert!(!heights.contains(&older_height), "{heights:?}");
                    }
                    (snapshot, older) => held = snapshot.map(|s| (s, new_root_height)).or(older),
                },
            }
            if held.is_some() {
                // Not restarted, which would drop the task.
                db.background_cleanup().unwrap();
                check(&db, &chain, &expected);
                continue;
            }
            // Committed by the confirmation, whether or not its snapshot is written.
            drop(db);
            db = reopen();
            check(&db, &chain, &expected);
            assert!(
                !db.as_reader()
                    .unwrap()
                    .is_newer_than_pending_root(new_root_height),
                "{policy:?}, round {round}: the confirmation is kept"
            );

            db.background_cleanup().unwrap();
            drop(db);
            db = reopen();
            check(&db, &chain, &expected);
        }
        let rounds = 3 * CHANGE_ROOTS_PER_SNAPSHOT + 2;
        let expected_due = match policy {
            Deferred::Run | Deferred::Drop | Deferred::Late => rounds / CHANGE_ROOTS_PER_SNAPSHOT,
            // Restarted before anything is logged after the confirmation, recovery
            // cannot tell the new snapshot ID was moved on to, so every confirmation
            // from the first one due is due again, until a snapshot is written.
            Deferred::DropAndRestart => rounds - (CHANGE_ROOTS_PER_SNAPSHOT - 1),
        };
        assert_eq!(snapshots_due, expected_due, "{policy:?}");
        // The cleanup leaves the latest snapshot at or below the durable height, and
        // the ones above it.
        let heights = snapshot_heights::<KeyValueSchema, _>(&pending_db).unwrap();
        assert!(heights.len() <= 2, "{policy:?}: {heights:?}");
    }
}
