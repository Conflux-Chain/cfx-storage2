//! Recovery with snapshots written only once every `CHANGE_ROOTS_PER_SNAPSHOT`
//! confirmations: restarting after every confirmation, after an incomplete one,
//! and after a cleanup.

use crate::middlewares::CHANGE_ROOTS_PER_SNAPSHOT;

use super::*;

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
