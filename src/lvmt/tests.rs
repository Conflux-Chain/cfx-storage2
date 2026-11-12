use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use once_cell::sync::Lazy;
use rand_chacha::ChaChaRng;

use amt::{AmtParams, CreateMode};

use crate::{
    backends::{
        impls::kvdb_rocksdb::WrappedRocksDb, DatabaseTrait, HistoricalTableName, PendingTableName,
        WrappedInMemoryDb,
    },
    errors::Result,
    lvmt::types::{LvmtValue, KEY_SLOT_SIZE},
    middlewares::{
        clear_dir, clear_dir_then_create, confirm_ids_to_history, confirm_maps_to_history,
        gen_random_commit_id, gen_updates, get_rng_for_test, CommitID, RecoveryError,
    },
    traits::{KeyValueStoreIterable, KeyValueStoreManager, KeyValueStoreRead},
    StorageError,
};

use super::{compute_state_root, crypto::PE, example::LvmtStorage, storage::LvmtStore};

pub const TEST_LEVEL: usize = 16;

#[cfg(test)]
mod bench_proof_reads;
#[cfg(test)]
mod proofs;

pub static AMT: Lazy<AmtParams<PE>> = Lazy::new(|| {
    AmtParams::from_dir_mont("./pp", TEST_LEVEL, TEST_LEVEL, CreateMode::Neither, None)
});

fn u64_to_boxed_u8(value: u64) -> Box<[u8]> {
    let byte_array: [u8; 8] = value.to_ne_bytes();

    byte_array.into()
}

fn gen_novel_commit_id(rng: &mut ChaChaRng, previous: &mut HashSet<CommitID>) -> CommitID {
    for _ in 0..1 << 4 {
        let novel = gen_random_commit_id(rng);
        if !previous.contains(&novel) {
            previous.insert(novel);
            return novel;
        }
    }

    panic!(
        "Failed to generate a novel commit ID after {} attempts",
        1 << 4
    )
}

/// Holds all the pre-generated data needed for the test.
#[derive(Clone)]
pub struct TestSetup {
    pub commit_1: CommitID,
    pub commit_2: CommitID,
    pub commit_2_1: CommitID,
    pub commit_3: CommitID,
    pub updates_1: Vec<(u64, Option<u64>)>,
    pub updates_2: Vec<(u64, Option<u64>)>,
    pub updates_2_1: Vec<(u64, Option<u64>)>,
    pub updates_3: Vec<(u64, Option<u64>)>,
}

impl TestSetup {
    fn new(num_keys: usize) -> Self {
        let mut rng = get_rng_for_test();

        // Generate different commit_ids
        let mut previous_commits = HashSet::new();
        let commit_1 = gen_novel_commit_id(&mut rng, &mut previous_commits);
        let commit_2 = gen_novel_commit_id(&mut rng, &mut previous_commits);
        let commit_2_1 = gen_novel_commit_id(&mut rng, &mut previous_commits);
        let commit_3 = gen_novel_commit_id(&mut rng, &mut previous_commits);

        // Generate (key, value) changes for each commit
        let previous_keys = Default::default();
        let mut all_keys = Default::default();
        let updates_1_map = gen_updates(&mut rng, &previous_keys, num_keys, 0, &mut all_keys);

        let previous_keys = all_keys.clone();
        let mut all_keys_2_1 = all_keys.clone();
        let updates_2_map =
            gen_updates(&mut rng, &previous_keys, num_keys, num_keys, &mut all_keys);

        let updates_2_1_map = gen_updates(
            &mut rng,
            &previous_keys,
            num_keys,
            num_keys,
            &mut all_keys_2_1,
        );

        let previous_keys = all_keys.clone();
        let updates_3_map =
            gen_updates(&mut rng, &previous_keys, num_keys, num_keys, &mut all_keys);

        let updates_1 = Self::map_to_vec(updates_1_map);
        let updates_2 = Self::map_to_vec(updates_2_map);
        let updates_2_1 = Self::map_to_vec(updates_2_1_map);
        let updates_3 = Self::map_to_vec(updates_3_map);

        TestSetup {
            commit_1,
            commit_2,
            commit_2_1,
            commit_3,
            updates_1,
            updates_2,
            updates_2_1,
            updates_3,
        }
    }

    fn map_to_vec(m: HashMap<u64, Option<u64>>) -> Vec<(u64, Option<u64>)> {
        m.into_iter().collect()
    }

    pub fn changes_iter(
        updates: &[(u64, Option<u64>)],
    ) -> impl Iterator<Item = (Box<[u8]>, Option<Box<[u8]>>)> + '_ {
        updates
            .iter()
            .map(|(k, v)| (u64_to_boxed_u8(*k), v.map(u64_to_boxed_u8)))
    }
}

/// Executes the first phase of the test: performing a series of commit operations.
pub fn run_phase_1<D: DatabaseTrait<HistoricalTableName>, P: DatabaseTrait<PendingTableName>>(
    db: &mut LvmtStorage<D, P>,
    setup: &TestSetup,
) {
    // Get a manager for db
    let mut lvmt = db.as_manager().unwrap();

    // Perform non-forking commits
    lvmt.commit(
        None,
        setup.commit_1,
        TestSetup::changes_iter(&setup.updates_1),
        &AMT,
    )
    .unwrap();
    lvmt.check_consistency(setup.commit_1, &AMT).unwrap();

    lvmt.commit(
        Some(setup.commit_1),
        setup.commit_2,
        TestSetup::changes_iter(&setup.updates_2),
        &AMT,
    )
    .unwrap();
    lvmt.check_consistency(setup.commit_2, &AMT).unwrap();

    // Perform a forking commit
    lvmt.commit(
        Some(setup.commit_1),
        setup.commit_2_1,
        TestSetup::changes_iter(&setup.updates_2_1),
        &AMT,
    )
    .unwrap();
    lvmt.check_consistency(setup.commit_2_1, &AMT).unwrap();

    // Check the previous commit again after adding subsequent commits
    lvmt.check_consistency(setup.commit_1, &AMT).unwrap();
}

/// Verifies the state after a successful root promotion.
pub fn run_verification_after_successful_promotion<
    D: DatabaseTrait<HistoricalTableName>,
    P: DatabaseTrait<PendingTableName>,
>(
    db: &mut LvmtStorage<D, P>,
    setup: &TestSetup,
) {
    let mut lvmt = db.as_manager().unwrap();

    // Commit again to verify success after persisting changes to the backend
    lvmt.commit(
        Some(setup.commit_2),
        setup.commit_3,
        TestSetup::changes_iter(&setup.updates_3),
        &AMT,
    )
    .unwrap();
    lvmt.check_consistency(setup.commit_3, &AMT).unwrap();

    // Check previous commits again after they are confirmed or removed
    lvmt.check_consistency(setup.commit_2, &AMT).unwrap();
    lvmt.check_consistency(setup.commit_1, &AMT).unwrap();
    // commit_2_1 should have been pruned due to the confirmation of commit_2, so this will fail.
    lvmt.check_consistency(setup.commit_2_1, &AMT).unwrap_err();
}

/// Verifies the state after a failed root promotion (which has been rolled back).
pub fn run_verification_after_failed_promotion<
    D: DatabaseTrait<HistoricalTableName>,
    P: DatabaseTrait<PendingTableName>,
>(
    db: &mut LvmtStorage<D, P>,
    setup: &TestSetup,
) {
    let lvmt = db.as_manager().unwrap();

    // 1. Verify the state after recovery: commit_2_1 still exists.
    // Because the root promotion was rolled back, both commit_2 and commit_2_1 are valid children.
    lvmt.check_consistency(setup.commit_2_1, &AMT).unwrap();
    lvmt.check_consistency(setup.commit_2, &AMT).unwrap();
    lvmt.check_consistency(setup.commit_1, &AMT).unwrap();
    drop(lvmt);

    // 2. Now, we re-attempt the previously failed operation. This simulates a real system retrying a failed task after recovery.
    db.confirmed_pending_to_history_with_commit_id(setup.commit_2)
        .unwrap();

    // 3. After the promotion is successfully retried, the system state should be identical to the "successful scenario".
    //    We can directly reuse the successful verification logic to perform the remaining checks.
    run_verification_after_successful_promotion(db, setup);
}

const LARGE_NUM_KEYS: usize = 100000;
static LARGE_TEST_SETUP: Lazy<TestSetup> = Lazy::new(|| TestSetup::new(LARGE_NUM_KEYS));

pub fn get_setup(num_keys: usize) -> Cow<'static, TestSetup> {
    if num_keys == LARGE_NUM_KEYS {
        println!("--- Using cached LARGE TestSetup. ---");
        Cow::Borrowed(&*LARGE_TEST_SETUP)
    } else {
        println!(
            "--- Generating new ad-hoc TestSetup ({} keys)... ---",
            num_keys
        );
        Cow::Owned(TestSetup::new(num_keys))
    }
}

// num_keys = 8 * 10^6 has been tested, but still contain no amt_node_id whose depth > 1
fn test_lvmt_store<D: DatabaseTrait<HistoricalTableName>, P: DatabaseTrait<PendingTableName>>(
    historical_db: Arc<D>,
    pending_db: Arc<P>,
    num_keys: usize,
) {
    let setup = get_setup(num_keys);

    // Initialize db
    let mut db = LvmtStorage::<D, P>::new(historical_db.clone(), pending_db.clone()).unwrap();

    // --- Phase 1 ---
    run_phase_1(&mut db, &setup);

    // Persist confirmed commits from caches to the backend.
    db.confirmed_pending_to_history_with_commit_id(setup.commit_2)
        .unwrap();

    // Note: We are still using the same db instance here because we haven't actually restarted.
    // In the recovery tests, we will create new LvmtStorage instances.
    run_verification_after_successful_promotion(&mut db, &setup);
}

#[derive(Clone)]
pub enum ConstructionMethod {
    Split,   // invoke from_empty_pending and from_recovery
    Unified, // invoke new
}

/// Helper function to set up, simulate a shutdown, and then recover.
/// It returns the recovered db and lvmt instances for subsequent verification.
fn setup_and_reconstruct_for_test<
    D: DatabaseTrait<HistoricalTableName>,
    P: DatabaseTrait<PendingTableName>,
    F: FnOnce(&mut LvmtStorage<D, P>, &TestSetup),
>(
    historical_db: Arc<D>,
    pending_db: Arc<P>,
    num_keys: usize,
    simulate_shutdown_state: F,
    method: ConstructionMethod,
) -> (Result<LvmtStorage<D, P>>, Cow<'static, TestSetup>) {
    let setup = get_setup(num_keys);

    // --- Simulate pre-shutdown operations ---
    {
        let mut db = match method {
            ConstructionMethod::Split => {
                LvmtStorage::<D, P>::from_empty_pending(historical_db.clone(), pending_db.clone())
                    .unwrap()
            }
            ConstructionMethod::Unified => {
                LvmtStorage::<D, P>::new(historical_db.clone(), pending_db.clone()).unwrap()
            }
        };
        run_phase_1(&mut db, &setup);

        simulate_shutdown_state(&mut db, &setup);
    }

    // --- Simulate post-shutdown recovery ---
    let db_res = match method {
        ConstructionMethod::Split => {
            LvmtStorage::<D, P>::from_recovery(historical_db.clone(), pending_db.clone())
        }
        ConstructionMethod::Unified => {
            LvmtStorage::<D, P>::new(historical_db.clone(), pending_db.clone())
        }
    };

    (db_res, setup)
}

fn test_lvmt_recovery_consistent_state<
    D: DatabaseTrait<HistoricalTableName>,
    P: DatabaseTrait<PendingTableName>,
>(
    historical_db: Arc<D>,
    pending_db: Arc<P>,
    num_keys: usize,
    method: ConstructionMethod,
) {
    let (db_res, setup) = setup_and_reconstruct_for_test(
        historical_db.clone(),
        pending_db.clone(),
        num_keys,
        |db, setup| {
            // Simulate a normal shutdown: successfully promote commit_2 as the new root.
            db.confirmed_pending_to_history_with_commit_id(setup.commit_2)
                .unwrap();
        },
        method,
    );

    // The state after recovery should be that the promotion was successful, so we proceed directly to the success verification.
    let mut db = db_res.unwrap();
    run_verification_after_successful_promotion(&mut db, &setup);
}

fn test_lvmt_recovery_pending_ahead<
    D: DatabaseTrait<HistoricalTableName>,
    P: DatabaseTrait<PendingTableName>,
>(
    historical_db: Arc<D>,
    pending_db: Arc<P>,
    num_keys: usize,
    method: ConstructionMethod,
) {
    let (db_res, setup) = setup_and_reconstruct_for_test(
        historical_db.clone(),
        pending_db.clone(),
        num_keys,
        |db, setup| {
            // Simulate an abnormal shutdown: pending_db is updated, but historical_db is not.
            db.make_pending_db_ahead_for_test(setup.commit_2).unwrap();
        },
        method,
    );

    // The recovery logic should have rolled back the promotion, so we use the verification logic for the failed scenario.
    let mut db = db_res.unwrap();
    run_verification_after_failed_promotion(&mut db, &setup);
}

fn test_lvmt_recovery_historical_ahead<
    D: DatabaseTrait<HistoricalTableName>,
    P: DatabaseTrait<PendingTableName>,
>(
    historical_db: Arc<D>,
    pending_db: Arc<P>,
    num_keys: usize,
    method: ConstructionMethod,
) {
    let (db_res, setup) = setup_and_reconstruct_for_test(
        historical_db.clone(),
        pending_db.clone(),
        num_keys,
        |db, setup| {
            // Simulate an abnormal shutdown: historical_db is updated, but pending_db is not.
            db.make_historical_db_ahead_for_test(setup.commit_2)
                .unwrap();
        },
        method.clone(),
    );

    let mut db = match method {
        ConstructionMethod::Split => {
            // Assert that starting in recovery mode fails because pending_db is outdated.
            assert!(
                matches!(
                    db_res,
                    Err(StorageError::RecoveryError(
                        RecoveryError::NoValidSnapshotFound
                    ))
                ),
                "Expected recovery to fail with NoValidSnapshotFound"
            );

            // Since regular recovery failed, we perform a bootstrap recovery.
            LvmtStorage::from_bootstrap(historical_db.clone(), pending_db.clone()).unwrap()
        }

        ConstructionMethod::Unified => db_res.unwrap(),
    };

    let mut lvmt = db.as_manager().unwrap();

    // Verify the state.
    // After bootstrap recovery, the historical state should be equivalent to the state after a successful promotion, and the pending part should be empty.
    // In this example, the pending part of a successful recovery should have a commit_2 node, so we first add commit_2,
    // and then reuse `run_verification_after_successful_promotion` to verify that subsequent operations are correct.
    lvmt.commit(
        Some(setup.commit_1),
        setup.commit_2,
        TestSetup::changes_iter(&setup.updates_2),
        &AMT,
    )
    .unwrap();
    lvmt.check_consistency(setup.commit_2, &AMT).unwrap();
    drop(lvmt);

    run_verification_after_successful_promotion(&mut db, &setup);
}

#[test]
fn test_lvmt_store_rocksdb() {
    let historical_path = "__test_lvmt_store_historical";
    let pending_path = "__test_lvmt_store_pending";

    clear_dir_then_create(historical_path);
    clear_dir_then_create(pending_path);

    let historical_db = WrappedRocksDb::open(historical_path).unwrap();
    let pending_db = WrappedRocksDb::open(pending_path).unwrap();

    test_lvmt_store::<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>(
        Arc::new(historical_db),
        Arc::new(pending_db),
        100000,
    );

    clear_dir(historical_path);
    clear_dir(pending_path);
}

#[test]
fn test_lvmt_store_inmemory() {
    let historical_db = WrappedInMemoryDb::empty();
    let pending_db = WrappedInMemoryDb::empty();

    test_lvmt_store::<WrappedInMemoryDb<HistoricalTableName>, WrappedInMemoryDb<PendingTableName>>(
        Arc::new(historical_db),
        Arc::new(pending_db),
        1000,
    );
}

fn setup_logger() {
    let _ = env_logger::try_init();
}

#[test]
fn test_lvmt_recovery_consistent_state_rocksdb_unified() {
    let historical_path = "__test_lvmt_recovery_consistent_state_historical_unified";
    let pending_path = "__test_lvmt_recovery_consistent_state_pending_unified";

    clear_dir_then_create(historical_path);
    clear_dir_then_create(pending_path);

    let historical_db = WrappedRocksDb::open(historical_path).unwrap();
    let pending_db = WrappedRocksDb::open(pending_path).unwrap();

    test_lvmt_recovery_consistent_state::<
        WrappedRocksDb<HistoricalTableName>,
        WrappedRocksDb<PendingTableName>,
    >(
        Arc::new(historical_db),
        Arc::new(pending_db),
        100000,
        ConstructionMethod::Unified,
    );

    clear_dir(historical_path);
    clear_dir(pending_path);
}

#[test]
fn test_lvmt_recovery_consistent_state_inmemory_unified() {
    let historical_db = WrappedInMemoryDb::empty();
    let pending_db = WrappedInMemoryDb::empty();

    test_lvmt_recovery_consistent_state::<
        WrappedInMemoryDb<HistoricalTableName>,
        WrappedInMemoryDb<PendingTableName>,
    >(
        Arc::new(historical_db),
        Arc::new(pending_db),
        1000,
        ConstructionMethod::Unified,
    );
}

#[test]
fn test_lvmt_recovery_pending_ahead_rocksdb_unified() {
    setup_logger();

    let historical_path = "__test_lvmt_recovery_pending_ahead_historical_unified";
    let pending_path = "__test_lvmt_recovery_pending_ahead_pending_unified";

    clear_dir_then_create(historical_path);
    clear_dir_then_create(pending_path);

    let historical_db = WrappedRocksDb::open(historical_path).unwrap();
    let pending_db = WrappedRocksDb::open(pending_path).unwrap();

    test_lvmt_recovery_pending_ahead::<
        WrappedRocksDb<HistoricalTableName>,
        WrappedRocksDb<PendingTableName>,
    >(
        Arc::new(historical_db),
        Arc::new(pending_db),
        100000,
        ConstructionMethod::Unified,
    );

    clear_dir(historical_path);
    clear_dir(pending_path);
}

#[test]
fn test_lvmt_recovery_pending_ahead_inmemory_unified() {
    setup_logger();

    let historical_db = WrappedInMemoryDb::empty();
    let pending_db = WrappedInMemoryDb::empty();

    test_lvmt_recovery_pending_ahead::<
        WrappedInMemoryDb<HistoricalTableName>,
        WrappedInMemoryDb<PendingTableName>,
    >(
        Arc::new(historical_db),
        Arc::new(pending_db),
        1000,
        ConstructionMethod::Unified,
    );
}

#[test]
fn test_lvmt_recovery_historical_ahead_rocksdb_unified() {
    setup_logger();

    let historical_path = "__test_lvmt_recovery_historical_ahead_historical_unified";
    let pending_path = "__test_lvmt_recovery_historical_ahead_pending_unified";

    clear_dir_then_create(historical_path);
    clear_dir_then_create(pending_path);

    let historical_db = WrappedRocksDb::open(historical_path).unwrap();
    let pending_db = WrappedRocksDb::open(pending_path).unwrap();

    test_lvmt_recovery_historical_ahead::<
        WrappedRocksDb<HistoricalTableName>,
        WrappedRocksDb<PendingTableName>,
    >(
        Arc::new(historical_db),
        Arc::new(pending_db),
        100000,
        ConstructionMethod::Unified,
    );

    clear_dir(historical_path);
    clear_dir(pending_path);
}

#[test]
fn test_lvmt_recovery_historical_ahead_inmemory_unified() {
    setup_logger();

    let historical_db = WrappedInMemoryDb::empty();
    let pending_db = WrappedInMemoryDb::empty();

    test_lvmt_recovery_historical_ahead::<
        WrappedInMemoryDb<HistoricalTableName>,
        WrappedInMemoryDb<PendingTableName>,
    >(
        Arc::new(historical_db),
        Arc::new(pending_db),
        1000,
        ConstructionMethod::Unified,
    );
}

#[test]
fn test_lvmt_recovery_consistent_state_rocksdb() {
    let historical_path = "__test_lvmt_recovery_consistent_state_historical";
    let pending_path = "__test_lvmt_recovery_consistent_state_pending";

    clear_dir_then_create(historical_path);
    clear_dir_then_create(pending_path);

    let historical_db = WrappedRocksDb::open(historical_path).unwrap();
    let pending_db = WrappedRocksDb::open(pending_path).unwrap();

    test_lvmt_recovery_consistent_state::<
        WrappedRocksDb<HistoricalTableName>,
        WrappedRocksDb<PendingTableName>,
    >(
        Arc::new(historical_db),
        Arc::new(pending_db),
        100000,
        ConstructionMethod::Split,
    );

    clear_dir(historical_path);
    clear_dir(pending_path);
}

#[test]
fn test_lvmt_recovery_consistent_state_inmemory() {
    let historical_db = WrappedInMemoryDb::empty();
    let pending_db = WrappedInMemoryDb::empty();

    test_lvmt_recovery_consistent_state::<
        WrappedInMemoryDb<HistoricalTableName>,
        WrappedInMemoryDb<PendingTableName>,
    >(
        Arc::new(historical_db),
        Arc::new(pending_db),
        1000,
        ConstructionMethod::Split,
    );
}

#[test]
fn test_lvmt_recovery_pending_ahead_rocksdb() {
    setup_logger();

    let historical_path = "__test_lvmt_recovery_pending_ahead_historical";
    let pending_path = "__test_lvmt_recovery_pending_ahead_pending";

    clear_dir_then_create(historical_path);
    clear_dir_then_create(pending_path);

    let historical_db = WrappedRocksDb::open(historical_path).unwrap();
    let pending_db = WrappedRocksDb::open(pending_path).unwrap();

    test_lvmt_recovery_pending_ahead::<
        WrappedRocksDb<HistoricalTableName>,
        WrappedRocksDb<PendingTableName>,
    >(
        Arc::new(historical_db),
        Arc::new(pending_db),
        100000,
        ConstructionMethod::Split,
    );

    clear_dir(historical_path);
    clear_dir(pending_path);
}

#[test]
fn test_lvmt_recovery_pending_ahead_inmemory() {
    setup_logger();

    let historical_db = WrappedInMemoryDb::empty();
    let pending_db = WrappedInMemoryDb::empty();

    test_lvmt_recovery_pending_ahead::<
        WrappedInMemoryDb<HistoricalTableName>,
        WrappedInMemoryDb<PendingTableName>,
    >(
        Arc::new(historical_db),
        Arc::new(pending_db),
        1000,
        ConstructionMethod::Split,
    );
}

#[test]
fn test_lvmt_recovery_historical_ahead_rocksdb() {
    setup_logger();

    let historical_path = "__test_lvmt_recovery_historical_ahead_historical";
    let pending_path = "__test_lvmt_recovery_historical_ahead_pending";

    clear_dir_then_create(historical_path);
    clear_dir_then_create(pending_path);

    let historical_db = WrappedRocksDb::open(historical_path).unwrap();
    let pending_db = WrappedRocksDb::open(pending_path).unwrap();

    test_lvmt_recovery_historical_ahead::<
        WrappedRocksDb<HistoricalTableName>,
        WrappedRocksDb<PendingTableName>,
    >(
        Arc::new(historical_db),
        Arc::new(pending_db),
        100000,
        ConstructionMethod::Split,
    );

    clear_dir(historical_path);
    clear_dir(pending_path);
}

#[test]
fn test_lvmt_recovery_historical_ahead_inmemory() {
    setup_logger();

    let historical_db = WrappedInMemoryDb::empty();
    let pending_db = WrappedInMemoryDb::empty();

    test_lvmt_recovery_historical_ahead::<
        WrappedInMemoryDb<HistoricalTableName>,
        WrappedInMemoryDb<PendingTableName>,
    >(
        Arc::new(historical_db),
        Arc::new(pending_db),
        1000,
        ConstructionMethod::Split,
    );
}

#[test]
fn test_lvmt_commit_with_empty_changes() {
    // Define paths for the test databases.
    let historical_path = "__test_lvmt_commit_with_empty_changes_historical";
    let pending_path = "__test_lvmt_commit_with_empty_changes_pending";

    // Create clean directories for the test.
    clear_dir_then_create(historical_path);
    clear_dir_then_create(pending_path);

    // Initialize database instances.
    let historical_db = Arc::new(WrappedRocksDb::open(historical_path).unwrap());
    let pending_db = Arc::new(WrappedRocksDb::open(pending_path).unwrap());

    // Set up the LVMT storage from an empty state.
    let mut db =
        LvmtStorage::<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>::new(
            historical_db,
            pending_db,
        )
        .unwrap();

    // Get a manager to interact with the LVMT.
    let mut lvmt = db.as_manager().unwrap();

    // Generate a unique commit ID for our test commit.
    let mut rng = get_rng_for_test();
    let mut previous_commits = HashSet::new();
    let commit_id = gen_novel_commit_id(&mut rng, &mut previous_commits);

    // Perform a commit with an empty set of updates.
    lvmt.commit(None, commit_id, std::iter::empty(), &AMT)
        .unwrap();

    lvmt.check_consistency(commit_id, &AMT).unwrap();

    clear_dir(historical_path);
    clear_dir(pending_path);
}

#[test]
fn test_lvmt_leaf_with_multiple_keys() {
    use crate::utils::hash::blake2s;

    // Random keys almost never share a leaf (2^32 leaves at depth 1), so search for
    // two keys whose digests share the first 4 bytes (= the depth-1 leaf id).
    let (k_a, k_b) = {
        let mut seen = HashMap::new();
        (0u64..)
            .find_map(|i| {
                let key = u64_to_boxed_u8(i);
                let leaf: [u8; 4] = blake2s(&key)[..4].try_into().unwrap();
                seen.insert(leaf, key.clone()).map(|prev| (prev, key))
            })
            .unwrap()
    };

    let historical_path = "__test_lvmt_leaf_with_multiple_keys_historical";
    let pending_path = "__test_lvmt_leaf_with_multiple_keys_pending";
    clear_dir_then_create(historical_path);
    clear_dir_then_create(pending_path);

    let historical_db = Arc::new(WrappedRocksDb::open(historical_path).unwrap());
    let pending_db = Arc::new(WrappedRocksDb::open(pending_path).unwrap());
    let mut db =
        LvmtStorage::<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>::new(
            historical_db,
            pending_db,
        )
        .unwrap();

    let mut rng = get_rng_for_test();
    let mut previous_commits = HashSet::new();
    let mut novel = || gen_novel_commit_id(&mut rng, &mut previous_commits);
    let (c1, c2, c3) = (novel(), novel(), novel());
    let change =
        |k: &[u8]| std::iter::once((Box::<[u8]>::from(k), Some(Box::<[u8]>::from(&b"v"[..]))));

    {
        let mut lvmt = db.as_manager().unwrap();
        lvmt.commit(None, c1, change(&k_a), &AMT).unwrap();
        lvmt.commit(Some(c1), c2, change(&k_b), &AMT).unwrap();
        lvmt.commit(Some(c2), c3, std::iter::empty(), &AMT).unwrap();

        let (amt_id, node_index, _) = lvmt
            .get(c2, k_a.clone())
            .unwrap()
            .unwrap()
            .allocation
            .amt_info(&k_a);
        assert_eq!(
            lvmt.get_leaf_keys(c1, &amt_id, node_index)
                .unwrap()
                .unwrap()
                .keys(),
            &[k_a.clone()]
        );
        assert_eq!(
            lvmt.get_leaf_keys(c2, &amt_id, node_index)
                .unwrap()
                .unwrap()
                .keys(),
            &[k_a.clone(), k_b.clone()]
        );
        let scanned = lvmt.get_amt_slot_keys(c2, &amt_id).unwrap();
        assert_eq!(scanned.len(), 1);
        assert_eq!(scanned[0].0, node_index);
        assert_eq!(scanned[0].1.keys(), &[k_a.clone(), k_b.clone()]);
        for c in [c1, c2, c3] {
            lvmt.check_consistency(c, &AMT).unwrap();
        }
    }

    // The same lookups through the historical part.
    db.confirmed_pending_to_history_with_commit_id(c3).unwrap();
    {
        let lvmt = db.as_manager().unwrap();
        let (amt_id, node_index, _) = lvmt
            .get(c2, k_a.clone())
            .unwrap()
            .unwrap()
            .allocation
            .amt_info(&k_a);
        assert_eq!(
            lvmt.get_leaf_keys(c1, &amt_id, node_index)
                .unwrap()
                .unwrap()
                .keys(),
            &[k_a.clone()]
        );
        assert_eq!(
            lvmt.get_amt_slot_keys(c2, &amt_id).unwrap()[0].1.keys(),
            &[k_a.clone(), k_b.clone()]
        );
        for c in [c1, c2, c3] {
            lvmt.check_consistency(c, &AMT).unwrap();
        }
    }

    drop(db);
    clear_dir(historical_path);
    clear_dir(pending_path);
}

#[test]
fn test_lvmt_last_modified() {
    use super::types::AmtId;

    let historical_path = "__test_lvmt_last_modified_historical";
    let pending_path = "__test_lvmt_last_modified_pending";
    clear_dir_then_create(historical_path);
    clear_dir_then_create(pending_path);

    let historical_db = Arc::new(WrappedRocksDb::open(historical_path).unwrap());
    let pending_db = Arc::new(WrappedRocksDb::open(pending_path).unwrap());
    let mut db =
        LvmtStorage::<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>::new(
            historical_db,
            pending_db,
        )
        .unwrap();

    let mut rng = get_rng_for_test();
    let mut previous_commits = HashSet::new();
    let mut novel = || gen_novel_commit_id(&mut rng, &mut previous_commits);
    let (c1, c2, c3, c2b, c4) = (novel(), novel(), novel(), novel(), novel());
    let (k1, k2, k_none) = (u64_to_boxed_u8(1), u64_to_boxed_u8(2), u64_to_boxed_u8(99));

    // c1 -> c2 -> c3 (empty), and a fork c1 -> c2b
    {
        let mut lvmt = db.as_manager().unwrap();
        lvmt.commit(
            None,
            c1,
            TestSetup::changes_iter(&[(1, Some(10)), (2, Some(20))]),
            &AMT,
        )
        .unwrap();
        lvmt.commit(
            Some(c1),
            c2,
            TestSetup::changes_iter(&[(1, Some(11))]),
            &AMT,
        )
        .unwrap();
        lvmt.commit(Some(c2), c3, std::iter::empty(), &AMT).unwrap();
        lvmt.commit(
            Some(c1),
            c2b,
            TestSetup::changes_iter(&[(2, Some(21))]),
            &AMT,
        )
        .unwrap();
    }

    let last = |lvmt: &LvmtStore<'_, '_, _>, commit, key: &[u8]| {
        lvmt.get_key_last_modified(commit, key)
            .unwrap()
            .map(|(c, v)| {
                assert_eq!(Some(v.clone()), lvmt.get(commit, key.into()).unwrap());
                (c, v.get_value())
            })
    };
    let amt_last = |lvmt: &LvmtStore<'_, '_, _>, commit, amt_id: &AmtId| {
        lvmt.get_amt_last_modified(commit, amt_id)
            .unwrap()
            .map(|(c, _)| c)
    };

    // Pending part, with the current map placed at different commits: none,
    // an ancestor of the query (c2 for c3), the query itself, and a sibling (c2b).
    for checkout in [None, Some(c2), Some(c3), Some(c2b)] {
        let lvmt = db.as_manager().unwrap();
        if let Some(c) = checkout {
            lvmt.get_state(c, true).unwrap();
        }
        let b = |x: u64| Some(u64_to_boxed_u8(x));
        assert_eq!(last(&lvmt, c3, &k1), Some((c2, b(11))));
        assert_eq!(last(&lvmt, c3, &k2), Some((c1, b(20))));
        assert_eq!(last(&lvmt, c2, &k1), Some((c2, b(11))));
        assert_eq!(last(&lvmt, c1, &k1), Some((c1, b(10))));
        assert_eq!(last(&lvmt, c2b, &k1), Some((c1, b(10))));
        assert_eq!(last(&lvmt, c2b, &k2), Some((c2b, b(21))));
        assert_eq!(last(&lvmt, c3, &k_none), None);

        // The root AMT changes in every non-empty commit.
        assert_eq!(amt_last(&lvmt, c3, &AmtId::root()), Some(c2));
        assert_eq!(amt_last(&lvmt, c2b, &AmtId::root()), Some(c2b));
        lvmt.check_consistency(c3, &AMT).unwrap();
        lvmt.check_consistency(c2b, &AMT).unwrap();
    }

    // Confirm c1, c2 to history (c3 becomes the pending root; c2b is discarded).
    db.confirmed_pending_to_history_with_commit_id(c3).unwrap();
    {
        let mut lvmt = db.as_manager().unwrap();
        let b = |x: u64| Some(u64_to_boxed_u8(x));
        // Pending commit falling back to the latest historical version.
        assert_eq!(last(&lvmt, c3, &k1), Some((c2, b(11))));
        assert_eq!(last(&lvmt, c3, &k2), Some((c1, b(20))));
        // Latest historical version (c2) and a previous one (c1).
        assert_eq!(last(&lvmt, c2, &k1), Some((c2, b(11))));
        assert_eq!(last(&lvmt, c2, &k2), Some((c1, b(20))));
        assert_eq!(last(&lvmt, c1, &k1), Some((c1, b(10))));
        assert_eq!(last(&lvmt, c1, &k2), Some((c1, b(20))));
        assert_eq!(last(&lvmt, c1, &k_none), None);
        assert_eq!(last(&lvmt, c2, &k_none), None);
        assert_eq!(amt_last(&lvmt, c3, &AmtId::root()), Some(c2));
        assert_eq!(amt_last(&lvmt, c1, &AmtId::root()), Some(c1));

        // Scanning the changes of a historical commit: every AMT it updated, each
        // last modified at that very commit; the root AMT comes first.
        for c in [c1, c2] {
            let changes = lvmt.get_historical_amt_changes(c).unwrap();
            assert_eq!(changes.first().map(|(id, _)| *id), Some(AmtId::root()));
            assert!(changes.windows(2).all(|w| w[0].0 < w[1].0));
            for (amt_id, point) in changes {
                assert_eq!(
                    lvmt.get_amt_last_modified(c, &amt_id).unwrap(),
                    Some((c, point))
                );
            }
        }
        // c1 wrote two keys, c2 one: at least one non-root AMT each.
        assert!(lvmt.get_historical_amt_changes(c1).unwrap().len() >= 2);
        assert!(lvmt.get_historical_amt_changes(c3).is_err()); // still pending

        // A new pending commit on top of the confirmed history.
        lvmt.commit(
            Some(c3),
            c4,
            TestSetup::changes_iter(&[(2, Some(22))]),
            &AMT,
        )
        .unwrap();
        assert_eq!(last(&lvmt, c4, &k2), Some((c4, b(22))));
        assert_eq!(last(&lvmt, c4, &k1), Some((c2, b(11))));

        // Unknown commit.
        assert!(lvmt.get_key_last_modified(novel(), &k1).is_err());

        for c in [c1, c2, c3, c4] {
            lvmt.check_consistency(c, &AMT).unwrap();
        }
    }

    drop(db);
    clear_dir(historical_path);
    clear_dir(pending_path);
}

#[test]
fn test_lvmt_state_root() {
    use super::{types::CurvePointWithVersion, EMPTY_AUTH_CHANGE_ROOT};

    let historical_path = "__test_lvmt_state_root_historical";
    let pending_path = "__test_lvmt_state_root_pending";
    clear_dir_then_create(historical_path);
    clear_dir_then_create(pending_path);

    let historical_db = Arc::new(WrappedRocksDb::open(historical_path).unwrap());
    let pending_db = Arc::new(WrappedRocksDb::open(pending_path).unwrap());
    let mut db =
        LvmtStorage::<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>::new(
            historical_db,
            pending_db,
        )
        .unwrap();
    let mut lvmt = db.as_manager().unwrap();

    let mut rng = get_rng_for_test();
    let mut previous_commits = HashSet::new();
    let mut novel = || gen_novel_commit_id(&mut rng, &mut previous_commits);
    let (c0, c1, c2, c3) = (novel(), novel(), novel(), novel());
    let updates = [(1u64, Some(10u64)), (2, Some(20)), (3, None)];

    // Empty commit from scratch: default root commitment, empty auth-change tree.
    let r0 = lvmt.commit(None, c0, std::iter::empty(), &AMT).unwrap();
    assert_eq!(lvmt.get_state_root(c0).unwrap(), Some(r0));
    assert_eq!(
        lvmt.get_root_commitment(c0).unwrap(),
        CurvePointWithVersion::default()
    );
    assert_eq!(
        lvmt.get_auth_change_root(c0).unwrap(),
        Some(EMPTY_AUTH_CHANGE_ROOT)
    );
    lvmt.check_consistency(c0, &AMT).unwrap();

    // A commit with changes: the returned state root is the recorded one.
    let r1 = lvmt
        .commit(Some(c0), c1, TestSetup::changes_iter(&updates), &AMT)
        .unwrap();
    assert_eq!(lvmt.get_state_root(c1).unwrap(), Some(r1));
    assert_ne!(r1, r0);
    assert_ne!(
        lvmt.get_auth_change_root(c1).unwrap(),
        Some(EMPTY_AUTH_CHANGE_ROOT)
    );
    lvmt.check_consistency(c1, &AMT).unwrap();

    // An empty child inherits the root commitment, but its auth-change root is empty,
    // so its state root differs from the parent's.
    let r2 = lvmt.commit(Some(c1), c2, std::iter::empty(), &AMT).unwrap();
    assert_eq!(
        lvmt.get_root_commitment(c2).unwrap(),
        lvmt.get_root_commitment(c1).unwrap()
    );
    assert_eq!(
        lvmt.get_auth_change_root(c2).unwrap(),
        Some(EMPTY_AUTH_CHANGE_ROOT)
    );
    assert_ne!(r2, r1);
    lvmt.check_consistency(c2, &AMT).unwrap();

    // Re-applying the same changes bumps the versions, hence a different state root.
    let r3 = lvmt
        .commit(Some(c1), c3, TestSetup::changes_iter(&updates), &AMT)
        .unwrap();
    assert_ne!(r3, r1);
    lvmt.check_consistency(c3, &AMT).unwrap();

    // Unknown commit.
    assert_eq!(lvmt.get_auth_change_root(novel()).unwrap(), None);

    drop(lvmt);
    drop(db);
    clear_dir(historical_path);
    clear_dir(pending_path);
}

impl<P: DatabaseTrait<PendingTableName>> LvmtStore<'_, '_, P> {
    pub fn check_consistency(&self, commit: CommitID, pp: &AmtParams<PE>) -> Result<()> {
        use std::collections::BTreeSet;

        use ark_ec::CurveGroup;

        use crate::lvmt::{
            crypto::{FrInt, VariableBaseMSM, G1},
            types::SLOT_SIZE,
        };

        let amt_node_view = self
            .get_amt_node_store()
            .get_versioned_store(&commit, true)?;
        let slot_alloc_view = self
            .get_slot_alloc_store()
            .get_versioned_store(&commit, true)?;
        let key_value_view = self
            .get_key_value_store()
            .get_versioned_store(&commit, true)?;

        // For each Amt tree (except the children of the root Amt),
        // the leaf node with the same AmtId in its parent Amt tree must be fully allocated.
        // The expection of the children of the root Amt is due to the design that the root Amt does not allocate slots.
        for (amt_id, curve_point_with_version) in amt_node_view.iter()? {
            if amt_id.len() > 1 {
                let amt_node_id = amt_id;
                let alloc_key_info = slot_alloc_view.get(&amt_node_id)?.unwrap();
                assert_eq!(alloc_key_info.index as usize, KEY_SLOT_SIZE - 1);
            }
        }

        // Each Amt node with allocated slots should be in an Amt tree
        for (amt_node_id, alloc_key_info) in slot_alloc_view.iter()? {
            let mut parent_amt_id = amt_node_id;
            parent_amt_id.pop().unwrap();

            amt_node_view.get(&parent_amt_id)?.unwrap();
        }

        // Gather the versions of allocated slots for keys
        let mut slot_versions = BTreeMap::new();
        for (key, lvmt_value) in key_value_view.iter()? {
            let LvmtValue {
                allocation,
                version,
                ..
            } = lvmt_value;
            let (amt_id, node_index, slot_index) = allocation.amt_info(&key);
            let node_map = slot_versions.entry(amt_id).or_insert_with(BTreeMap::new);
            let slot_map = node_map.entry(node_index).or_insert_with(BTreeMap::new);
            slot_map.insert(slot_index, version);
        }

        // G1: every key is recorded in the slot it is allocated to ...
        for (key, lvmt_value) in key_value_view.iter()? {
            let (amt_id, node_index, slot_index) = lvmt_value.allocation.amt_info(&key);
            let info = self.get_leaf_keys(commit, &amt_id, node_index)?.unwrap();
            assert_eq!(
                info.keys()[slot_index as usize],
                key,
                "Key missing from its slot"
            );
        }
        // ... and every recorded key is allocated to that slot.
        let mut leaves_by_amt: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for (amt_node_id, alloc_key_info) in slot_alloc_view.iter()? {
            assert_eq!(
                alloc_key_info.keys().len(),
                alloc_key_info.index as usize + 1
            );
            let mut amt_id = amt_node_id;
            let node_index = amt_id.pop().unwrap();
            for (slot_index, key) in alloc_key_info.keys().iter().enumerate() {
                let lvmt_value = key_value_view.get(key)?.unwrap();
                assert_eq!(
                    lvmt_value.allocation.amt_info(key),
                    (amt_id, node_index, slot_index as u8),
                    "Recorded key not allocated to its slot"
                );
            }
            leaves_by_amt
                .entry(amt_id)
                .or_default()
                .push((node_index, alloc_key_info));
        }
        // The prefix scan of an AMT returns exactly its leaves.
        for (amt_id, mut leaves) in leaves_by_amt {
            leaves.sort_by_key(|(node_index, _)| *node_index);
            assert_eq!(
                self.get_amt_slot_keys(commit, &amt_id)?,
                leaves,
                "Prefix scan mismatch"
            );
        }

        // Gather allocated slots for keys, in another way
        let mut slot_allocs = BTreeMap::new();
        for (amt_node_id, alloc_key_info) in slot_alloc_view.iter()? {
            let mut parent_amt_id = amt_node_id;
            let node_index = parent_amt_id.pop().unwrap();

            for slot_index in 0..=alloc_key_info.index {
                let node_map = slot_allocs
                    .entry(parent_amt_id)
                    .or_insert_with(BTreeMap::new);
                let slot_map = node_map.entry(node_index).or_insert_with(BTreeSet::new);
                slot_map.insert(slot_index);
            }
        }

        // Check consistency between slot_versions and slot_allocs
        let slot_versions_simple: BTreeMap<_, _> = slot_versions
            .iter()
            .map(|(amt_id, node_map)| {
                let node_map_simple: BTreeMap<_, _> = node_map
                    .iter()
                    .map(|(node_index, slot_map)| {
                        let slot_set: BTreeSet<_> = slot_map.keys().cloned().collect();
                        (*node_index, slot_set)
                    })
                    .collect();
                (*amt_id, node_map_simple)
            })
            .collect();

        assert_eq!(
            slot_versions_simple, slot_allocs,
            "Inconsistent allocations"
        );

        // Gather the versions of allocated slots for Amt trees (except the root Amt)
        for (amt_id, curve_point_with_version) in amt_node_view.iter()? {
            if !amt_id.is_empty() {
                let mut parent_amt_id = amt_id;
                let node_index = parent_amt_id.pop().unwrap();
                let slot_index = SLOT_SIZE - 1;
                let version = curve_point_with_version.version;

                let node_map = slot_versions
                    .entry(parent_amt_id)
                    .or_insert_with(BTreeMap::new);
                let slot_map = node_map.entry(node_index).or_insert_with(BTreeMap::new);
                slot_map.insert(slot_index as u8, version);
            }
        }

        // Compute the commitment of each Amt tree
        for (amt_id, node_map) in slot_versions {
            let mut basis = vec![];
            let mut bigints = vec![];
            for (node_index, slot_map) in node_map {
                let basis_power = pp.get_basis_power_at(node_index as usize);
                for (slot_index, version) in slot_map {
                    basis.push(basis_power[slot_index as usize]);
                    bigints.push(FrInt::from(version));
                }
            }

            let commitment = G1::msm_bigint(&basis[..], &bigints[..]).into_affine();

            let stored_commitment = amt_node_view
                .get(&amt_id)?
                .unwrap()
                .point
                .affine()
                .into_owned();

            assert_eq!(commitment, stored_commitment, "Inconsitent commitments");
        }

        // The last-modified lookup must agree with the snapshot view, and point to a
        // known commit.
        for (key, value) in key_value_view.iter()? {
            let (modified_at, found) = self.get_key_last_modified(commit, &key)?.unwrap();
            assert_eq!(found, value, "Inconsistent last-modified key value");
            assert!(self.get_state_root(modified_at)?.is_some());
        }
        for (amt_id, point) in amt_node_view.iter()? {
            let (modified_at, found) = self.get_amt_last_modified(commit, &amt_id)?.unwrap();
            assert_eq!(found, point, "Inconsistent last-modified AMT commitment");
            assert!(self.get_state_root(modified_at)?.is_some());
        }

        // The recorded state root must be recomputable from the read-only interfaces.
        let state_root = self.get_state_root(commit)?.unwrap();
        let root_commitment = self.get_root_commitment(commit)?;
        let auth_change_root = self.get_auth_change_root(commit)?.unwrap();
        let hroot = self.get_hroot(commit)?;
        assert_eq!(
            state_root,
            compute_state_root(&root_commitment, &auth_change_root, &hroot),
            "Inconsistent state root"
        );

        // The hroot accumulates the state roots of the ancestors, provably.
        let height = self
            .get_key_value_store()
            .get_commit_height(&commit)?
            .unwrap();
        let mut heights: Vec<u64> = [0, height / 3, height / 2, height.saturating_sub(1)]
            .into_iter()
            .filter(|h| *h < height)
            .collect();
        heights.dedup();
        for h in heights {
            let ancestor = self
                .get_key_value_store()
                .get_ancestor_at_height(&commit, h)?;
            let proof = self.prove_hroot_leaf(commit, ancestor)?;
            let ancestor_root = self.get_state_root(ancestor)?.unwrap();
            assert!(
                proof.verify(&hroot, &ancestor_root),
                "MMR proof of height {h}"
            );
        }
        assert!(self.prove_hroot_leaf(commit, commit).is_err());
        Ok(())
    }
}

impl<D: DatabaseTrait<HistoricalTableName>, P: DatabaseTrait<PendingTableName>> LvmtStorage<D, P> {
    pub fn confirmed_pending_to_history_with_commit_id(
        &mut self,
        new_root_commit_id: CommitID,
    ) -> Result<()> {
        self.confirmed_pending_to_history_with_commit_id_inner(new_root_commit_id)
    }

    /// **For testing only**
    /// Simulates a crash that occurs after pending_db is updated but before historical_db is updated.
    /// This executes a part of the logic of `confirmed_pending_to_history_with_commit_id`:
    /// It updates the in-memory state of the pending component and pending_db, but does not move the nodes to historical_db.
    pub fn make_pending_db_ahead_for_test(&mut self, new_root_commit_id: CommitID) -> Result<()> {
        // pending part
        let pending_write_schema = P::write_schema();

        let maybe_key_value_confirmed_path = self
            .key_value_cache
            .change_root(new_root_commit_id, &pending_write_schema)?;
        if let Some(key_value_confirmed_path) = maybe_key_value_confirmed_path {
            let amt_node_confirmed_path = self
                .amt_node_cache
                .change_root(new_root_commit_id, &pending_write_schema)?
                .expect("AMT node cache should have changed root if key-value cache did");
            let slot_alloc_confirmed_path = self
                .slot_alloc_cache
                .change_root(new_root_commit_id, &pending_write_schema)?
                .expect("Slot alloc cache should have changed root if key-value cache did");

            assert!(key_value_confirmed_path.is_same_path(&amt_node_confirmed_path));
            assert!(key_value_confirmed_path.is_same_path(&slot_alloc_confirmed_path));

            let start_height = key_value_confirmed_path.start_height;
            let commit_ids = &key_value_confirmed_path.commit_ids;

            self.commit_to_pending_db(pending_write_schema)?;

            // historical part, no write, in order to simulate failure in writing to historical_db
        } // else: nothing is changed and no records are in pending_write_schema, so skip directly

        Ok(())
    }

    /// **For testing only**
    /// Simulates a crash that occurs after historical_db is updated but before pending_db is updated.
    /// This executes a part of the logic of `confirmed_pending_to_history_with_commit_id`:
    /// It updates the in-memory state of the pending component and moves the nodes to historical_db, but does not update pending_db.
    pub fn make_historical_db_ahead_for_test(
        &mut self,
        new_root_commit_id: CommitID,
    ) -> Result<()> {
        use crate::lvmt::table_schema::{AmtNodes, FlatKeyValue, SlotAllocations};

        // pending part
        let maybe_key_value_confirmed_path = self
            .key_value_cache
            .change_root_without_persistence(new_root_commit_id)?;
        if let Some(key_value_confirmed_path) = maybe_key_value_confirmed_path {
            let amt_node_confirmed_path = self
                .amt_node_cache
                .change_root_without_persistence(new_root_commit_id)?
                .expect("AMT node cache should have changed root if key-value cache did");
            let slot_alloc_confirmed_path = self
                .slot_alloc_cache
                .change_root_without_persistence(new_root_commit_id)?
                .expect("Slot alloc cache should have changed root if key-value cache did");

            assert!(key_value_confirmed_path.is_same_path(&amt_node_confirmed_path));
            assert!(key_value_confirmed_path.is_same_path(&slot_alloc_confirmed_path));

            let start_height = key_value_confirmed_path.start_height;
            let commit_ids = &key_value_confirmed_path.commit_ids;

            // historical part
            let historical_write_schema = D::write_schema();

            confirm_ids_to_history::<D>(
                self.historical_db.clone(),
                start_height,
                commit_ids,
                &historical_write_schema,
            )?;

            confirm_maps_to_history::<D, FlatKeyValue>(
                self.historical_db.clone(),
                start_height,
                key_value_confirmed_path.key_value_maps,
                &historical_write_schema,
            )?;
            confirm_maps_to_history::<D, AmtNodes>(
                self.historical_db.clone(),
                start_height,
                amt_node_confirmed_path.key_value_maps,
                &historical_write_schema,
            )?;
            confirm_maps_to_history::<D, SlotAllocations>(
                self.historical_db.clone(),
                start_height,
                slot_alloc_confirmed_path.key_value_maps,
                &historical_write_schema,
            )?;

            self.commit_to_historical_db(historical_write_schema)?;
        } // else: nothing is changed and no records are in pending_write_schema, so skip directly

        Ok(())
    }
}

#[test]
fn test_lvmt_root_leaf_versions() {
    use super::types::AmtId;

    let historical_path = "__test_lvmt_root_leaf_versions_historical";
    let pending_path = "__test_lvmt_root_leaf_versions_pending";
    clear_dir_then_create(historical_path);
    clear_dir_then_create(pending_path);

    let historical_db = Arc::new(WrappedRocksDb::open(historical_path).unwrap());
    let pending_db = Arc::new(WrappedRocksDb::open(pending_path).unwrap());
    let mut db =
        LvmtStorage::<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>::new(
            historical_db,
            pending_db,
        )
        .unwrap();

    let mut rng = get_rng_for_test();
    let mut previous_commits = HashSet::new();
    let mut novel = || gen_novel_commit_id(&mut rng, &mut previous_commits);
    let (c1, c2, c3, c4, c2b, c3b) = (novel(), novel(), novel(), novel(), novel(), novel());

    // c1 -> c2 -> c3 -> c4 (empty), and a fork c1 -> c2b -> c3b. Keys overlap
    // between commits, so some depth-1 AMTs change several times on a path.
    let changes = |range: std::ops::Range<u64>, value: u64| {
        range
            .map(move |k| (u64_to_boxed_u8(k), Some(u64_to_boxed_u8(value))))
            .collect::<Vec<_>>()
    };
    {
        let mut lvmt = db.as_manager().unwrap();
        let mut commit = |old, new, ch: Vec<_>| {
            lvmt.commit(old, new, ch.into_iter(), &AMT).unwrap();
        };
        commit(None, c1, changes(0..300, 1));
        commit(Some(c1), c2, changes(200..500, 2));
        commit(Some(c2), c3, changes(0..100, 3));
        commit(Some(c3), c4, vec![]);
        commit(Some(c1), c2b, changes(250..400, 4));
        commit(Some(c2b), c3b, changes(600..700, 5));
    }

    let check = |db: &mut LvmtStorage<_, _>, commits: &[CommitID]| {
        // With the current map at different commits, which the overlay must ignore.
        for checkout in [None, Some(c2), Some(c3b)] {
            let lvmt = db.as_manager().unwrap();
            if let Some(c) = checkout {
                if lvmt
                    .get_amt_node_store()
                    .query_commit_existence(&c)
                    .unwrap()
                {
                    lvmt.get_state(c, true).unwrap();
                }
            }
            for &c in commits {
                let view = lvmt
                    .get_amt_node_store()
                    .get_versioned_store(&c, false)
                    .unwrap();
                let expected: Vec<u64> = (0..=u16::MAX)
                    .map(|i| {
                        view.get(&AmtId::root().child(i))
                            .unwrap()
                            .map_or(0, |p| p.version)
                    })
                    .collect();
                assert_eq!(lvmt.get_root_leaf_versions(c).unwrap(), expected);
            }
        }
    };

    // All pending, with a fork.
    check(&mut db, &[c1, c2, c3, c4, c2b, c3b]);

    // c1, c2 historical (c2 the latest), c3, c4 pending; the fork is dropped.
    db.confirmed_pending_to_history_with_commit_id(c3).unwrap();
    check(&mut db, &[c1, c2, c3, c4]);
}

/// The invariants behind correcting a checkpoint of root leaf versions by counting
/// AMT changes (see `AmtChangeManager::compute_amt_changes`).
#[test]
fn test_lvmt_root_leaf_version_counting() {
    use super::types::{AmtId, CurvePointWithVersion, SLOT_SIZE};
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use std::collections::BTreeMap;

    let historical_path = "__test_lvmt_root_leaf_version_counting_historical";
    let pending_path = "__test_lvmt_root_leaf_version_counting_pending";
    clear_dir_then_create(historical_path);
    clear_dir_then_create(pending_path);

    let historical_db = Arc::new(WrappedRocksDb::open(historical_path).unwrap());
    let pending_db = Arc::new(WrappedRocksDb::open(pending_path).unwrap());
    let mut db =
        LvmtStorage::<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>::new(
            historical_db,
            pending_db,
        )
        .unwrap();

    // 60 commits of 300 writes over a growing key space (overwrites, new keys and
    // deletions), the first 40 confirmed.
    let mut rng = get_rng_for_test();
    let mut previous_commits = HashSet::new();
    let mut value_rng = StdRng::seed_from_u64(11);
    let mut chain: Vec<CommitID> = vec![];
    let mut next_key = 0u64;
    for _ in 0..60 {
        let changes: Vec<_> = (0..300)
            .map(|_| {
                let k = if next_key == 0 || value_rng.gen_range(0..4) == 0 {
                    next_key += 1;
                    next_key - 1
                } else {
                    value_rng.gen_range(0..next_key)
                };
                let v = (value_rng.gen_range(0..10) > 0).then(|| u64_to_boxed_u8(value_rng.gen()));
                (u64_to_boxed_u8(k), v)
            })
            .collect();
        let id = gen_novel_commit_id(&mut rng, &mut previous_commits);
        let mut lvmt = db.as_manager().unwrap();
        lvmt.commit(chain.last().copied(), id, changes.into_iter(), &AMT)
            .unwrap();
        chain.push(id);
    }
    let confirmed = 40; // chain[..confirmed] historical
    db.confirmed_pending_to_history_with_commit_id(chain[confirmed])
        .unwrap();

    let lvmt = db.as_manager().unwrap();
    let versions: Vec<Vec<u64>> = chain
        .iter()
        .map(|c| lvmt.get_root_leaf_versions(*c).unwrap())
        .collect();

    // Per-leaf count of depth-1 AMT changes in historical commits chain[from+1..=to].
    let changes_of = |c: usize| -> BTreeMap<u16, CurvePointWithVersion> {
        lvmt.get_historical_amt_changes(chain[c])
            .unwrap()
            .into_iter()
            .filter(|(id, _)| id.len() == 1)
            .map(|(id, p)| (id[0], p))
            .collect()
    };
    let count = |from: usize, to: usize| -> Vec<u64> {
        let mut counts = vec![0u64; 1 << 16];
        for c in from + 1..=to {
            for i in changes_of(c).keys() {
                counts[*i as usize] += 1;
            }
        }
        counts
    };

    // (1) Between consecutive commits a version goes up by 0 or 1; in a historical
    // commit, by 1 exactly for the AMTs it changed, to the version written there.
    for c in 1..chain.len() {
        for i in 0..1 << 16 {
            let diff = versions[c][i] - versions[c - 1][i];
            assert!(diff <= 1, "commit {c} leaf {i}: +{diff}");
        }
        if c < confirmed {
            let changed = changes_of(c);
            for i in 0..1 << 16 {
                let expected = match changed.get(&(i as u16)) {
                    Some(p) => {
                        assert_eq!(p.version, versions[c][i]);
                        1
                    }
                    None => 0,
                };
                assert_eq!(versions[c][i] - versions[c - 1][i], expected);
            }
        }
    }

    // (2) Correcting from a checkpoint C to Q in either direction.
    for (q, c) in [(5, 30), (30, 5), (0, 39), (39, 0), (17, 18), (20, 20)] {
        let (lo, hi) = (q.min(c), q.max(c));
        let counts = count(lo, hi);
        let corrected: Vec<u64> = (0..1 << 16)
            .map(|i| {
                if c > q {
                    versions[c][i] - counts[i]
                } else {
                    versions[c][i] + counts[i]
                }
            })
            .collect();
        assert_eq!(corrected, versions[q], "Q = {q}, C = {c}");
    }

    // (3) Slot 5 of root leaf i is the version of AMT [i]: a root-AMT proof built
    // from these versions verifies against the stored root commitment.
    let leaf_value = |version: u64| {
        let mut slots = [0u64; SLOT_SIZE];
        slots[SLOT_SIZE - 1] = version;
        amt::pack_slots(&slots).unwrap()
    };
    for c in [0, 20, confirmed - 1, confirmed, chain.len() - 1] {
        let values: Vec<_> = versions[c]
            .iter()
            .enumerate()
            .filter(|(_, v)| **v > 0)
            .map(|(i, v)| (i, leaf_value(*v)))
            .collect();
        let idx = values[values.len() / 2].0;
        let proof = AMT.prove_sparse(values.iter().copied(), idx);
        let root = lvmt.get_root_commitment(chain[c]).unwrap();
        AMT.verify(
            &root.point.affine(),
            idx,
            leaf_value(versions[c][idx]),
            &proof,
        )
        .unwrap();
        assert_eq!(
            lvmt.get_amt_node_store()
                .get_versioned_store(&chain[c], false)
                .unwrap()
                .get(&AmtId::root().child(idx as u16))
                .unwrap()
                .unwrap()
                .version,
            versions[c][idx]
        );
    }
}

/// Root leaf versions at historical commits read from checkpoints (with the
/// count correction) equal direct point lookups; checkpoints are written at the
/// interval, pruned below the depth, and still used after the interval changes.
#[test]
fn test_lvmt_root_leaf_checkpoints() {
    use super::checkpoint::{ProofConfig, RootLeafCheckpointTable};
    use super::types::AmtId;
    use rand::{rngs::StdRng, Rng, SeedableRng};

    type Db = LvmtStorage<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>;
    let historical_path = "__test_lvmt_root_leaf_checkpoints_historical";
    let pending_path = "__test_lvmt_root_leaf_checkpoints_pending";
    clear_dir_then_create(historical_path);
    clear_dir_then_create(pending_path);

    let historical_db = Arc::new(WrappedRocksDb::open(historical_path).unwrap());
    let pending_db = Arc::new(WrappedRocksDb::open(pending_path).unwrap());
    let mut db = Db::new(historical_db, pending_db).unwrap();
    db.set_proof_config(ProofConfig {
        history_depth: 20,
        checkpoint_interval: 7,
    });

    let mut rng = get_rng_for_test();
    let mut previous_commits = HashSet::new();
    let mut value_rng = StdRng::seed_from_u64(12);
    let mut chain: Vec<CommitID> = vec![];
    let mut grow = |db: &mut Db, chain: &mut Vec<CommitID>, n: usize| {
        for _ in 0..n {
            let changes: Vec<_> = (0..200)
                .map(|_| {
                    let k: u64 = value_rng.gen_range(0..3000);
                    (u64_to_boxed_u8(k), Some(u64_to_boxed_u8(value_rng.gen())))
                })
                .collect();
            let id = gen_novel_commit_id(&mut rng, &mut previous_commits);
            let mut lvmt = db.as_manager().unwrap();
            lvmt.commit(chain.last().copied(), id, changes.into_iter(), &AMT)
                .unwrap();
            chain.push(id);
        }
    };

    let direct = |lvmt: &LvmtStore<'_, '_, _>, c: CommitID| -> Vec<u64> {
        let view = lvmt
            .get_amt_node_store()
            .get_versioned_store(&c, false)
            .unwrap();
        (0..=u16::MAX)
            .map(|i| {
                view.get(&AmtId::root().child(i))
                    .unwrap()
                    .map_or(0, |p| p.version)
            })
            .collect()
    };
    let stored_checkpoints = |db: &Db| -> Vec<u64> {
        db.historical_db
            .view::<RootLeafCheckpointTable>()
            .unwrap()
            .iter_from_start()
            .unwrap()
            .map(|item| item.unwrap().0.into_owned())
            .collect()
    };
    let check_all = |db: &mut Db, chain: &[CommitID]| {
        let expected: Vec<_> = {
            let lvmt = db.as_manager().unwrap();
            chain.iter().map(|c| direct(&lvmt, *c)).collect()
        };
        let lvmt = db.as_manager().unwrap();
        for (h, c) in chain.iter().enumerate() {
            assert_eq!(
                lvmt.get_root_leaf_versions(*c).unwrap(),
                expected[h],
                "height {h}"
            );
        }
        drop(lvmt);
        let reader = db.as_reader().unwrap();
        for (h, c) in chain.iter().enumerate() {
            assert_eq!(
                reader.get_root_leaf_versions(*c).unwrap(),
                expected[h],
                "reader, height {h}"
            );
        }
    };

    // Confirm in rounds of uneven length, so that each round passes 0, 1 or 2
    // checkpoint heights.
    for (grow_by, confirm_to) in [(10, 3), (10, 12), (20, 33), (25, 55)] {
        grow(&mut db, &mut chain, grow_by);
        db.confirmed_pending_to_history_with_commit_id(chain[confirm_to])
            .unwrap();
    }
    // Historical tip at height 54: checkpoints at multiples of 7 from
    // 54 - 20 - 7 = 27 on.
    assert_eq!(stored_checkpoints(&db), vec![28, 35, 42, 49]);
    check_all(&mut db, &chain);

    // A new interval: the old checkpoints stay usable until pruned, new ones are
    // written at multiples of 5.
    db.set_proof_config(ProofConfig {
        history_depth: 20,
        checkpoint_interval: 5,
    });
    grow(&mut db, &mut chain, 10);
    db.confirmed_pending_to_history_with_commit_id(chain[62])
        .unwrap();
    // Tip at 61: prune below 61 - 25 = 36.
    assert_eq!(stored_checkpoints(&db), vec![42, 49, 55, 60]);
    check_all(&mut db, &chain);
}

/// `hroot` of every commit equals the MMR root recomputed from its ancestors'
/// state roots, across the historical and pending parts and on a fork, and MMR
/// proofs verify for ancestors only.
#[test]
fn test_lvmt_hroot() {
    use super::mmr::{bag_peaks, MmrEntry};
    use rand::{rngs::StdRng, Rng, SeedableRng};

    type Db = LvmtStorage<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>;
    let historical_path = "__test_lvmt_hroot_historical";
    let pending_path = "__test_lvmt_hroot_pending";
    clear_dir_then_create(historical_path);
    clear_dir_then_create(pending_path);
    let historical_db = Arc::new(WrappedRocksDb::open(historical_path).unwrap());
    let pending_db = Arc::new(WrappedRocksDb::open(pending_path).unwrap());
    let mut db = Db::new(historical_db, pending_db).unwrap();

    let mut rng = get_rng_for_test();
    let mut previous_commits = HashSet::new();
    let mut value_rng = StdRng::seed_from_u64(13);
    let mut commit = |db: &mut Db, parent: Option<CommitID>| -> CommitID {
        let changes: Vec<_> = (0..20)
            .map(|_| {
                let k: u64 = value_rng.gen_range(0..500);
                (u64_to_boxed_u8(k), Some(u64_to_boxed_u8(value_rng.gen())))
            })
            .collect();
        let id = gen_novel_commit_id(&mut rng, &mut previous_commits);
        let mut lvmt = db.as_manager().unwrap();
        lvmt.commit(parent, id, changes.into_iter(), &AMT).unwrap();
        id
    };

    let mut main: Vec<CommitID> = vec![];
    for _ in 0..40 {
        let parent = main.last().copied();
        main.push(commit(&mut db, parent));
    }
    // A fork off height 25.
    let mut fork: Vec<CommitID> = main[..=25].to_vec();
    for _ in 0..6 {
        let parent = fork.last().copied();
        fork.push(commit(&mut db, parent));
    }

    let check = |db: &mut Db, chain: &[CommitID], other: &[CommitID]| {
        let lvmt = db.as_manager().unwrap();
        let roots: Vec<_> = chain
            .iter()
            .map(|c| lvmt.get_state_root(*c).unwrap().unwrap())
            .collect();
        let mut peaks: Vec<_> = vec![];
        for (h, c) in chain.iter().enumerate() {
            assert_eq!(
                lvmt.get_hroot(*c).unwrap(),
                bag_peaks(h as u64, &peaks),
                "height {h}"
            );
            peaks = MmrEntry::append(&peaks, h as u64, &roots[h]).peaks;
        }
        for q in [1usize, 2, 7, 16, 20, 21, 30, chain.len() - 1] {
            let hroot = lvmt.get_hroot(chain[q]).unwrap();
            for a in 0..q {
                let proof = lvmt.prove_hroot_leaf(chain[q], chain[a]).unwrap();
                assert!(proof.verify(&hroot, &roots[a]), "q = {q}, a = {a}");
            }
            assert!(lvmt.prove_hroot_leaf(chain[q], chain[q]).is_err());
            assert!(lvmt.prove_hroot_leaf(chain[0], chain[q]).is_err());
        }
        // Commits of the other branch past the fork point are not ancestors.
        assert!(lvmt
            .prove_hroot_leaf(*chain.last().unwrap(), other[27])
            .is_err());
    };

    check(&mut db, &main, &fork);
    check(&mut db, &fork, &main);

    // Ancestors move to the historical part.
    db.confirmed_pending_to_history_with_commit_id(main[20])
        .unwrap();
    check(&mut db, &main, &fork);
    check(&mut db, &fork, &main);

    // Past the fork point: the fork is discarded.
    db.confirmed_pending_to_history_with_commit_id(main[30])
        .unwrap();
    let lvmt = db.as_manager().unwrap();
    let hroot = lvmt.get_hroot(main[39]).unwrap();
    for a in 0..39 {
        let proof = lvmt.prove_hroot_leaf(main[39], main[a]).unwrap();
        let root = lvmt.get_state_root(main[a]).unwrap().unwrap();
        assert!(proof.verify(&hroot, &root), "a = {a}");
    }
}

/// The state root, auth-change tree and MMR entry of commits on discarded forks
/// are deleted, by `make_pivot` and by confirmation; those of kept commits stay.
#[test]
fn test_lvmt_discarded_commit_rows() {
    use super::types::AuthChangeKey;

    type Db = LvmtStorage<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>;
    let historical_path = "__test_lvmt_discarded_commit_rows_historical";
    let pending_path = "__test_lvmt_discarded_commit_rows_pending";
    clear_dir_then_create(historical_path);
    clear_dir_then_create(pending_path);
    let historical_db = Arc::new(WrappedRocksDb::open(historical_path).unwrap());
    let pending_db = Arc::new(WrappedRocksDb::open(pending_path).unwrap());
    let mut db = Db::new(historical_db, pending_db).unwrap();

    let mut rng = get_rng_for_test();
    let mut previous_commits = HashSet::new();
    let mut next = 0u64;
    let mut commit = |db: &mut Db, parent: Option<CommitID>| -> CommitID {
        next += 1;
        let changes = (0..5).map(|i| (u64_to_boxed_u8(next * 10 + i), Some(u64_to_boxed_u8(i))));
        let id = gen_novel_commit_id(&mut rng, &mut previous_commits);
        let mut lvmt = db.as_manager().unwrap();
        lvmt.commit(parent, id, changes, &AMT).unwrap();
        id
    };
    let mut chain_from = |db: &mut Db, parent: Option<CommitID>, n: usize| -> Vec<CommitID> {
        let mut out = vec![];
        for _ in 0..n {
            let p = out.last().copied().or(parent);
            out.push(commit(db, p));
        }
        out
    };
    let has_rows = |db: &mut Db, c: CommitID| -> [bool; 3] {
        let lvmt = db.as_manager().unwrap();
        [
            lvmt.get_state_root(c).unwrap().is_some(),
            lvmt.get_mmr_entry(c).unwrap().is_some(),
            lvmt.get_auth_change_node(c, &AuthChangeKey::root())
                .unwrap()
                .is_some(),
        ]
    };

    let main = chain_from(&mut db, None, 10);
    let fork_a = chain_from(&mut db, Some(main[3]), 4);
    let fork_b = chain_from(&mut db, Some(main[6]), 2);
    for c in main.iter().chain(&fork_a).chain(&fork_b) {
        assert_eq!(has_rows(&mut db, *c), [true; 3]);
    }

    assert!(db.as_manager().unwrap().make_pivot(main[8]).unwrap());
    for c in fork_a.iter().chain(&fork_b) {
        assert_eq!(has_rows(&mut db, *c), [false; 3]);
    }
    for c in &main {
        assert_eq!(has_rows(&mut db, *c), [true; 3]);
    }

    let fork_c = chain_from(&mut db, Some(main[7]), 3);
    db.confirmed_pending_to_history_with_commit_id(main[9])
        .unwrap();
    for c in &fork_c {
        assert_eq!(has_rows(&mut db, *c), [false; 3]);
    }
    for c in &main {
        assert_eq!(has_rows(&mut db, *c), [true; 3]);
        db.as_manager()
            .unwrap()
            .check_consistency(*c, &AMT)
            .unwrap();
    }
}
