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
        assert_eq!(
            state_root,
            compute_state_root(&root_commitment, &auth_change_root),
            "Inconsistent state root"
        );
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
