use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use ethereum_types::H256;
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

use super::{crypto::PE, example::LvmtStorage, storage::LvmtStore};

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
struct TestSetup {
    commit_1: CommitID,
    commit_2: CommitID,
    commit_2_1: CommitID,
    commit_3: CommitID,
    updates_1: Vec<(u64, Option<u64>)>,
    updates_2: Vec<(u64, Option<u64>)>,
    updates_2_1: Vec<(u64, Option<u64>)>,
    updates_3: Vec<(u64, Option<u64>)>,
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

    fn changes_iter(
        updates: &[(u64, Option<u64>)],
    ) -> impl Iterator<Item = (Box<[u8]>, Option<Box<[u8]>>)> + '_ {
        updates
            .iter()
            .map(|(k, v)| (u64_to_boxed_u8(*k), v.map(u64_to_boxed_u8)))
    }
}

/// Executes the first phase of the test: performing a series of commit operations.
fn run_phase_1<D: DatabaseTrait<HistoricalTableName>, P: DatabaseTrait<PendingTableName>>(
    db: &mut LvmtStorage<D, P>,
    setup: &TestSetup,
) {
    // Get a manager for db
    let mut lvmt = db.as_manager().unwrap();

    // Perform non-forking commits
    lvmt.commit(
        None,
        setup.commit_1,
        H256::zero(), // state_root is not used in the test case, so just provide a toy
        TestSetup::changes_iter(&setup.updates_1),
        &AMT,
    )
    .unwrap();
    lvmt.check_consistency(setup.commit_1, &AMT).unwrap();

    lvmt.commit(
        Some(setup.commit_1),
        setup.commit_2,
        H256::zero(),
        TestSetup::changes_iter(&setup.updates_2),
        &AMT,
    )
    .unwrap();
    lvmt.check_consistency(setup.commit_2, &AMT).unwrap();

    // Perform a forking commit
    lvmt.commit(
        Some(setup.commit_1),
        setup.commit_2_1,
        H256::zero(),
        TestSetup::changes_iter(&setup.updates_2_1),
        &AMT,
    )
    .unwrap();
    lvmt.check_consistency(setup.commit_2_1, &AMT).unwrap();

    // Check the previous commit again after adding subsequent commits
    lvmt.check_consistency(setup.commit_1, &AMT).unwrap();
}

/// Verifies the state after a successful root promotion.
fn run_verification_after_successful_promotion<
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
        H256::zero(),
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
fn run_verification_after_failed_promotion<
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

fn get_setup(num_keys: usize) -> Cow<'static, TestSetup> {
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
enum ConstructionMethod {
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
        H256::zero(),
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
    lvmt.commit(None, commit_id, H256::zero(), std::iter::empty(), &AMT)
        .unwrap();

    lvmt.check_consistency(commit_id, &AMT).unwrap();

    clear_dir(historical_path);
    clear_dir(pending_path);
}

impl<'cache, 'db, P: DatabaseTrait<PendingTableName>> LvmtStore<'cache, 'db, P> {
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
            if amt_id.len() > 0 {
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
    pub(crate) fn make_historical_db_ahead_for_test(
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
