#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use tempfile::tempdir;
use std::fs;
use std::sync::Arc;

//==============================================================================
// 1. MOCK IMPLEMENTATION (THE "MODEL")
//    This is a simplified, correct-by-construction model of the system under
//    test. The fuzzer will compare the behavior of the real system against this model.
//==============================================================================

use cfx_storage2::middlewares::versioned_flat_key_value::tests::{MockVersionedStore, CommitIDType, UniqueVec};
use cfx_storage2::middlewares::CommitID; // H256
use cfx_storage2::middlewares::table_schema::VersionedKeyValueSchema;
use cfx_storage2::backends::{VersionedKVName, DatabaseTrait, PendingTableName, HistoricalTableName};
use cfx_storage2::backends::impls::kvdb_rocksdb::WrappedRocksDb;
use cfx_storage2::traits::KeyValueStoreManager;
use cfx_storage2::backends::serde::{Encode, Decode};
use once_cell::sync::Lazy;
use amt::{AmtParams, CreateMode};
use ethereum_types::H256;
use cfx_storage2::lvmt::crypto::PE;
use cfx_storage2::LvmtStorage;

//==============================================================================
// 2. HELPER & DATA STRUCTURES
//    These are the basic building blocks for our keys and values.
//==============================================================================

#[path = "bounded_vec.rs"]
mod bounded_vec;
use bounded_vec::BoundedVec;

fn get_commit_id_from_u64(val: u64) -> CommitID {
    let mut bytes = [0u8; 32];
    // Place the u64 bytes at the end of the array for simplicity. This is sufficient
    // to guarantee uniqueness for 2^64 commits.
    bytes[24..].copy_from_slice(&val.to_be_bytes());
    CommitID::from_slice(&bytes)
}

//==============================================================================
// 2. FUZZER INPUT SPECIFICATION
//    These structs define the "language" of our fuzzer. They describe
//    the *intent* of an operation, not the concrete data.
//==============================================================================

/// A configurable constant for the size of the freshness pool.
const FRESHNESS_POOL_SIZE: usize = 20;
/// A bound for modulo operations to prevent picking from a huge, sparse space.
const MODULO_BOUND: usize = 200;

/// Defines the specification for selecting a key. This is the core of the corrected design.
/// We've added a `UseFresh` variant to introduce a bias towards recently generated keys.
#[derive(Arbitrary, Debug, Clone)]
#[arbitrary(bound = "K: for<'a> Arbitrary<'a>")]
enum KeySpec<K>
{
    /// Intent: Use a key from the "freshness pool" (i.e., one that has been seen recently).
    /// This helps find bugs related to recent state changes.
    UseFresh(#[arbitrary(with = |u: &mut Unstructured| u.int_in_range(0..=FRESHNESS_POOL_SIZE))] usize),

    /// Intent: Use any key that has been seen before from the entire history.
    /// The `usize` will be used as an index into a runtime vector of all known keys.
    UseExisting(#[arbitrary(with = |u: &mut Unstructured| u.int_in_range(0..=MODULO_BOUND))] usize),

    /// Intent: Generate a completely new, random key.
    /// The `K` is generated directly by the `arbitrary` crate, sampling from its
    /// entire possible value space. This allows for discovering random collisions.
    GenerateNew(K),
}

/// A fuzzer-driven specification for selecting a commit ID. The `usize` is used
/// as a pseudo-random index into the available commits of the specified type.
#[derive(Arbitrary, Debug, Clone)]
enum CommitIDSpec {
    History(#[arbitrary(with = |u: &mut Unstructured| u.int_in_range(0..=MODULO_BOUND))] usize),
    PendingRoot, // Only one root, so no index needed
    PendingNonRoot(#[arbitrary(with = |u: &mut Unstructured| u.int_in_range(0..=MODULO_BOUND))] usize),
    Novel,
}

/// A fuzzer-driven specification for selecting a parent commit ID.
#[derive(Arbitrary, Debug, Clone)]
enum ParentCommitSpec {
    Pending(#[arbitrary(with = |u: &mut Unstructured| u.int_in_range(0..=MODULO_BOUND))] usize),
    ParentOfPendingRoot,
    None,
    HistoryButInvalid(#[arbitrary(with = |u: &mut Unstructured| u.int_in_range(0..=MODULO_BOUND))] usize),
    Novel,
}

/// Represents a set of changes to be included in a single commit.
#[derive(Arbitrary, Debug, Clone)]
#[arbitrary(bound = "K: for<'a> Arbitrary<'a>, V: for<'a> Arbitrary<'a>")]
struct FuzzUpdate<K, V> {
    changes: Vec<(KeySpec<K>, Option<V>)>,
}

/// Represents a high-level operation to be performed.
#[derive(Arbitrary, Debug, Clone)]
#[arbitrary(bound = "K: for<'a> Arbitrary<'a>, V: for<'a> Arbitrary<'a>")]
enum FuzzOperation<K, V> {
    AddToPending {
        parent_spec: ParentCommitSpec,
        update: FuzzUpdate<K, V>,
    },
    Discard {
        commit_spec: CommitIDSpec,
    },
    Confirm {
        commit_spec: CommitIDSpec,
    },
    GetVersionedKey {
        commit_spec: CommitIDSpec,
        key_spec: KeySpec<K>,
    },
}

/// The top-level input structure for a single fuzzing run.
/// It contains a sequence of instructions to build and interact with the state.
#[derive(Arbitrary, Debug)]
#[arbitrary(bound = "K: for<'a> Arbitrary<'a>, V: for<'a> Arbitrary<'a>")]
struct FuzzInput<K, V> {
    /// A sequence of operations to perform after the initial history is established.
    operations: Vec<FuzzOperation<K, V>>,
}

//==============================================================================
// 4. FUZZ TARGET EXECUTION LOGIC
//==============================================================================
/// Type alias for the key type we are testing.
type TestKey = BoundedVec;
/// Type alias for the value type we are testing.
type TestValue = u64;

#[derive(Clone, Copy, Debug)]
struct TestSchema;

impl VersionedKeyValueSchema for TestSchema {
    const NAME: VersionedKVName = VersionedKVName::FlatKV;
    type Key = TestKey;
    type Value = TestValue;
}

/// Helper function to resolve a `FuzzUpdate` into a concrete `HashMap` of changes.
/// It populates the `known_keys` and `fresh_keys` vectors as a side effect.
fn resolve_update<T: VersionedKeyValueSchema>(
    update: &FuzzUpdate<T::Key, T::Value>,
    known_keys: &mut Vec<T::Key>,
    fresh_keys: &mut VecDeque<T::Key>, // Now also takes a mutable reference to the fresh pool
) -> HashMap<T::Key, Option<T::Value>> {
    let mut changes_map = HashMap::new();
    for (key_spec, value) in &update.changes {
        let key_opt = match key_spec {
            KeySpec::GenerateNew(new_key) => {
                // The fuzzer provided a brand new key.
                // Add it to the total list of known keys.
                known_keys.push(new_key.clone());
                
                // Also add it to the front of the freshness pool.
                fresh_keys.push_front(new_key.clone());
                // If the pool is too large, remove the oldest element from the back.
                if fresh_keys.len() > FRESHNESS_POOL_SIZE {
                    fresh_keys.pop_back();
                }
                
                Some(new_key.clone())
            }
            KeySpec::UseFresh(idx) => {
                if fresh_keys.is_empty() {
                    // Fallback to the general pool if the fresh pool is empty.
                    if known_keys.is_empty() { None } 
                    else { Some(known_keys[*idx % known_keys.len()].clone()) }
                } else {
                    // Safely select a key from the fresh pool.
                    Some(fresh_keys[*idx % fresh_keys.len()].clone())
                }
            }
            KeySpec::UseExisting(idx) => {
                if known_keys.is_empty() {
                    // There are no keys to choose from.
                    None
                } else {
                    // Safely select a key from the general pool.
                    Some(known_keys[*idx % known_keys.len()].clone())
                }
            }
        };

        if let Some(key) = key_opt {
            changes_map.insert(key, value.clone());
        }
    }
    changes_map
}

pub fn resolve_changes<T: VersionedKeyValueSchema>(
    update: HashMap<T::Key, Option<T::Value>>,
) -> impl Iterator<Item = (Box<[u8]>, Option<Box<[u8]>>)> {
    update.into_iter().map(|(key, opt_val)| {
        let key_bytes: Box<[u8]> = key.encode().into_owned().into_boxed_slice();
        let val_bytes_opt = opt_val.map(|v| v.encode().into_owned().into_boxed_slice());
        (key_bytes, val_bytes_opt)
    })
}

/// Resolves a `CommitIDSpec` into a concrete `CommitID` and its `CommitIDType`.
fn resolve_commit_id<T, P>(
    spec: &CommitIDSpec,
    mock_store: &MockVersionedStore<T, P>,
    commit_counter: &mut u64,
) -> (Option<CommitID>, CommitIDType)
where
    T: VersionedKeyValueSchema,
    P: DatabaseTrait<PendingTableName>,
{
    match spec {
        CommitIDSpec::History(idx) => {
            let history = mock_store.get_history();
            if history.is_empty() {
                (None, CommitIDType::History)
            } else {
                (Some(history[*idx % history.len()]), CommitIDType::History)
            }
        }
        CommitIDSpec::PendingRoot => {
            let root = mock_store.get_pending_root();
            if root.is_empty() {
                (None, CommitIDType::PendingRoot)
            } else {
                (Some(root[0]), CommitIDType::PendingRoot)
            }
        }
        CommitIDSpec::PendingNonRoot(idx) => {
            let non_root = mock_store.get_pending_non_root();
            if non_root.is_empty() {
                (None, CommitIDType::PendingNonRoot)
            } else {
                (Some(non_root[*idx % non_root.len()]), CommitIDType::PendingNonRoot)
            }
        }
        CommitIDSpec::Novel => {
            *commit_counter += 1;
            (Some(get_commit_id_from_u64(*commit_counter)), CommitIDType::Novel)
        }
    }
}

/// Resolves a `ParentCommitSpec` into a concrete parent `CommitID`.
fn resolve_parent_commit_id<T, P>(
    spec: &ParentCommitSpec,
    mock_store: &MockVersionedStore<T, P>,
    commit_counter: &mut u64,
) -> Option<CommitID>
where
    T: VersionedKeyValueSchema,
    P: DatabaseTrait<PendingTableName>,
{
    match spec {
        ParentCommitSpec::Pending(idx) => {
            let pending = mock_store.get_pending();
            if pending.is_empty() {
                None
            } else {
                Some(pending[*idx % pending.len()])
            }
        }
        ParentCommitSpec::ParentOfPendingRoot =>
            mock_store.get_parent_of_root(),
        ParentCommitSpec::None => None,
        ParentCommitSpec::HistoryButInvalid(idx) => {
            let history = mock_store.get_history_but_parent_of_root();
            if history.is_empty() {
                None
            } else {
                Some(history[*idx % history.len()])
            }
        }
        ParentCommitSpec::Novel => {
            *commit_counter += 1;
            Some(get_commit_id_from_u64(*commit_counter))
        }
    }
}

fn get_lvmt_key_from_key<T: VersionedKeyValueSchema>(
    key: T::Key,
) -> Box<[u8]> {
    key.encode().into_owned().into_boxed_slice()
}

fn get_value_from_lvmt_value<T: VersionedKeyValueSchema>(
    lvmt_value: Option<Box<[u8]>>,
) -> Option<T::Value> {
    lvmt_value.map(|b| {
        // Box<[u8]> -> Vec<u8> (no clone)
        T::Value::decode_owned(b.into_vec())
            .expect("decode_owned should not fail")
    })
}

pub const TEST_LEVEL: usize = 16;
pub static AMT: Lazy<AmtParams<PE>> = Lazy::new(|| {
    AmtParams::from_dir_mont("./pp", TEST_LEVEL, TEST_LEVEL, CreateMode::Neither, None)
});

fuzz_target!(|input: FuzzInput<TestKey, TestValue>| {
    // --- SETUP ---
    let mut mock_store: MockVersionedStore<TestSchema, WrappedRocksDb<PendingTableName>> = MockVersionedStore::build(UniqueVec::new(), vec![]);

    let temp_root_dir = tempdir().expect("Failed to create temporary directory");

    let historical_path = temp_root_dir.path().join("historical");
    let pending_path = temp_root_dir.path().join("pending");

    fs::create_dir(&historical_path).expect("Failed to create historical subdirectory");
    fs::create_dir(&pending_path).expect("Failed to create pending subdirectory");

    let historical_db = Arc::new(WrappedRocksDb::open(&historical_path).unwrap());
    let pending_db = Arc::new(WrappedRocksDb::open(&pending_path).unwrap());

    let mut db = LvmtStorage::<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>::new(historical_db.clone(), pending_db.clone()).unwrap();
    let mut real_store = db.as_manager().unwrap();

    // Runtime state for the fuzzer to manage its resources.
    let mut commit_counter: u64 = 0;
    let mut known_keys: Vec<TestKey> = Vec::new();
    // NEW: The freshness pool for recently generated keys.
    let mut fresh_keys: VecDeque<TestKey> = VecDeque::with_capacity(FRESHNESS_POOL_SIZE);

    // --- Perform Operations ---
    for op in &input.operations {
        match op {
            FuzzOperation::AddToPending { parent_spec, update } => {
                let parent_commit =
                    resolve_parent_commit_id(parent_spec, &mock_store, &mut commit_counter);

                let updates = resolve_update::<TestSchema>(update, &mut known_keys, &mut fresh_keys);
                
                commit_counter += 1;
                let new_commit_id = get_commit_id_from_u64(commit_counter);

                let mock_res = mock_store.add_to_pending_part(parent_commit, new_commit_id, updates.clone());
                // The commit function includes initializing pending_write_schema and committing it to pending_db.
                let real_res = real_store.commit(
                    parent_commit,
                    new_commit_id,
                    H256::zero(), // state_root is not used in the test case, so just provide a toy
                    resolve_changes::<TestSchema>(updates),
                    &AMT,
                );

                assert_eq!(mock_res.is_ok(), real_res.is_ok(), "AddToPending results must match");
            }

            // TODO: make_pivot
            FuzzOperation::Discard { commit_spec } => {
                ()
            }
            //     let (commit_id_opt, _) = resolve_commit_id(commit_spec, &mock_store, &mut commit_counter);
            //     if let Some(commit_id) = commit_id_opt {
            //         let pending_ws = WrappedRocksDb::write_schema();
            //         let mock_res = mock_store.discard(commit_id, &pending_ws);
            //         let real_res = real_store.discard(commit_id, &pending_ws);
            //         pending_db.commit(pending_ws).unwrap();
                    
            //         assert_eq!(mock_res, real_res, "Discard results must match");
            //     }
            // }

            FuzzOperation::Confirm { commit_spec } => {
                let (commit_id_opt, _) = resolve_commit_id(commit_spec, &mock_store, &mut commit_counter);
                if let Some(commit_id) = commit_id_opt {
                    let mock_res = mock_store.confirmed_pending_to_history(commit_id);

                    drop(real_store);
                    let real_res = db.confirmed_pending_to_history_with_commit_id(commit_id);
                    
                    real_store = db.as_manager().unwrap();
                    if real_res.is_ok() {
                        real_store.check_consistency(commit_id, &AMT).unwrap();
                    }
                    
                    assert_eq!(mock_res.is_ok(), real_res.is_ok(), "Confirm results must match");
                }
            }

            FuzzOperation::GetVersionedKey { commit_spec, key_spec } => {
                let (commit_id_opt, _) = resolve_commit_id(commit_spec, &mock_store, &mut commit_counter);
                if let Some(commit_id) = commit_id_opt {
                    let key_to_get_opt = match key_spec {
                        KeySpec::GenerateNew(key) => Some(key.clone()),
                        KeySpec::UseFresh(idx) => {
                            if fresh_keys.is_empty() { None } 
                            else { Some(fresh_keys[*idx % fresh_keys.len()].clone()) }
                        }
                        KeySpec::UseExisting(idx) => {
                            if known_keys.is_empty() { None } 
                            else { Some(known_keys[*idx % known_keys.len()].clone()) }
                        }
                    };

                    if let Some(key_to_get) = key_to_get_opt {
                        let mock_res = mock_store.get_versioned_key(&commit_id, &key_to_get);
                        let real_res = real_store.get(commit_id, get_lvmt_key_from_key::<TestSchema>(key_to_get));
                        
                        match real_res {
                            Ok(opt_lvmt) => {
                                let real_inner = if let Some(lvmt_v) = opt_lvmt {
                                    // get opt_bytes from LvmtValue
                                    let opt_bytes = lvmt_v.get_value();

                                    // get Option<T::Value> from opt_bytes
                                    get_value_from_lvmt_value::<TestSchema>(opt_bytes)

                                } else { // commit_id exists, but this key is not in the snapshot of this commit_id
                                    None
                                };
                                
                                match mock_res {
                                    Ok(mock_inner) => {
                                        assert_eq!(
                                            mock_inner,
                                            real_inner,
                                            "mock_store.get_versioned_key vs real_store.get mismatch after normalization"
                                        );
                                    }
                                    Err(mock_err) => {
                                        panic!(
                                            "mock_store errored while real_store succeeded: {mock_err:?}"
                                        );
                                    }
                                }
                            }
                            Err(real_err) => {
                                match mock_res {
                                    Ok(_) => {
                                        panic!(
                                            "mock_store succeeded while real_store errored: {real_err:?}"
                                        );
                                    }
                                    Err(mock_err) => {
                                        assert_eq!(
                                            mock_err,
                                            real_err,
                                            "mock_store.get_versioned_key vs real_store.get mismatch for failure cases"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        // After any potential state change, check that the mock store's invariants still hold.
        mock_store.check_consistency();
    }
});