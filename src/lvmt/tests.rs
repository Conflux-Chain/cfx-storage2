use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use ethereum_types::H256;
use once_cell::sync::Lazy;
use rand_chacha::ChaChaRng;

use amt::{AmtParams, CreateMode};

use crate::{
    backends::{DatabaseTrait, InMemoryDatabase},
    errors::Result,
    lvmt::types::{LvmtValue, KEY_SLOT_SIZE},
    middlewares::{
        confirmed_pending_to_history, empty_rocksdb, gen_random_commit_id, gen_updates,
        get_rng_for_test, KeyValueStoreBulks, VersionedStore, VersionedStoreCache,
    },
    traits::{KeyValueStoreManager, KeyValueStoreRead},
};

use super::{
    auth_changes::AuthChangeTable,
    storage::LvmtStore,
    table_schema::{AmtNodes, FlatKeyValue, SlotAllocations},
};

pub struct Storage<D: DatabaseTrait> {
    backend: D,
    key_value_cache: VersionedStoreCache<FlatKeyValue>,
    amt_node_cache: VersionedStoreCache<AmtNodes>,
    slot_alloc_cache: VersionedStoreCache<SlotAllocations>,
}

impl<D: DatabaseTrait> Storage<D> {
    pub fn new(backend: D) -> Result<Self> {
        Ok(Self {
            backend,
            key_value_cache: VersionedStoreCache::new_empty(),
            amt_node_cache: VersionedStoreCache::new_empty(),
            slot_alloc_cache: VersionedStoreCache::new_empty(),
        })
    }

    pub fn as_manager(&mut self) -> Result<LvmtStore<'_, '_>> {
        let key_value_store = VersionedStore::new(&self.backend, &mut self.key_value_cache)?;
        let amt_node_store = VersionedStore::new(&self.backend, &mut self.amt_node_cache)?;
        let slot_alloc_store = VersionedStore::new(&self.backend, &mut self.slot_alloc_cache)?;
        let auth_changes =
            KeyValueStoreBulks::new(Arc::new(self.backend.view::<AuthChangeTable>()?));

        Ok(LvmtStore::new(
            key_value_store,
            amt_node_store,
            slot_alloc_store,
            auth_changes,
        ))
    }

    pub fn commit_auth(&mut self, write_schema: <D as DatabaseTrait>::WriteSchema) -> Result<()> {
        self.backend.commit(write_schema)
    }

    pub fn confirmed_pending_to_history(&mut self, new_root_commit_id: H256) -> Result<()> {
        confirmed_pending_to_history(
            &mut self.backend,
            &mut self.key_value_cache,
            new_root_commit_id,
            true,
        )?;

        confirmed_pending_to_history(
            &mut self.backend,
            &mut self.amt_node_cache,
            new_root_commit_id,
            false,
        )?;

        confirmed_pending_to_history(
            &mut self.backend,
            &mut self.slot_alloc_cache,
            new_root_commit_id,
            false,
        )?;

        Ok(())
    }
}

fn u64_to_boxed_u8(value: u64) -> Box<[u8]> {
    let byte_array: [u8; 8] = value.to_ne_bytes();

    byte_array.into()
}

fn option_u64_to_boxed_u8(opt: Option<u64>) -> Box<[u8]> {
    match opt {
        Some(value) => u64_to_boxed_u8(value),
        None => Box::new([]),
    }
}

pub const TEST_LEVEL: usize = 16;
pub const TEST_LENGTH: usize = 1 << TEST_LEVEL;

pub type PE = ark_bls12_381::Bls12_381;

pub static AMT: Lazy<AmtParams<PE>> =
    Lazy::new(|| AmtParams::from_dir_mont("./pp", TEST_LEVEL, TEST_LEVEL, CreateMode::Both, None));

fn get_changes_from_updates(
    updates: BTreeMap<u64, Option<u64>>,
) -> impl Iterator<Item = (Box<[u8]>, Box<[u8]>)> {
    updates
        .into_iter()
        .map(|(k, v)| (u64_to_boxed_u8(k), option_u64_to_boxed_u8(v)))
}

fn gen_novel_commit_id(rng: &mut ChaChaRng, previous: &mut HashSet<H256>) -> H256 {
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

// `num_keys` = 8 * 10^6 has been tested, but still contain no amt_node_id whose depth > 1
fn test_lvmt_store<D: DatabaseTrait>(backend: D, num_keys: usize) {
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
    let updates_1 = gen_updates(&mut rng, &previous_keys, num_keys, 0, &mut all_keys);

    let previous_keys = all_keys.clone();
    let mut all_keys_2_1 = all_keys.clone();
    let updates_2 = gen_updates(&mut rng, &previous_keys, num_keys, num_keys, &mut all_keys);

    let updates_2_1 = gen_updates(
        &mut rng,
        &previous_keys,
        num_keys,
        num_keys,
        &mut all_keys_2_1,
    );

    let previous_keys = all_keys.clone();
    let updates_3 = gen_updates(&mut rng, &previous_keys, num_keys, num_keys, &mut all_keys);

    let changes_1 = get_changes_from_updates(updates_1);
    let changes_2 = get_changes_from_updates(updates_2);
    let changes_2_1 = get_changes_from_updates(updates_2_1);
    let changes_3 = get_changes_from_updates(updates_3);

    // Initialize db
    let mut db = Storage::<D>::new(backend).unwrap();

    // Get a manager for db
    let mut lvmt = db.as_manager().unwrap();
    let write_schema = D::write_schema();

    // Perform non-forking commits
    lvmt.commit(None, commit_1, changes_1, &write_schema, &AMT)
        .unwrap();
    lvmt.check_consistency(commit_1, &AMT).unwrap();

    lvmt.commit(Some(commit_1), commit_2, changes_2, &write_schema, &AMT)
        .unwrap();
    lvmt.check_consistency(commit_2, &AMT).unwrap();

    // Perform a forking commit
    lvmt.commit(Some(commit_1), commit_2_1, changes_2_1, &write_schema, &AMT)
        .unwrap();
    lvmt.check_consistency(commit_2_1, &AMT).unwrap();

    // Check the previous commit again after adding subsequent commits
    lvmt.check_consistency(commit_1, &AMT).unwrap();

    // Persist confirmed commits from caches to the backend.
    // Must drop the manager first because it holds a read reference to the backend.
    drop(lvmt);
    db.commit_auth(write_schema).unwrap();
    db.confirmed_pending_to_history(commit_2).unwrap();

    // Reinitialize the manager
    lvmt = db.as_manager().unwrap();
    let write_schema = D::write_schema();

    // Commit again to verify success after persisting changes to the backend
    lvmt.commit(Some(commit_2), commit_3, changes_3, &write_schema, &AMT)
        .unwrap();
    lvmt.check_consistency(commit_3, &AMT).unwrap();

    // Check previous commits again after they are confirmed or removed
    lvmt.check_consistency(commit_2, &AMT).unwrap();
    lvmt.check_consistency(commit_1, &AMT).unwrap();
    lvmt.check_consistency(commit_2_1, &AMT).unwrap_err();
}

#[test]
#[ignore]
fn test_lvmt_store_rocksdb() {
    let db_path = "__test_lvmt_store";

    let backend = empty_rocksdb(db_path).unwrap();
    test_lvmt_store::<kvdb_rocksdb::Database>(backend, 100000);

    if std::path::Path::new(db_path).exists() {
        std::fs::remove_dir_all(db_path).unwrap();
    }
}

#[test]
#[ignore]
fn test_lvmt_store_inmemory() {
    let backend = InMemoryDatabase::empty();
    test_lvmt_store::<InMemoryDatabase>(backend, 100000);
}

impl<'cache, 'db> LvmtStore<'cache, 'db> {
    pub fn check_consistency(&mut self, commit: H256, pp: &AmtParams<PE>) -> Result<()> {
        use std::collections::BTreeSet;

        use ark_ec::CurveGroup;

        use crate::lvmt::{
            crypto::{FrInt, VariableBaseMSM, G1},
            types::SLOT_SIZE,
        };

        let amt_node_view = self.get_amt_node_store().get_versioned_store(&commit)?;
        let slot_alloc_view = self.get_slot_alloc_store().get_versioned_store(&commit)?;
        let key_value_view = self.get_key_value_store().get_versioned_store(&commit)?;

        // For each Amt tree (except the children of the root Amt),
        // the leaf node with the same `AmtId` in its parent Amt tree must be fully allocated.
        // The expection of the children of the root Amt is due to the design that the root Amt does not allocate slots.
        let amt_node_iter = amt_node_view.iter()?;
        for (amt_id, curve_point_with_version) in amt_node_iter {
            if amt_id.len() > 1 {
                let amt_node_id = amt_id;
                let alloc_key_info = slot_alloc_view.get(&amt_node_id)?.unwrap();
                assert_eq!(alloc_key_info.index as usize, KEY_SLOT_SIZE - 1);
            }

            assert_ne!(
                curve_point_with_version,
                crate::types::ValueEntry::Deleted,
                "amt node should not contain deletion"
            );
        }

        // Each Amt node with allocated slots should be in an Amt tree
        let slot_alloc_iter = slot_alloc_view.iter()?;
        for (amt_node_id, alloc_key_info) in slot_alloc_iter {
            let mut parent_amt_id = amt_node_id;
            parent_amt_id.pop().unwrap();

            amt_node_view.get(&parent_amt_id)?.unwrap();

            assert_ne!(
                alloc_key_info,
                crate::types::ValueEntry::Deleted,
                "slot alloc should not contain deletion"
            );
        }

        // Gather the versions of allocated slots for keys
        let mut slot_versions = BTreeMap::new();
        let key_value_iter = key_value_view.iter()?;
        for (key, lvmt_value) in key_value_iter {
            match lvmt_value {
                crate::types::ValueEntry::Value(lvmt_value) => {
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
                crate::types::ValueEntry::Deleted => {
                    panic!("key value should not contain deletion")
                }
            }
        }

        // Gather allocated slots for keys, in another way
        let mut slot_allocs = BTreeMap::new();
        let slot_alloc_iter = slot_alloc_view.iter()?;
        for (amt_node_id, alloc_key_info) in slot_alloc_iter {
            let mut parent_amt_id = amt_node_id;
            let node_index = parent_amt_id.pop().unwrap();

            match alloc_key_info {
                crate::types::ValueEntry::Value(alloc_key_info) => {
                    for slot_index in 0..=alloc_key_info.index {
                        let node_map = slot_allocs
                            .entry(parent_amt_id)
                            .or_insert_with(BTreeMap::new);
                        let slot_map = node_map.entry(node_index).or_insert_with(BTreeSet::new);
                        slot_map.insert(slot_index);
                    }
                }
                crate::types::ValueEntry::Deleted => {
                    panic!("slot alloc should not contain deletion")
                }
            }
        }

        // Check consistency between `slot_versions` and `slot_allocs`
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
            "Inconsistent allocations."
        );

        // Gather the versions of allocated slots for Amt trees (except the root Amt)
        let amt_node_iter = amt_node_view.iter()?;
        for (amt_id, curve_point_with_version) in amt_node_iter {
            if amt_id.len() > 0 {
                let mut parent_amt_id = amt_id;
                let node_index = parent_amt_id.pop().unwrap();
                let slot_index = SLOT_SIZE - 1;
                let version = {
                    match curve_point_with_version {
                        crate::types::ValueEntry::Value(curve_point_with_version) => {
                            curve_point_with_version.version
                        }
                        crate::types::ValueEntry::Deleted => {
                            panic!("amt node should not contain deletion")
                        }
                    }
                };

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

            assert_eq!(commitment, stored_commitment, "Inconsitent commitments.");
        }
        Ok(())
    }
}
