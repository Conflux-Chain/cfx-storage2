use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use ethereum_types::H256;
use once_cell::sync::Lazy;

use crate::{
    backends::{DatabaseTrait, InMemoryDatabase},
    errors::Result,
    middlewares::{
        confirmed_pending_to_history, gen_random_commit_id, gen_updates, get_rng_for_test,
        KeyValueStoreBulks, VersionedStore, VersionedStoreCache,
    },
    AmtParams, CreateMode,
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
    pub fn new() -> Result<Self> {
        Ok(Self {
            backend: D::empty_for_test()?,
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
        dbg!(1);
        confirmed_pending_to_history(
            &mut self.backend,
            &mut self.key_value_cache,
            new_root_commit_id,
            true,
        )?;
        dbg!(2);
        confirmed_pending_to_history(
            &mut self.backend,
            &mut self.amt_node_cache,
            new_root_commit_id,
            false,
        )?;
        dbg!(3);
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
    // Step 1: Convert u64 to an array of bytes
    let byte_array: [u8; 8] = value.to_ne_bytes();

    // Step 2: Convert the array to a Box<[u8]>
    byte_array.into()
}

fn option_u64_to_boxed_u8(opt: Option<u64>) -> Box<[u8]> {
    match opt {
        Some(value) => u64_to_boxed_u8(value),
        None => {
            // If None, return an empty boxed slice
            Box::new([])
        }
    }
}

pub const TEST_LEVEL: usize = 16;
pub const TEST_LENGTH: usize = 1 << TEST_LEVEL;

#[cfg(not(feature = "bls12-381"))]
pub type PE = ark_bn254::Bn254;
#[cfg(feature = "bls12-381")]
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

fn basic<D: DatabaseTrait>() {
    let mut db = Storage::<D>::new().unwrap();
    let mut lvmt = db.as_manager().unwrap();
    let mut rng = get_rng_for_test();
    let commit_1 = gen_random_commit_id(&mut rng);
    let commit_2 = gen_random_commit_id(&mut rng);
    let commit_2_1 = gen_random_commit_id(&mut rng);
    let commit_3 = gen_random_commit_id(&mut rng);
    let mut commits = HashSet::new();
    commits.insert(commit_1);
    commits.insert(commit_2);
    commits.insert(commit_2_1);
    commits.insert(commit_3);
    assert_eq!(commits.len(), 4);
    let previous_keys = Default::default();
    let num_keys = 100000; //00; // 10^7 has been tested, but still contain no amt_id whose len > 1
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
    let write_schema = D::write_schema();
    dbg!("init");
    lvmt.first_commit(commit_1, changes_1, &write_schema, &AMT)
        .unwrap();
    lvmt.check_consistency(commit_1, &AMT).unwrap();
    dbg!("first");
    lvmt.commit_for_test(commit_1, commit_2, changes_2, &write_schema, &AMT)
        .unwrap();
    lvmt.check_consistency(commit_2, &AMT).unwrap();
    dbg!("second");
    lvmt.commit_for_test(commit_1, commit_2_1, changes_2_1, &write_schema, &AMT)
        .unwrap();
    lvmt.check_consistency(commit_2_1, &AMT).unwrap();
    dbg!("second-branch");
    lvmt.check_consistency(commit_1, &AMT).unwrap();
    dbg!("check first after second");
    drop(lvmt);
    db.commit_auth(write_schema).unwrap();
    dbg!("commit auth");
    db.confirmed_pending_to_history(commit_2).unwrap();
    dbg!("confirmed_pending_to_history");
    lvmt = db.as_manager().unwrap();
    let write_schema = D::write_schema();
    lvmt.commit_for_test(commit_2, commit_3, changes_3, &write_schema, &AMT)
        .unwrap();
}

#[test]
#[ignore]
fn basic_rocksdb() {
    basic::<kvdb_rocksdb::Database>()
}

#[test]
#[ignore]
fn basic_inmemory() {
    basic::<InMemoryDatabase>()
}
