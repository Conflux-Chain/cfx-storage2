use std::{collections::BTreeMap, sync::Arc};

use once_cell::sync::Lazy;

use crate::{
    backends::{DatabaseTrait, InMemoryDatabase},
    errors::Result,
    middlewares::{
        gen_random_commit_id, gen_updates, get_rng_for_test, KeyValueStoreBulks,
        MockVersionedStore, VersionedStore, VersionedStoreCache,
    },
    AmtParams, CreateMode,
};

use super::{
    auth_changes::AuthChangeTable,
    storage::LvmtStore,
    table_schema::{AmtNodes, FlatKeyValue, SlotAllocations},
};

pub struct MockLvmtStore<'db> {
    key_value_store: MockVersionedStore<FlatKeyValue>,
    amt_node_store: MockVersionedStore<AmtNodes>,
    slot_alloc_store: MockVersionedStore<SlotAllocations>,
    auth_changes: KeyValueStoreBulks<'db, AuthChangeTable>,
}

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
    let old_commit = gen_random_commit_id(&mut rng);
    let new_commit = gen_random_commit_id(&mut rng);
    assert_ne!(old_commit, new_commit);
    let previous_keys = Default::default();
    let num_keys = 10;
    let mut all_keys = Default::default();
    let updates_1 = gen_updates(&mut rng, &previous_keys, num_keys, 0, &mut all_keys);
    let previous_keys = all_keys.clone();
    let updates_2 = gen_updates(&mut rng, &previous_keys, num_keys, num_keys, &mut all_keys);
    let changes_1 = get_changes_from_updates(updates_1);
    let changes_2 = get_changes_from_updates(updates_2);
    let write_schema = D::write_schema();
    lvmt.first_commit(old_commit, changes_1, &write_schema, &AMT)
        .unwrap();
    lvmt.commit_for_test(old_commit, new_commit, changes_2, &write_schema, &AMT)
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
