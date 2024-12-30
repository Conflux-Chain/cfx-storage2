use std::collections::{BTreeMap, HashSet};

use amt::AmtParams;
use ethereum_types::H256;

use super::{
    amt_change_manager::AmtChangeManager,
    auth_changes::{amt_change_hash, key_value_hash, process_dump_items, AuthChangeTable},
    crypto::PE,
    table_schema::{AmtNodes, FlatKeyValue, SlotAllocations},
    types::{AllocatePosition, AmtNodeId},
};
use crate::{
    backends::WriteSchemaTrait,
    errors::Result,
    lvmt::types::{compute_amt_node_id, AllocationKeyInfo, KEY_SLOT_SIZE},
    middlewares::table_schema::KeyValueSnapshotRead,
};
use crate::{
    lvmt::types::LvmtValue,
    middlewares::{KeyValueStoreBulks, VersionedStore},
    traits::{KeyValueStoreManager, KeyValueStoreRead},
    utils::hash::blake2s,
};

pub struct LvmtStore<'cache, 'db> {
    key_value_store: VersionedStore<'cache, 'db, FlatKeyValue>,
    amt_node_store: VersionedStore<'cache, 'db, AmtNodes>,
    slot_alloc_store: VersionedStore<'cache, 'db, SlotAllocations>,
    auth_changes: KeyValueStoreBulks<'db, AuthChangeTable>,
}

const ALLOC_START_VERSION: u64 = 1;

impl<'cache, 'db> LvmtStore<'cache, 'db> {
    fn commit(
        &self,
        old_commit: H256,
        new_commit: H256,
        changes: impl Iterator<Item = (Box<[u8]>, Box<[u8]>)>,
        write_schema: &impl WriteSchemaTrait,
        pp: &AmtParams<PE>,
    ) -> Result<()> {
        let amt_node_view = self.amt_node_store.get_versioned_store(&old_commit)?;
        let slot_alloc_view = self.slot_alloc_store.get_versioned_store(&old_commit)?;
        let key_value_view = self.key_value_store.get_versioned_store(&old_commit)?;

        let mut key_value_changes = vec![];
        let mut allocations = AllocationCacheDb::new(&slot_alloc_view);
        let mut amt_change_manager = AmtChangeManager::default();

        let mut set_of_keys = HashSet::new();

        // Update version number
        for (key, value) in changes {
            // skip the duplicated keys
            if !set_of_keys.insert(key.clone()) {
                continue;
            }

            let (allocation, version) = if let Some(old_value) = key_value_view.get(&key)? {
                (old_value.allocation, old_value.version + 1)
            } else {
                let allocation = allocate_version_slot(&key, &mut allocations)?;
                (allocation, ALLOC_START_VERSION)
            };

            amt_change_manager.record_with_allocation(allocation, &key);

            key_value_changes.push((
                key,
                LvmtValue {
                    allocation,
                    version,
                    value,
                },
            ));
        }

        let amt_changes = amt_change_manager.compute_amt_changes(&amt_node_view, pp)?;

        // Update auth changes
        let auth_changes = {
            let auth_change_iter = amt_changes
                .iter()
                .filter(|&(amt_id, curve_point)| (amt_id.len() > 0))
                .map(|(amt_id, curve_point)| amt_change_hash(amt_id, curve_point));
            let key_value_iter = key_value_changes
                .iter()
                .map(|(key, value)| key_value_hash(key, value));

            let hashes = key_value_iter.chain(auth_change_iter).collect();
            process_dump_items(hashes)
        };

        // TODO: write down to db

        Ok(())
    }
}

struct AllocationCacheDb<'db> {
    db: &'db KeyValueSnapshotRead<'db, SlotAllocations>,
    cache: BTreeMap<AmtNodeId, AllocationKeyInfo>,
}

impl<'db> AllocationCacheDb<'db> {
    fn new(db: &'db KeyValueSnapshotRead<SlotAllocations>) -> Self {
        Self {
            db,
            cache: Default::default(),
        }
    }

    fn get(&self, amt_node_id: &AmtNodeId) -> Result<Option<AllocationKeyInfo>> {
        match self.cache.get(amt_node_id) {
            Some(cached_value) => Ok(Some(cached_value.clone())),
            None => Ok(self.db.get(amt_node_id)?),
        }
    }

    fn set(&mut self, amt_node_id: AmtNodeId, alloc_info: AllocationKeyInfo) {
        self.cache.insert(amt_node_id, alloc_info);
    }
}

fn allocate_version_slot(
    key: &[u8],
    allocation_cache_db: &mut AllocationCacheDb,
) -> Result<AllocatePosition> {
    let key_digest = blake2s(key);

    let mut depth = 1;
    loop {
        let amt_node_id = compute_amt_node_id(key_digest, depth);
        let slot_alloc = allocation_cache_db.get(&amt_node_id)?;
        let next_index = match slot_alloc {
            None => 0,
            Some(x) if (x.index as usize) < KEY_SLOT_SIZE - 1 => x.index + 1,
            _ => {
                depth += 1;
                continue;
            }
        };

        allocation_cache_db.set(amt_node_id, AllocationKeyInfo::new(next_index, key.into()));

        return Ok(AllocatePosition {
            depth: depth as u8,
            slot_index: next_index as u8,
        });
    }
}
