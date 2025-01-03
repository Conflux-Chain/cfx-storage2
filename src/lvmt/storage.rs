use std::collections::{BTreeMap, HashSet};

use amt::AmtParams;

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
    middlewares::{table_schema::KeyValueSnapshotRead, CommitID},
    traits::KeyValueStoreBulksTrait,
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
    pub fn new(
        key_value_store: VersionedStore<'cache, 'db, FlatKeyValue>,
        amt_node_store: VersionedStore<'cache, 'db, AmtNodes>,
        slot_alloc_store: VersionedStore<'cache, 'db, SlotAllocations>,
        auth_changes: KeyValueStoreBulks<'db, AuthChangeTable>,
    ) -> Self {
        Self {
            key_value_store,
            amt_node_store,
            slot_alloc_store,
            auth_changes,
        }
    }

    fn commit(
        &mut self,
        old_commit: Option<CommitID>,
        new_commit: CommitID,
        changes: impl Iterator<Item = (Box<[u8]>, Box<[u8]>)>,
        write_schema: &impl WriteSchemaTrait,
        pp: &AmtParams<PE>,
    ) -> Result<()> {
        let (amt_node_view, slot_alloc_view, key_value_view) = if let Some(old_commit) = old_commit
        {
            (
                Some(self.amt_node_store.get_versioned_store(&old_commit)?),
                Some(self.slot_alloc_store.get_versioned_store(&old_commit)?),
                Some(self.key_value_store.get_versioned_store(&old_commit)?),
            )
        } else {
            (None, None, None)
        };

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

        // Write to the pending part of db.
        // TODO: Write to the history part is beyond the range of LvmtStore.
        // TODO: LvmtStore.auth_changes includes all commits, even if they are removed but not confirmed,
        //       so consider gc_commit elsewhere.
        let amt_node_updates: BTreeMap<_, _> =
            amt_changes.into_iter().map(|(k, v)| (k, Some(v))).collect();
        self.amt_node_store
            .add_to_pending_part(old_commit, new_commit, amt_node_updates)?;

        let key_value_updates: BTreeMap<_, _> = key_value_changes
            .into_iter()
            .map(|(k, v)| (k, Some(v)))
            .collect();
        self.key_value_store
            .add_to_pending_part(old_commit, new_commit, key_value_updates)?;

        let slot_alloc_updates: BTreeMap<_, _> = allocations
            .into_changes()
            .into_iter()
            .map(|(k, v)| (k, Some(v)))
            .collect();
        self.slot_alloc_store
            .add_to_pending_part(old_commit, new_commit, slot_alloc_updates)?;

        let auth_change_bulk = auth_changes.into_iter().map(|(k, v)| (k, Some(v)));
        self.auth_changes
            .commit(new_commit, auth_change_bulk, write_schema)?;

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

    fn into_changes(self) -> BTreeMap<AmtNodeId, AllocationKeyInfo> {
        self.cache
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
