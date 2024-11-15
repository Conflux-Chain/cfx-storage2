use ethereum_types::H256;

use super::{
    amt_change_manager::AmtChangeManager,
    auth_changes::{amt_change_hash, key_value_hash, process_dump_items, AuthChangeTable},
    table_schema::{AmtNodes, FlatKeyValue, SlotAllocations},
    types::AllocatePosition,
};
use crate::{
    backends::WriteSchemaTrait,
    errors::Result,
    lvmt::types::{amt_node_id, AllocationInfo, KEY_SLOT_SIZE},
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

impl<'cache, 'db> LvmtStore<'cache, 'db> {
    fn commit(
        &self,
        old_commit: H256,
        new_commit: H256,
        changes: impl Iterator<Item = (Box<[u8]>, Box<[u8]>)>,
        write_schema: &impl WriteSchemaTrait,
    ) -> Result<()> {
        let amt_node_view = self.amt_node_store.get_versioned_store(&old_commit)?;
        let slot_alloc_view = self.slot_alloc_store.get_versioned_store(&old_commit)?;
        let key_value_view = self.key_value_store.get_versioned_store(&old_commit)?;

        let mut key_value_changes = vec![];
        let mut allocations = vec![];
        let mut amt_change_manager = AmtChangeManager::default();

        // Update version number
        for (key, value) in changes {
            let (allocation, version) = if let Some(old_value) = key_value_view.get(&key)? {
                (old_value.allocation, old_value.version + 1)
            } else {
                let allocation = allocate_version_slot(&*key, &slot_alloc_view)?;
                allocations.push(AllocationInfo::new(allocation.slot_index, key.clone()));
                (allocation, 0)
            };

            amt_change_manager.record_with_allocation(allocation, &*key);

            key_value_changes.push((
                key,
                LvmtValue {
                    allocation,
                    version,
                    value,
                },
            ));
        }

        let amt_changes = amt_change_manager.compute_amt_changes(&amt_node_view)?;

        // Update auth changes
        let auth_changes = {
            let auth_change_iter = amt_changes.iter().filter_map(|(amt_id, curve_point)| {
                (amt_id.len() > 0).then(|| amt_change_hash(amt_id, curve_point))
            });
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

fn allocate_version_slot(
    key: &[u8],
    db: &KeyValueSnapshotRead<SlotAllocations>,
) -> Result<AllocatePosition> {
    let key_digest = blake2s(&*key);

    let mut depth = 1;
    loop {
        let amt_node_id = amt_node_id(key_digest, depth);
        let slot_alloc = db.get(&amt_node_id)?;
        let next_index = match slot_alloc {
            None => 0,
            Some(x) if (x.index as usize) < KEY_SLOT_SIZE - 1 => x.index + 1,
            _ => {
                depth += 1;
                continue;
            }
        };

        return Ok(AllocatePosition {
            depth: depth as u8,
            node_index: amt_node_id.last().cloned().unwrap(),
            slot_index: next_index as u8,
        });
    }
}
