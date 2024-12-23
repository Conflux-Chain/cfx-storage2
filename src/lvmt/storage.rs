use std::collections::{BTreeMap, HashSet};

use amt::AmtParams;
use ethereum_types::H256;

use super::{
    amt_change_manager::AmtChangeManager,
    auth_changes::{amt_change_hash, key_value_hash, process_dump_items, AuthChangeTable},
    crypto::PE,
    table_schema::{AmtNodes, FlatKeyValue, SlotAllocations},
    types::{AllocatePosition, AmtNodeId, AuthChangeKey, AuthChangeNode, CurvePointWithVersion},
};
use crate::{
    backends::WriteSchemaTrait, errors::Result, lvmt::types::{compute_amt_node_id, AllocationKeyInfo, KEY_SLOT_SIZE}, middlewares::table_schema::KeyValueSnapshotRead, traits::KeyValueStoreBulksTrait, StorageError
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

pub const ALLOC_START_VERSION: u64 = 1;

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

    pub fn commit(
        &mut self,
        old_commit: Option<H256>,
        new_commit: H256,
        changes: impl Iterator<Item = (Box<[u8]>, Box<[u8]>)>,
        write_schema: &impl WriteSchemaTrait,
        pp: &AmtParams<PE>,
    ) -> Result<()> {
        if let Some(old_commit) = old_commit {
            self.subsequent_commit(old_commit, new_commit, changes, write_schema, pp)
        } else {
            self.first_commit(new_commit, changes, write_schema, pp)
        }
    }

    fn first_commit(
        &mut self,
        commit: H256,
        changes: impl Iterator<Item = (Box<[u8]>, Box<[u8]>)>,
        write_schema: &impl WriteSchemaTrait,
        pp: &AmtParams<PE>,
    ) -> Result<()> {
        let mut key_value_changes = vec![];
        let mut allocations = BTreeMap::new();
        let mut amt_change_manager = AmtChangeManager::default();

        let mut set_of_keys = HashSet::new();

        // Update version number
        for (key, value) in changes {
            if !set_of_keys.insert(key.clone()) {
                return Err(StorageError::DuplicateKeysInOneCommit);
            }

            let (allocation, version) = {
                let (allocation_wrt_db, key_digest) = allocate_version_slot_from_empty_db(&key)?;
                let allocation =
                    resolve_allocation_slot(&key, allocation_wrt_db, key_digest, &mut allocations);
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

        let amt_changes = amt_change_manager.compute_amt_changes(None, pp)?;

        // Write down to db
        self.write_to_db(
            None,
            commit,
            amt_changes,
            key_value_changes,
            allocations,
            write_schema,
        )?;

        Ok(())
    }

    #[cfg(test)]
    pub fn check_consistency(&mut self, commit: H256, pp: &AmtParams<PE>) -> Result<()> {
        use std::collections::BTreeSet;

        use ark_ec::CurveGroup;

        use crate::lvmt::{
            crypto::{FrInt, VariableBaseMSM, G1},
            types::SLOT_SIZE,
        };

        let amt_node_view = self.amt_node_store.get_versioned_store(&commit)?;
        let slot_alloc_view = self.slot_alloc_store.get_versioned_store(&commit)?;
        let key_value_view = self.key_value_store.get_versioned_store(&commit)?;

        // Each Amt subtree (amt_id) (except the children of the root Amt)
        // should be a fully-allocated leaf node of its parent Amt tree.
        // The expection of the children of the root Amt is due to the design that the root Amt does not allocate slots.
        let amt_node_iter = amt_node_view.iter_pending();
        for (amt_id, curve_point_with_version) in amt_node_iter {
            if amt_id.len() > 1 {
                let amt_node_id = amt_id;
                let alloc_key_info = slot_alloc_view.get(&amt_node_id)?.unwrap();
                assert_eq!(alloc_key_info.index as usize, KEY_SLOT_SIZE - 1);
            }

            if curve_point_with_version == crate::types::ValueEntry::Deleted {
                panic!("amt node should not contain deletion")
            }
        }

        // Each AmtNode should be in an Amt tree
        let slot_alloc_iter = slot_alloc_view.iter_pending();
        for (amt_node_id, alloc_key_info) in slot_alloc_iter {
            let mut parent_amt_id = amt_node_id;
            parent_amt_id.pop().unwrap();

            amt_node_view.get(&parent_amt_id)?.unwrap();

            if alloc_key_info == crate::types::ValueEntry::Deleted {
                panic!("slot alloc should not contain deletion")
            }
        }

        // Gather the version of each allocated slot
        let mut slot_versions = BTreeMap::new();
        let key_value_iter = key_value_view.iter_pending();
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

        // Gather each allocated slot
        let mut slot_allocs = BTreeMap::new();
        let slot_alloc_iter = slot_alloc_view.iter_pending();
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
                (amt_id.clone(), node_map_simple)
            })
            .collect();
        assert_eq!(
            slot_versions_simple, slot_allocs,
            "Inconsistent allocations."
        );

        // Add amt node to `slot_versions`
        let amt_node_iter = amt_node_view.iter_pending();
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
            assert_eq!(commitment, stored_commitment);
        }
        Ok(())
    }

    fn subsequent_commit(
        &mut self,
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
        let mut allocations = BTreeMap::new();
        let mut amt_change_manager = AmtChangeManager::default();

        let mut set_of_keys = HashSet::new();

        // Update version number
        for (key, value) in changes {
            if !set_of_keys.insert(key.clone()) {
                return Err(StorageError::DuplicateKeysInOneCommit);
            }

            let (allocation, version) = if let Some(old_value) = key_value_view.get(&key)? {
                (old_value.allocation, old_value.version + 1)
            } else {
                let (allocation_wrt_db, key_digest) =
                    allocate_version_slot(&key, &slot_alloc_view)?;
                let allocation =
                    resolve_allocation_slot(&key, allocation_wrt_db, key_digest, &mut allocations);
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

        let amt_changes = amt_change_manager.compute_amt_changes(Some(&amt_node_view), pp)?;

        // Write down to db
        self.write_to_db(
            Some(old_commit),
            new_commit,
            amt_changes,
            key_value_changes,
            allocations,
            write_schema,
        )?;

        Ok(())
    }

    /// Write to the pending part of db.
    /// Write to the history part is beyond the range of [`LvmtStore`].
    /// Note: `self.auth_changes` includes all commits, even if they are not confirmed, so consider `gc_commit` elsewhere.
    fn write_to_db(
        &mut self,
        parent_commit: Option<H256>,
        new_commit: H256,
        amt_changes: Vec<(AmtNodeId, CurvePointWithVersion)>,
        key_value_changes: Vec<(Box<[u8]>, LvmtValue)>,
        allocations: BTreeMap<AmtNodeId, AllocationKeyInfo>,
        write_schema: &impl WriteSchemaTrait,
    ) -> Result<()> {
        // Update auth changes
        let auth_changes = compute_auth_changes(&amt_changes, &key_value_changes);

        let amt_node_updates: BTreeMap<_, _> =
            amt_changes.into_iter().map(|(k, v)| (k, Some(v))).collect();
        self.amt_node_store
            .add_to_pending_part(parent_commit, new_commit, amt_node_updates)?;

        let key_value_updates: BTreeMap<_, _> = key_value_changes
            .into_iter()
            .map(|(k, v)| (k, Some(v)))
            .collect();
        self.key_value_store
            .add_to_pending_part(parent_commit, new_commit, key_value_updates)?;

        let slot_alloc_updates: BTreeMap<_, _> =
            allocations.into_iter().map(|(k, v)| (k, Some(v))).collect();
        self.slot_alloc_store
            .add_to_pending_part(parent_commit, new_commit, slot_alloc_updates)?;

        let auth_change_bulk = auth_changes.into_iter().map(|(k, v)| (k, Some(v)));
        self.auth_changes
            .commit(new_commit, auth_change_bulk, write_schema)?;

        Ok(())
    }
}

fn compute_auth_changes(
    amt_changes: &[(AmtNodeId, CurvePointWithVersion)],
    key_value_changes: &[(Box<[u8]>, LvmtValue)],
) -> BTreeMap<AuthChangeKey, AuthChangeNode> {
    let auth_change_iter = amt_changes
        .iter()
        .filter(|&(amt_id, curve_point)| (amt_id.len() > 0))
        .map(|(amt_id, curve_point)| amt_change_hash(amt_id, curve_point));
    let key_value_iter = key_value_changes
        .iter()
        .map(|(key, value)| key_value_hash(key, value));

    let hashes = key_value_iter.chain(auth_change_iter).collect();
    process_dump_items(hashes)
}

fn allocate_version_slot_from_empty_db(key: &[u8]) -> Result<(AllocatePosition, H256)> {
    let key_digest = blake2s(key);
    Ok((
        AllocatePosition {
            depth: 1,
            slot_index: 0,
        },
        key_digest,
    ))
}

fn allocate_version_slot(
    key: &[u8],
    db: &KeyValueSnapshotRead<SlotAllocations>,
) -> Result<(AllocatePosition, H256)> {
    let key_digest = blake2s(key);

    let mut depth = 1;
    loop {
        let amt_node_id = compute_amt_node_id(key_digest, depth);
        let slot_alloc = db.get(&amt_node_id)?;
        let next_index = match slot_alloc {
            None => 0,
            Some(x) if (x.index as usize) < KEY_SLOT_SIZE - 1 => x.index + 1,
            _ => {
                depth += 1;
                continue;
            }
        };

        return Ok((
            AllocatePosition {
                depth: depth as u8,
                slot_index: next_index as u8,
            },
            key_digest,
        ));
    }
}

fn resolve_allocation_slot(
    key: &[u8],
    allocation_wrt_db: AllocatePosition,
    key_digest: H256,
    allocations: &mut BTreeMap<AmtNodeId, AllocationKeyInfo>,
) -> AllocatePosition {
    let mut depth = allocation_wrt_db.depth as usize;

    loop {
        let amt_node_id = compute_amt_node_id(key_digest, depth);
        let slot_alloc = allocations.get(&amt_node_id);
        let next_index = match slot_alloc {
            None => {
                if depth > allocation_wrt_db.depth as usize {
                    0
                } else {
                    allocation_wrt_db.slot_index
                }
            }
            Some(alloc) => {
                if (alloc.index as usize) < KEY_SLOT_SIZE - 1 {
                    alloc.index + 1
                } else {
                    depth += 1;
                    continue;
                }
            }
        };

        assert!(depth >= allocation_wrt_db.depth as usize);
        if depth == allocation_wrt_db.depth as usize {
            assert!(next_index >= allocation_wrt_db.slot_index);
        }

        allocations.insert(amt_node_id, AllocationKeyInfo::new(next_index, key.into()));

        return AllocatePosition {
            depth: depth as u8,
            slot_index: next_index as u8,
        };
    }
}